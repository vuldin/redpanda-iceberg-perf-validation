// Synthetic protobuf producer for the iceberg perf validation harness.
//
// Generates VARIED, realistic OpenRTB-style BidderRequest records (schemas/bid-request.proto):
// ~100 leaf columns across nested messages (imps, device/geo, user/data/segments, banner/video)
// so translation CPU reflects real per-byte cost (many parquet columns), not the artificially
// cheap cost of one dominant string. Records are randomized per-call; a pool of distinct markups
// keeps generation fast while preserving entropy (compresses ~2-3:1 like real ad traffic).
//
// Produces with N parallel workers (default GOMAXPROCS) sharing an atomic token bucket on actual
// encoded size. Can produce directly to the TARGET cluster over TLS (TLS_ENABLED=true) to remove
// the migrator from the path when isolating translation cost.
//
// Exposes Prometheus metrics on $METRICS_PORT/metrics (host network).
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protowire"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

var (
	produceSuccess = prometheus.NewCounter(prometheus.CounterOpts{Name: "producer_records_produced_total", Help: "Records produced"})
	produceFailure = prometheus.NewCounter(prometheus.CounterOpts{Name: "producer_records_failed_total", Help: "Records failed"})
	produceBytes   = prometheus.NewCounter(prometheus.CounterOpts{Name: "producer_bytes_produced_total", Help: "Encoded bytes produced (pre-compression)"})
	produceLatency = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "producer_record_latency_seconds", Help: "Per-record produce latency", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14)})
)

func init() { prometheus.MustRegister(produceSuccess, produceFailure, produceBytes, produceLatency) }

// --- protobuf wire helpers (field numbers MUST match bid-request.proto) ---
func pstr(b []byte, n int, s string) []byte {
	b = protowire.AppendTag(b, protowire.Number(n), protowire.BytesType)
	return protowire.AppendString(b, s)
}
func pvar(b []byte, n int, v uint64) []byte {
	b = protowire.AppendTag(b, protowire.Number(n), protowire.VarintType)
	return protowire.AppendVarint(b, v)
}
func pf64(b []byte, n int, f float64) []byte {
	b = protowire.AppendTag(b, protowire.Number(n), protowire.Fixed64Type)
	return protowire.AppendFixed64(b, math.Float64bits(f))
}
func pmsg(b []byte, n int, sub []byte) []byte {
	b = protowire.AppendTag(b, protowire.Number(n), protowire.BytesType)
	return protowire.AppendBytes(b, sub)
}

var (
	exchanges  = []string{"exchange-a", "exchange-b", "exchange-c", "exchange-d", "exchange-e", "exchange-f"}
	devTypes   = []string{"ctv", "mobile", "desktop", "tablet", "audio"}
	makes      = []string{"samsung", "lg", "roku", "apple", "amazon", "vizio", "google"}
	oses       = []string{"tizen", "webos", "roku_os", "ios", "android", "fire_os"}
	countries  = []string{"USA", "CAN", "GBR", "DEU", "FRA", "AUS", "BRA", "MEX", "JPN"}
	regions    = []string{"NY", "CA", "TX", "FL", "IL", "WA", "MA", "GA"}
	cities     = []string{"new_york", "los_angeles", "chicago", "houston", "seattle", "boston", "atlanta"}
	metros     = []string{"501", "803", "602", "618", "819", "506", "524"}
	currencies = []string{"USD", "EUR", "GBP", "CAD"}
	kwpool     = []string{"sports", "news", "drama", "comedy", "kids", "live", "premium", "sd", "hd", "4k", "ott", "vod"}
	cats       = []string{"IAB1", "IAB2", "IAB3", "IAB9", "IAB17", "IAB19", "IAB21"}
	mimePool   = []string{"video/mp4", "video/webm", "application/x-mpegURL", "video/3gpp"}
)

const b64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

func randMarkup(r *rand.Rand, n int) string {
	var b strings.Builder
	b.Grow(n + 128)
	b.WriteString("<VAST version=\"4.0\"><Ad>")
	for b.Len() < n {
		tok := make([]byte, 32)
		for i := range tok {
			tok[i] = b64[r.Intn(len(b64))]
		}
		fmt.Fprintf(&b, `<Creative id="%08x"><MediaFile>%s</MediaFile></Creative>`, r.Uint32(), tok)
	}
	b.WriteString("</Ad></VAST>")
	s := b.String()
	if len(s) > n {
		s = s[:n]
	}
	return s
}

func pick(r *rand.Rand, s []string) string { return s[r.Intn(len(s))] }

// repeated int32 (one tag+varint per element; representative of OpenRTB int arrays)
func prepvar(b []byte, n int, r *rand.Rand, count, maxv int) []byte {
	for i := 0; i < count; i++ {
		b = pvar(b, n, uint64(r.Intn(maxv)+1))
	}
	return b
}

func buildGeo(r *rand.Rand) []byte {
	var g []byte
	g = pf64(g, 1, -90+r.Float64()*180)
	g = pf64(g, 2, -180+r.Float64()*360)
	g = pstr(g, 3, pick(r, countries))
	g = pstr(g, 4, pick(r, regions))
	g = pstr(g, 5, pick(r, metros))
	g = pstr(g, 6, pick(r, cities))
	g = pstr(g, 7, fmt.Sprintf("%05d", r.Intn(100000)))
	g = pvar(g, 8, uint64(r.Intn(3)+1))
	g = pvar(g, 9, uint64(r.Intn(24)))
	g = pstr(g, 10, "ip2geo")
	return g
}

func buildDevice(r *rand.Rand) []byte {
	var d []byte
	d = pstr(d, 1, fmt.Sprintf("Mozilla/5.0 (%s; %s %d.%d) build/%06x", pick(r, makes), pick(r, oses), r.Intn(15), r.Intn(9), r.Uint32()))
	d = pstr(d, 2, fmt.Sprintf("%d.%d.%d.%d", r.Intn(256), r.Intn(256), r.Intn(256), r.Intn(256)))
	d = pstr(d, 3, pick(r, makes))
	d = pstr(d, 4, fmt.Sprintf("model-%04x", r.Intn(65536)))
	d = pstr(d, 5, pick(r, oses))
	d = pstr(d, 6, fmt.Sprintf("%d.%d", r.Intn(15), r.Intn(10)))
	d = pstr(d, 7, fmt.Sprintf("hw%d", r.Intn(5)))
	d = pvar(d, 8, uint64([]int{720, 1080, 1440, 2160}[r.Intn(4)]))
	d = pvar(d, 9, uint64([]int{1280, 1920, 2560, 3840}[r.Intn(4)]))
	d = pvar(d, 10, uint64(r.Intn(600)))
	d = pvar(d, 11, uint64(r.Intn(2)))
	d = pvar(d, 12, uint64(r.Intn(7)+1))
	d = pvar(d, 13, uint64(r.Intn(7)))
	d = pstr(d, 14, hex.EncodeToString([]byte{byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256))}))
	d = pvar(d, 15, uint64(r.Intn(2)))
	d = pstr(d, 16, "en")
	d = pstr(d, 17, pick(r, makes)+"_net")
	d = pstr(d, 18, fmt.Sprintf("%03d-%03d", r.Intn(1000), r.Intn(1000)))
	d = pmsg(d, 19, buildGeo(r))
	return d
}

func buildUser(r *rand.Rand) []byte {
	var u []byte
	u = pstr(u, 1, hex.EncodeToString([]byte{byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256))}))
	u = pstr(u, 2, fmt.Sprintf("buyer-%08x", r.Uint32()))
	u = pvar(u, 3, uint64(1950+r.Intn(60)))
	u = pstr(u, 4, []string{"M", "F", "O"}[r.Intn(3)])
	u = pstr(u, 5, pick(r, kwpool)+","+pick(r, kwpool))
	u = pstr(u, 6, fmt.Sprintf("cd-%06x", r.Intn(1<<24)))
	// 1-3 data blocks, each with 1-4 segments
	for nd := r.Intn(3) + 1; nd > 0; nd-- {
		var d []byte
		d = pstr(d, 1, fmt.Sprintf("dp-%04x", r.Intn(65536)))
		d = pstr(d, 2, pick(r, exchanges)+"_dmp")
		for ns := r.Intn(4) + 1; ns > 0; ns-- {
			var s []byte
			s = pstr(s, 1, fmt.Sprintf("seg-%05x", r.Intn(1<<20)))
			s = pstr(s, 2, pick(r, kwpool))
			s = pstr(s, 3, fmt.Sprintf("%.2f", r.Float64()))
			d = pmsg(d, 3, s)
		}
		u = pmsg(u, 7, d)
	}
	return u
}

func buildBanner(r *rand.Rand) []byte {
	var b []byte
	b = pvar(b, 1, uint64([]int{300, 728, 970, 320}[r.Intn(4)]))
	b = pvar(b, 2, uint64([]int{250, 90, 50, 100}[r.Intn(4)]))
	b = pvar(b, 3, uint64(r.Intn(7)))
	b = pstr(b, 4, "image/jpeg,image/png")
	b = pvar(b, 5, uint64(r.Intn(2)))
	b = prepvar(b, 6, r, r.Intn(3)+1, 8)
	b = prepvar(b, 7, r, r.Intn(4)+1, 16)
	b = pstr(b, 8, fmt.Sprintf("ban-%04x", r.Intn(65536)))
	return b
}

func buildVideo(r *rand.Rand) []byte {
	var v []byte
	for nm := r.Intn(3) + 1; nm > 0; nm-- {
		v = pstr(v, 1, pick(r, mimePool))
	}
	v = pvar(v, 2, uint64(r.Intn(10)+5))
	v = pvar(v, 3, uint64(r.Intn(60)+15))
	v = prepvar(v, 4, r, r.Intn(3)+1, 8)
	v = pvar(v, 5, uint64([]int{640, 1280, 1920}[r.Intn(3)]))
	v = pvar(v, 6, uint64([]int{480, 720, 1080}[r.Intn(3)]))
	v = pvar(v, 7, uint64(r.Intn(3)))
	v = pvar(v, 8, uint64(r.Intn(5)+1))
	v = pvar(v, 9, uint64(r.Intn(2)+1))
	v = pvar(v, 10, uint64(r.Intn(2)))
	v = pvar(v, 11, uint64(r.Intn(30)))
	v = pvar(v, 12, uint64(r.Intn(3)))
	v = prepvar(v, 13, r, r.Intn(4)+1, 16)
	v = pvar(v, 14, uint64(r.Intn(8000)+500))
	v = pvar(v, 15, uint64(r.Intn(500)+100))
	v = prepvar(v, 16, r, r.Intn(3)+1, 6)
	v = prepvar(v, 17, r, r.Intn(2)+1, 3)
	v = pvar(v, 18, uint64(r.Intn(7)))
	v = prepvar(v, 19, r, r.Intn(3)+1, 7)
	return v
}

func buildImp(r *rand.Rand) []byte {
	var im []byte
	im = pstr(im, 1, fmt.Sprintf("imp-%06x", r.Intn(1<<24)))
	im = pstr(im, 2, fmt.Sprintf("slot-%06x", r.Intn(1<<24)))
	im = pf64(im, 3, r.Float64()*50)
	im = pstr(im, 4, pick(r, currencies))
	im = pvar(im, 5, uint64(r.Intn(2)))
	im = pvar(im, 6, uint64(r.Intn(2)))
	im = pstr(im, 7, pick(r, exchanges)+"_sdk")
	im = pstr(im, 8, fmt.Sprintf("%d.%d.%d", r.Intn(5), r.Intn(9), r.Intn(9)))
	im = pvar(im, 9, uint64(r.Intn(2)))
	im = pvar(im, 10, uint64(r.Intn(300)+30))
	if r.Intn(2) == 0 {
		im = pmsg(im, 11, buildBanner(r))
	} else {
		im = pmsg(im, 12, buildVideo(r))
	}
	return im
}

// buildRecord assembles one randomized BidderRequest; markup is drawn from the shared pool.
func buildRecord(r *rand.Rand, pool []string) []byte {
	var b []byte
	b = pstr(b, 1, fmt.Sprintf("br-%08x%08x", r.Uint32(), r.Uint32()))
	b = pvar(b, 2, uint64(r.Intn(3)+1))
	b = pvar(b, 3, uint64(100+r.Intn(400)))
	b = pstr(b, 4, pick(r, currencies))
	for n := r.Intn(3) + 1; n > 0; n-- {
		b = pstr(b, 5, pick(r, cats))
	}
	for n := r.Intn(3); n > 0; n-- {
		b = pstr(b, 6, pick(r, exchanges)+".com")
	}
	for n := r.Intn(2); n > 0; n-- {
		b = pstr(b, 7, fmt.Sprintf("seat-%04x", r.Intn(65536)))
	}
	b = pvar(b, 8, uint64(r.Intn(2)))
	b = pvar(b, 9, uint64(r.Intn(2)))
	for ni := r.Intn(3) + 1; ni > 0; ni-- { // 1-3 imps
		b = pmsg(b, 10, buildImp(r))
	}
	b = pmsg(b, 11, buildDevice(r))
	b = pmsg(b, 12, buildUser(r))
	var src []byte
	src = pvar(src, 1, uint64(r.Intn(2)))
	src = pstr(src, 2, fmt.Sprintf("tid-%08x", r.Uint32()))
	src = pstr(src, 3, fmt.Sprintf("%08x.com", r.Uint32()))
	b = pmsg(b, 13, src)
	var reg []byte
	reg = pvar(reg, 1, uint64(r.Intn(2)))
	reg = pvar(reg, 2, uint64(r.Intn(2)))
	reg = pstr(reg, 3, "1YNN")
	b = pmsg(b, 14, reg)
	b = pstr(b, 15, pick(r, exchanges))
	b = pvar(b, 16, uint64(r.Intn(2)+1))
	b = pstr(b, 17, fmt.Sprintf("key-%08x", r.Uint32()))
	b = pstr(b, 18, pick(r, makes)+"_partner")
	b = pstr(b, 19, fmt.Sprintf("deal-%06x", r.Intn(1<<24)))
	for n := 2 + r.Intn(4); n > 0; n-- {
		b = pstr(b, 20, pick(r, kwpool))
	}
	b = pvar(b, 21, uint64(time.Now().UnixMilli()))
	b = pstr(b, 22, fmt.Sprintf("%08x", r.Uint32()))
	b = pstr(b, 23, pool[r.Intn(len(pool))])
	b = pstr(b, 24, fmt.Sprintf(`{"dsp":%d,"pri":%d}`, r.Intn(50), r.Intn(10)))
	return b
}

// loadCorpus reads a length-delimited protobuf corpus ([uint32 LE len][bytes]...) and replays it
// instead of generating synthetic records. Replaying a fixed, pre-encoded corpus keeps the
// per-record decode cost (decompressed bytes + leaf-column count) faithful to whatever you
// captured, which is what translation CPU is paid on. Optional; leave unset to use the generator.
func loadCorpus(path string) [][]byte {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open corpus %s: %v", path, err)
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	var out [][]byte
	var hdr [4]byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("corpus read len: %v", err)
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		rec := make([]byte, n)
		if _, err := io.ReadFull(br, rec); err != nil {
			log.Fatalf("corpus read rec: %v", err)
		}
		out = append(out, rec)
	}
	return out
}

func main() {
	brokers := strings.Split(envOr("BROKERS", "localhost:9092"), ",")
	topic := envOr("TOPIC", "bid-request")
	targetMiBps := envIntOr("TARGET_MIBPS", 160)
	xmlBytes := envIntOr("XML_PAYLOAD_BYTES", 512)
	poolSize := envIntOr("MARKUP_POOL", 1024)
	metricsPort := envOr("METRICS_PORT", "9090")
	workers := envIntOr("WORKERS", 0)
	if workers <= 0 {
		workers = envIntOr("GOMAXPROCS", 0)
	}
	if workers <= 0 {
		workers = 8
	}
	useTLS := envOr("TLS_ENABLED", "false") == "true"

	// Replay mode: if CORPUS_PATH is set, produce REAL records from the corpus (cycling)
	// instead of synthetic ones. TARGET_MIBPS<=0 means unlimited (saturate translation).
	var corpus [][]byte
	if cp := envOr("CORPUS_PATH", ""); cp != "" {
		corpus = loadCorpus(cp)
		var tot int
		for _, r := range corpus {
			tot += len(r)
		}
		avg := 0
		if len(corpus) > 0 {
			avg = tot / len(corpus)
		}
		log.Printf("replay corpus: %d records, %d bytes (%d B/rec avg) from %s", len(corpus), tot, avg, cp)
	}

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Printf("metrics on :%s/metrics", metricsPort)
		log.Fatal(http.ListenAndServe(":"+metricsPort, mux))
	}()

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerLinger(20 * time.Millisecond),
		kgo.MaxBufferedRecords(1 << 21),
		kgo.ClientID("rp-iceberg-perf-producer"),
	}
	if useTLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{InsecureSkipVerify: true}))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()
	log.Printf("producing to %v topic=%s target=%d MiB/s markup=%d B pool=%d workers=%d tls=%v (realistic OpenRTB protobuf)",
		brokers, topic, targetMiBps, xmlBytes, poolSize, workers, useTLS)

	// Shared read-only markup pool (varied bytes, fast). Synthetic path only.
	seed := rand.New(rand.NewSource(time.Now().UnixNano()))
	pool := make([]string, poolSize)
	if corpus == nil {
		for i := range pool {
			pool[i] = randMarkup(seed, xmlBytes)
		}
	}

	// Atomic token bucket on encoded bytes, refilled by a ticker. targetMiBps<=0 => unlimited.
	unlimited := targetMiBps <= 0
	var bucket atomic.Int64
	bytesPerTick := int64(targetMiBps) * 1024 * 1024 / 100
	if !unlimited {
		bucket.Store(bytesPerTick)
		go func() {
			t := time.NewTicker(10 * time.Millisecond)
			defer t.Stop()
			cap := bytesPerTick * 5
			for range t.C {
				if bucket.Add(bytesPerTick) > cap {
					bucket.Store(cap)
				}
			}
		}()
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			idx := int(uint64(seed) % uint64(len(corpus)+1))
			for {
				var b []byte
				if corpus != nil {
					b = corpus[idx%len(corpus)]
					idx++
				} else {
					b = buildRecord(r, pool)
				}
				sz := int64(len(b))
				if !unlimited {
					for bucket.Add(-sz) < 0 { // reserve; if over-drawn, give back and wait
						bucket.Add(sz)
						time.Sleep(time.Millisecond)
					}
				}
				n := len(b)
				start := time.Now()
				cl.Produce(ctx, &kgo.Record{Value: b}, func(_ *kgo.Record, err error) {
					produceLatency.Observe(time.Since(start).Seconds())
					if err != nil {
						produceFailure.Inc()
						return
					}
					produceSuccess.Inc()
					produceBytes.Add(float64(n))
				})
			}
		}(time.Now().UnixNano() ^ int64(w)*0x9e3779b9)
	}
	wg.Wait()
}
