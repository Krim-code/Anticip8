// main.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

var (
	ctx = context.Background()

	// =========================
	// Config
	// =========================
	redisURL = getenv("REDIS_URL", "redis://redis:6379/0")
	rdb      *redis.Client

	noisePrefixes = []string{"/docs", "/openapi.json", "/redoc", "/metrics", "/_whoami", "/health"}

	// Item2Vec
	i2vPrefix = getenv("I2V_PREFIX", "anticip8:i2v:topk:")
	i2vAlpha  = getenvFloat("I2V_ALPHA", 0.45)
	i2vTopK   = getenvInt("I2V_TOPK", 30)

	// Policy behavior
	allowPrefetchAttemptsInPolicy = getenvBool("ALLOW_PREFETCH_ATTEMPTS_IN_POLICY", false) || getenvBool("ALLOW_PREFETCH_ATTEMPTS", false)
	prefetchAttemptWeight         = getenvFloat("PREFETCH_ATTEMPT_WEIGHT", 0.15)

	defaultMaxPrefetch      = getenvInt("MAX_PREFETCH", 2)
	defaultPrefetchBudgetMS = getenvInt("PREFETCH_BUDGET_MS", 120)

	markovSmooth  = getenvFloat("MARKOV_SMOOTH", 0.5)
	minProb       = getenvFloat("MIN_PROB", 0.01)
	dropSelfLoops = getenvBool("DROP_SELF_LOOPS", true)

	// =========================
	// Path normalization
	// =========================
	reUUID = regexp.MustCompile(`/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}(/|$)`)
    reInt  = regexp.MustCompile(`/\d+(/|$)`)
	reUUIDToken = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}`)
	reIntToken  = regexp.MustCompile(`(?:^|/)\d+(?:/|$)`)
)

type Event struct {
	Service   string `json:"service"`
	UserKey   string `json:"user_key"`
	FromPath  string `json:"from_path"`
	ToPath    string `json:"to_path"`
	Status    int    `json:"status"`
	LatencyMS int    `json:"latency_ms"`
}

type EdgeEvent struct {
	SrcService string `json:"src_service"`
	UserKey    string `json:"user_key"`
	SrcPath    string `json:"src_path"`
	DstService string `json:"dst_service"`
	DstPath    string `json:"dst_path"`
	Status     int    `json:"status"`
	LatencyMS  int    `json:"latency_ms"`
}

type PrefetchAttempt struct {
	SrcService string `json:"src_service"`
	UserKey    string `json:"user_key"`
	SrcPath    string `json:"src_path"`
	DstService string `json:"dst_service"`
	DstPath    string `json:"dst_path"`
	Status     int    `json:"status"`
	LatencyMS  int    `json:"latency_ms"`
}

type NextPath struct {
	Service string  `json:"service"`
	Path    string  `json:"path"`
	Score   float64 `json:"score"`
}

type PolicyResp struct {
	NextPaths         []NextPath `json:"next_paths"`
	MaxPrefetch       int        `json:"max_prefetch"`
	MaxPrefetchTimeMS int        `json:"max_prefetch_time_ms"`
}

// Item2Vec payload: [{ "item": "svc::/path", "cos": 0.93 }, ...]
type i2vItem struct {
	Item string  `json:"item"`
	Cos  float64 `json:"cos"`
}

func main() {
	// Redis init
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("bad REDIS_URL: %v", err)
	}
	rdb = redis.NewClient(opt)

	r := gin.New()
	r.Use(gin.Recovery())

	// -------------------------
	// Ingest endpoints
	// -------------------------
	r.POST("/ingest/event", func(c *gin.Context) {
		var ev Event
		if err := c.ShouldBindJSON(&ev); err != nil {
			c.JSON(400, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if ev.UserKey == "" {
			ev.UserKey = "anon"
		}
		f := normPath(ev.FromPath)
		t := normPath(ev.ToPath)
		if isNoise(f) || isNoise(t) {
			c.JSON(200, gin.H{"ok": true, "skipped": true})
			return
		}

		// HINCRBY trans:{service}:{from} to 1
		if err := rdb.HIncrBy(ctx, kTrans(ev.Service, f), t, 1).Err(); err != nil {
			c.JSON(500, gin.H{"ok": false, "error": err.Error()})
			return
		}
		// HINCRBY tot:{service} from 1
		if err := rdb.HIncrBy(ctx, kTotal(ev.Service), f, 1).Err(); err != nil {
			c.JSON(500, gin.H{"ok": false, "error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"ok": true})
	})

	r.POST("/ingest/edge", func(c *gin.Context) {
		var ev EdgeEvent
		if err := c.ShouldBindJSON(&ev); err != nil {
			c.JSON(400, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if ev.UserKey == "" {
			ev.UserKey = "anon"
		}
		src := normPath(ev.SrcPath)
		dst := normPath(ev.DstPath)
		if isNoise(src) || isNoise(dst) {
			c.JSON(200, gin.H{"ok": true, "skipped": true})
			return
		}

		packed := pack(ev.DstService, dst)
		if err := rdb.HIncrBy(ctx, kTransAny(ev.SrcService, src), packed, 1).Err(); err != nil {
			c.JSON(500, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if err := rdb.HIncrBy(ctx, kTotalAny(ev.SrcService), src, 1).Err(); err != nil {
			c.JSON(500, gin.H{"ok": false, "error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"ok": true})
	})

	r.POST("/ingest/prefetch", func(c *gin.Context) {
		var ev PrefetchAttempt
		if err := c.ShouldBindJSON(&ev); err != nil {
			c.JSON(400, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if ev.UserKey == "" {
			ev.UserKey = "anon"
		}
		src := normPath(ev.SrcPath)
		dst := normPath(ev.DstPath)
		if isNoise(src) || isNoise(dst) {
			c.JSON(200, gin.H{"ok": true, "skipped": true})
			return
		}

		packed := pack(ev.DstService, dst)
		if err := rdb.HIncrBy(ctx, kTransPrefetch(ev.SrcService, src), packed, 1).Err(); err != nil {
			c.JSON(500, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if err := rdb.HIncrBy(ctx, kTotalPrefetch(ev.SrcService), src, 1).Err(); err != nil {
			c.JSON(500, gin.H{"ok": false, "error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"ok": true})
	})

	// -------------------------
	// Policy
	// -------------------------
	r.GET("/policy/next", func(c *gin.Context) {
	service := c.Query("service")
	rawPath := c.Query("path")

	limit := getenvInt("POLICY_LIMIT_DEFAULT", 3)
	if q := c.Query("limit"); q != "" {
		if v, err := strconv.Atoi(q); err == nil {
			limit = v
		}
	}
	if limit < 0 {
		limit = 0
	}
	if service == "" || rawPath == "" {
		c.JSON(400, gin.H{"error": "service and path required"})
		return
	}

	// normalized node key
	p := normPath(rawPath)
	if isNoise(p) {
		c.JSON(200, PolicyResp{NextPaths: []NextPath{}, MaxPrefetch: 0, MaxPrefetchTimeMS: 0})
		return
	}

	// detect tokens from *source* path
	srcHasID := reIntToken.MatchString(rawPath) || strings.Contains(p, "{id}")
	srcHasUUID := reUUIDToken.MatchString(rawPath) || strings.Contains(p, "{uuid}")

	// collected probs for candidates: packed "svc|path" -> prob
	markov := make(map[string]float64)

	// helper: apply (smoothed) multinomial counts into markov map
	applyCounts := func(counts map[string]int64) {
		var total int64
		for _, v := range counts {
			total += v
		}
		if total <= 0 {
			return
		}
		k := float64(len(counts))
		den := float64(total)
		if markovSmooth > 0 && k > 0 {
			den += markovSmooth * k
		}
		for key, cn := range counts {
			num := float64(cn)
			if markovSmooth > 0 {
				num += markovSmooth
			}
			prob := num / den
			if prob < minProb {
				continue
			}
			if cur, ok := markov[key]; !ok || prob > cur {
				markov[key] = prob
			}
		}
	}

	// 1) intra-service
	if trans, err := rdb.HGetAll(ctx, kTrans(service, p)).Result(); err == nil && len(trans) > 0 {
		counts := make(map[string]int64)
		for to, cntStr := range trans {
			cn, err := strconv.ParseInt(cntStr, 10, 64)
			if err != nil || cn <= 0 {
				continue
			}
			if dropSelfLoops && to == p {
				continue
			}
			counts[pack(service, to)] = cn
		}
		if len(counts) > 0 {
			applyCounts(counts)
		}
	}

	// 2) cross-service REAL
	if trans2, err := rdb.HGetAll(ctx, kTransAny(service, p)).Result(); err == nil && len(trans2) > 0 {
		counts := make(map[string]int64)
		for packed, cntStr := range trans2 {
			cn, err := strconv.ParseInt(cntStr, 10, 64)
			if err != nil || cn <= 0 {
				continue
			}
			if dropSelfLoops && packed == pack(service, p) {
				continue
			}
			counts[packed] = cn
		}
		if len(counts) > 0 {
			applyCounts(counts)
		}
	}

	// 3) OPTIONAL: prefetch attempts as weak hint
	if allowPrefetchAttemptsInPolicy {
		totalpStr, err := rdb.HGet(ctx, kTotalPrefetch(service), p).Result()
		if err == nil && totalpStr != "" {
			if totalp, err2 := strconv.ParseInt(totalpStr, 10, 64); err2 == nil && totalp > 0 {
				if trans2p, err3 := rdb.HGetAll(ctx, kTransPrefetch(service, p)).Result(); err3 == nil && len(trans2p) > 0 {
					for packed, cntStr := range trans2p {
						cn, err := strconv.ParseInt(cntStr, 10, 64)
						if err != nil || cn <= 0 {
							continue
						}
						prob := (float64(cn) / float64(totalp)) * prefetchAttemptWeight
						if prob < minProb {
							continue
						}
						if cur, ok := markov[packed]; !ok || prob > cur {
							markov[packed] = prob
						}
					}
				}
			}
		}
	}

	// 4) i2v (once)
	cands := getI2VCandidates(service, p)

	// function: filter candidate list by source-token compatibility
	filterOut := func(items []NextPath) []NextPath {
		if len(items) == 0 {
			return items
		}
		out := items[:0]
		for _, it := range items {
			pp := it.Path
			if strings.Contains(pp, "{id}") && !srcHasID {
				continue
			}
			if strings.Contains(pp, "{uuid}") && !srcHasUUID {
				continue
			}
			out = append(out, it)
		}
		return out
	}

	// Fallback: pure markov
	if len(cands) == 0 {
		items := make([]NextPath, 0, len(markov))
		for packed, prob := range markov {
			svc, pp := unpack(packed)
			if math.IsNaN(prob) || math.IsInf(prob, 0) {
				continue
			}
			items = append(items, NextPath{Service: svc, Path: pp, Score: prob})
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Score > items[j].Score })

		items = filterOut(items)

		if len(items) > limit {
			items = items[:limit]
		}
		if len(items) == 0 {
			c.JSON(200, PolicyResp{NextPaths: []NextPath{}, MaxPrefetch: 0, MaxPrefetchTimeMS: 0})
			return
		}

		c.JSON(200, PolicyResp{
			NextPaths:         items,
			MaxPrefetch:       defaultMaxPrefetch,
			MaxPrefetchTimeMS: defaultPrefetchBudgetMS,
		})
		return
	}

	// Hybrid scoring
	best := map[string]float64{} // packed -> score
	alpha := i2vAlpha

	// score i2v cands
	for _, it := range cands {
		key := pack(it.Service, it.Path)
		if dropSelfLoops && key == pack(service, p) {
			continue
		}
		prob := markov[key]
		score := alpha*it.Score + (1.0-alpha)*prob
		if cur, ok := best[key]; !ok || score > cur {
			best[key] = score
		}
	}

	// insurance: top markov edges
	type kv struct {
		Key  string
		Prob float64
	}
	mItems := make([]kv, 0, len(markov))
	for k, v := range markov {
		mItems = append(mItems, kv{Key: k, Prob: v})
	}
	sort.Slice(mItems, func(i, j int) bool { return mItems[i].Prob > mItems[j].Prob })

	insCap := maxInt(5, limit*3)
	if len(mItems) > insCap {
		mItems = mItems[:insCap]
	}
	for _, kv := range mItems {
		score := (1.0 - alpha) * kv.Prob
		if cur, ok := best[kv.Key]; !ok || score > cur {
			best[kv.Key] = score
		}
	}

	out := make([]NextPath, 0, len(best))
	for packed, score := range best {
		if math.IsNaN(score) || math.IsInf(score, 0) {
			continue
		}
		svc, pp := unpack(packed)
		out = append(out, NextPath{Service: svc, Path: pp, Score: score})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })

	out = filterOut(out)

	if len(out) > limit {
		out = out[:limit]
	}
	if len(out) == 0 {
		c.JSON(200, PolicyResp{NextPaths: []NextPath{}, MaxPrefetch: 0, MaxPrefetchTimeMS: 0})
		return
	}

	c.JSON(200, PolicyResp{
		NextPaths:         out,
		MaxPrefetch:       defaultMaxPrefetch,
		MaxPrefetchTimeMS: defaultPrefetchBudgetMS,
	})
})
	// -------------------------
	// Debug + misc
	// -------------------------
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	r.GET("/debug/policy_raw", func(c *gin.Context) {
		service := c.Query("service")
		path := c.Query("path")
		if service == "" || path == "" {
			c.JSON(400, gin.H{"error": "service and path required"})
			return
		}
		p := normPath(path)

		trans, _ := rdb.HGetAll(ctx, kTrans(service, p)).Result()
		trans2, _ := rdb.HGetAll(ctx, kTransAny(service, p)).Result()

		topTrans := topNHash(trans, 10)
		topTrans2 := topNHash(trans2, 10)

		c.JSON(200, gin.H{
			"p":          p,
			"trans_keys": len(trans),
			"trans2_keys": len(trans2),
			"top_trans":  topTrans,
			"top_trans2": topTrans2,
			"i2v":        getI2VCandidates(service, p)[:minInt(10, len(getI2VCandidates(service, p)))],
		})
	})

	// Prometheus metrics endpoint (you already scrape /metrics)
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	port := getenv("PORT", "8000")
	srv := &http.Server{
		Addr:              "0.0.0.0:" + port,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("core-go up on :%s redis=%s", port, redisURL)
	log.Fatal(srv.ListenAndServe())
}

// =========================
// Helpers
// =========================

func normPath(p string) string {
	if p == "" {
		return p
	}
	if isNoise(p) {
		return p
	}
	if p != "/" && strings.HasSuffix(p, "/") {
		p = strings.TrimSuffix(p, "/")
	}
	p = reUUID.ReplaceAllString(p, "/{uuid}$1")
    p = reInt.ReplaceAllString(p, "/{id}$1")
	return p
}

func isNoise(p string) bool {
	for _, pref := range noisePrefixes {
		if strings.HasPrefix(p, pref) {
			return true
		}
	}
	return false
}

func i2vKey(service, path string) string {
	return i2vPrefix + service + "::" + path
}

func parseNode(node string) (string, string, bool) {
	parts := strings.SplitN(node, "::", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func getI2VCandidates(service, path string) []NextPath {
	raw, err := rdb.Get(ctx, i2vKey(service, path)).Result()
	if err != nil || raw == "" {
		return nil
	}
	var data []i2vItem
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return nil
	}
	out := make([]NextPath, 0, minInt(i2vTopK, len(data)))
	for i := 0; i < len(data) && i < i2vTopK; i++ {
		n := data[i].Item
		if n == "" {
			continue
		}
		svc, p, ok := parseNode(n)
		if !ok {
			continue
		}
		out = append(out, NextPath{Service: svc, Path: p, Score: data[i].Cos})
	}
	return out
}

// Redis keys
func kTrans(service, fromPath string) string       { return "trans:" + service + ":" + fromPath }
func kTotal(service string) string                 { return "tot:" + service }
func kTransAny(srcService, fromPath string) string { return "trans2:" + srcService + ":" + fromPath }
func kTotalAny(service string) string              { return "tot2:" + service }
func kTransPrefetch(srcService, fromPath string) string {
	return "ptrans:" + srcService + ":" + fromPath
}
func kTotalPrefetch(service string) string { return "ptot:" + service }

func pack(svc, path string) string { return svc + "|" + path }
func unpack(v string) (string, string) {
	parts := strings.SplitN(v, "|", 2)
	if len(parts) != 2 {
		return v, ""
	}
	return parts[0], parts[1]
}

func topNHash(m map[string]string, n int) [][]any {
	type pair struct {
		K string
		V int64
	}
	arr := make([]pair, 0, len(m))
	for k, v := range m {
		iv, _ := strconv.ParseInt(v, 10, 64)
		arr = append(arr, pair{K: k, V: iv})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].V > arr[j].V })
	if len(arr) > n {
		arr = arr[:n]
	}
	out := make([][]any, 0, len(arr))
	for _, p := range arr {
		out = append(out, []any{p.K, p.V})
	}
	return out
}

// env helpers
func getenv(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}
func getenvInt(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}
func getenvFloat(k string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}
func getenvBool(k string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
