package engine

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/weeaa/jito-go/clients/searcher_client"
)

type Config struct {
	Regions     []string
	Key         solana.PrivateKey
	RPC         *rpc.Client
	Window      int
	MinLandRate float64
	Cooldown    time.Duration
}

type slot struct {
	region string
	client *searcher_client.Client
}

type tally struct {
	mu      sync.Mutex
	sent    uint64
	landed  uint64
	dropped uint64
	ring    []bool
	seen    map[string]struct{}
	pos     int
	filled  int
}

func (t *tally) markSent() {
	t.mu.Lock()
	t.sent++
	t.mu.Unlock()
}

func (t *tally) mark(id string, landed bool) (rate float64, full bool, fresh bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, dup := t.seen[id]; dup {
		return t.rateLocked(), t.filled == len(t.ring), false
	}
	t.seen[id] = struct{}{}
	if landed {
		t.landed++
	} else {
		t.dropped++
	}
	t.ring[t.pos] = landed
	t.pos = (t.pos + 1) % len(t.ring)
	if t.filled < len(t.ring) {
		t.filled++
	}
	return t.rateLocked(), t.filled == len(t.ring), true
}

func (t *tally) rateLocked() float64 {
	if t.filled == 0 {
		return 1
	}
	n := 0
	for i := 0; i < t.filled; i++ {
		if t.ring[i] {
			n++
		}
	}
	return float64(n) / float64(t.filled)
}

type Engine struct {
	cfg     Config
	log     *slog.Logger
	active  atomic.Pointer[slot]
	slots   map[string]*slot
	rtts    map[string]time.Duration
	tallies map[string]*tally
	picking atomic.Bool
	lastAt  atomic.Int64
}

func New(cfg Config) *Engine {
	if cfg.Window <= 0 {
		cfg.Window = 50
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = time.Minute
	}
	e := &Engine{
		cfg:     cfg,
		log:     slog.Default().With("component", "engine"),
		slots:   map[string]*slot{},
		rtts:    map[string]time.Duration{},
		tallies: map[string]*tally{},
	}
	for _, r := range cfg.Regions {
		e.tallies[r] = &tally{ring: make([]bool, cfg.Window), seen: map[string]struct{}{}}
	}
	return e
}

func (e *Engine) Start(ctx context.Context) error {
	for _, region := range e.cfg.Regions {
		began := time.Now()
		cl, err := searcher_client.New(ctx, region, e.cfg.RPC, e.cfg.RPC, e.cfg.Key, &tls.Config{})
		if err != nil {
			e.log.Warn("region auth failed", "region", region, "err", err)
			continue
		}
		e.slots[region] = &slot{region: region, client: cl}
		e.rtts[region] = time.Since(began)
		go e.watch(ctx, e.slots[region])
	}
	if len(e.slots) == 0 {
		return errors.New("no region authenticated")
	}
	e.choose("")
	return nil
}

func (e *Engine) choose(avoid string) {
	var best *slot
	for region, s := range e.slots {
		if region == avoid {
			continue
		}
		if best == nil || e.rtts[region] < e.rtts[best.region] {
			best = s
		}
	}
	if best == nil {
		best = e.slots[avoid]
	}
	if best == nil {
		return
	}
	e.active.Store(best)
	e.lastAt.Store(time.Now().UnixNano())
	e.log.Info("region selected", "region", best.region, "rtt", e.rtts[best.region])
}

func (e *Engine) Send(ctx context.Context, txs []*solana.Transaction) (string, error) {
	cur := e.active.Load()
	if cur == nil {
		return "", errors.New("no active region")
	}
	resp, err := cur.client.BroadcastBundle(txs)
	if err != nil {
		return "", fmt.Errorf("broadcast via %s: %w", cur.region, err)
	}
	e.tallies[cur.region].markSent()
	return resp.GetUuid(), nil
}

func (e *Engine) LandRate(region string) float64 {
	t, ok := e.tallies[region]
	if !ok {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rateLocked()
}

func (e *Engine) watch(ctx context.Context, s *slot) {
	for ctx.Err() == nil {
		sub, err := s.client.NewBundleSubscriptionResults()
		if err != nil {
			e.log.Warn("subscribe failed", "region", s.region, "err", err)
			e.sleep(ctx, 2*time.Second)
			continue
		}
		for {
			res, err := sub.Recv()
			if err != nil {
				e.log.Warn("result stream closed", "region", s.region, "err", err)
				break
			}
			switch {
			case res.GetProcessed() != nil, res.GetFinalized() != nil:
				e.settle(s.region, res.GetBundleId(), true)
			case res.GetDropped() != nil, res.GetRejected() != nil:
				e.settle(s.region, res.GetBundleId(), false)
			}
		}
		e.sleep(ctx, time.Second)
	}
}

func (e *Engine) settle(region, id string, landed bool) {
	rate, full, fresh := e.tallies[region].mark(id, landed)
	if !fresh || !full || rate >= e.cfg.MinLandRate {
		return
	}
	cur := e.active.Load()
	if cur == nil || cur.region != region {
		return
	}
	if time.Since(time.Unix(0, e.lastAt.Load())) < e.cfg.Cooldown {
		return
	}
	if !e.picking.CompareAndSwap(false, true) {
		return
	}
	defer e.picking.Store(false)
	e.log.Warn("land rate below threshold, re-picking", "region", region, "rate", rate, "min", e.cfg.MinLandRate)
	e.choose(region)
}

func (e *Engine) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
