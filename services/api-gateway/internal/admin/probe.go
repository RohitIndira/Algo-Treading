package admin

// M3 — credential health: the live probe and the pre-market sweep.
//
// The stale-age dot in the fleet grid is a heuristic; this is the truth: a
// real strict-endpoint broker call (GetFundLimit — one of the endpoints that
// AU004s on a dead session even when the JWT looks fresh) made with the
// user's STORED credential, fetched decrypted over the existing user-config
// gRPC (the key never leaves user-config).
//
// Verdicts:
//	WORKS         — the stored credential drives live broker calls right now
//	AUTH_EXPIRED  — broker rejected the session (AU004 family): platform
//	                re-login required; trail/entries/arming are all failing
//	NO_CREDENTIAL — nothing stored (or force-expired)
//	ERROR         — broker/infra error, verdict unknown (detail attached)

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// probeTimeout bounds one broker call; the sweep is sequential and small
// (a handful of users), so per-call bounding is enough.
const probeTimeout = 8 * time.Second

// ProbeVerdict classifies one credential probe.
type ProbeVerdict struct {
	UserID    string    `json:"user_id"`
	Verdict   string    `json:"verdict"` // WORKS | AUTH_EXPIRED | NO_CREDENTIAL | ERROR
	Detail    string    `json:"detail,omitempty"`
	Source    string    `json:"source,omitempty"` // WEB / IOS / AND
	ProbedAt  time.Time `json:"probed_at"`
	LatencyMS int64     `json:"latency_ms"`
}

// credentialsFetcher is the slice of the user-config client the prober
// needs — an interface so tests can stub the gRPC hop.
type credentialsFetcher interface {
	GetUserCredentials(ctx context.Context, req *pb.GetUserCredentialsRequest) (*pb.GetUserCredentialsResponse, error)
}

// brokerProbe is the single strict call — injectable for tests.
type brokerProbe func(ctx context.Context, auth *indiraClient.AuthContext) error

// Prober performs live credential checks and remembers the last sweep so the
// attention queue can surface failures between sweeps. In-memory only: a
// restart forgets, the next sweep (or on-demand probe) relearns.
type Prober struct {
	creds credentialsFetcher
	probe brokerProbe

	mu        sync.Mutex
	lastSweep map[string]ProbeVerdict
	sweptAt   time.Time
}

// NewProber wires the production dependencies: the gateway's user-config
// gRPC client and a shared stateless indira client.
func NewProber(creds credentialsFetcher, ic *indiraClient.Client) *Prober {
	return &Prober{
		creds: creds,
		probe: func(ctx context.Context, auth *indiraClient.AuthContext) error {
			_, err := ic.GetFundLimit(ctx, auth)
			return err
		},
		lastSweep: map[string]ProbeVerdict{},
	}
}

// Probe runs one live check for userID. Named return so the deferred
// latency stamp lands on what the caller actually receives.
//
// On AUTH_EXPIRED it re-fetches the credential once and retries: the store
// moves under live probes (token_capture refreshes it whenever the user's
// own app sends a newer token — observed live 2026-08-31: S4450 probed dead
// at 16:25:23, token refreshed 16:26:19, probed alive 16:26:48). If a newer
// token just landed, the verdict self-corrects within the same call; if the
// token is unchanged, dead is dead.
func (p *Prober) Probe(ctx context.Context, userID string) (v ProbeVerdict) {
	v = ProbeVerdict{UserID: userID, ProbedAt: time.Now()}
	start := time.Now()
	defer func() { v.LatencyMS = time.Since(start).Milliseconds() }()

	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	auth, verdict, detail := p.fetchAuth(ctx, userID)
	if verdict != "" {
		v.Verdict, v.Detail = verdict, detail
		return v
	}
	v.Source = auth.Source

	if p.classify(ctx, auth, &v); v.Verdict != "AUTH_EXPIRED" {
		return v
	}
	// One refetch+retry: has a newer token landed since we read the store?
	auth2, verdict2, _ := p.fetchAuth(ctx, userID)
	if verdict2 != "" || auth2.BearerToken == auth.BearerToken {
		return v // nothing newer — the session is genuinely dead
	}
	v.Detail = ""
	p.classify(ctx, auth2, &v)
	if v.Verdict == "WORKS" {
		v.Detail = "recovered on retry — a fresher token had just been stored"
	}
	return v
}

// fetchAuth reads the decrypted credential; a non-empty verdict short-circuits.
func (p *Prober) fetchAuth(ctx context.Context, userID string) (*indiraClient.AuthContext, string, string) {
	return fetchAuthFor(ctx, p.creds, userID)
}

// fetchAuthFor is the shared credential→auth resolution (M3 probe, M5
// reconcile/mirror): a non-empty verdict (ERROR | NO_CREDENTIAL)
// short-circuits.
func fetchAuthFor(ctx context.Context, creds credentialsFetcher, userID string) (*indiraClient.AuthContext, string, string) {
	resp, err := creds.GetUserCredentials(ctx, &pb.GetUserCredentialsRequest{UserId: userID})
	switch {
	case err != nil:
		return nil, "ERROR", "credentials fetch: " + err.Error()
	case resp == nil || !resp.Success || resp.IndiraAuth == nil || resp.IndiraAuth.BearerToken == "":
		detail := ""
		if resp != nil && resp.Error != nil {
			detail = resp.Error.Message
		}
		return nil, "NO_CREDENTIAL", detail
	}
	return &indiraClient.AuthContext{
		UserId:      resp.IndiraAuth.UserId,
		AppId:       resp.IndiraAuth.AppId,
		Source:      resp.IndiraAuth.Source,
		BearerToken: resp.IndiraAuth.BearerToken,
	}, "", ""
}

// classify runs the strict broker call and writes the verdict into v.
func (p *Prober) classify(ctx context.Context, auth *indiraClient.AuthContext, v *ProbeVerdict) {
	err := p.probe(ctx, auth)
	switch {
	case err == nil:
		v.Verdict = "WORKS"
	case errors.Is(err, indiraClient.ErrAuthExpired):
		v.Verdict, v.Detail = "AUTH_EXPIRED", "broker rejected the session — platform re-login required"
	default:
		msg := err.Error()
		if len(msg) > 160 {
			msg = msg[:160]
		}
		v.Verdict, v.Detail = "ERROR", msg
	}
}

// Sweep probes every given user sequentially and stores the results for the
// attention queue. Returns the verdicts in input order.
func (p *Prober) Sweep(ctx context.Context, userIDs []string) []ProbeVerdict {
	out := make([]ProbeVerdict, 0, len(userIDs))
	for _, uid := range userIDs {
		out = append(out, p.Probe(ctx, uid))
	}
	p.mu.Lock()
	p.sweptAt = time.Now()
	for _, v := range out {
		p.lastSweep[v.UserID] = v
	}
	p.mu.Unlock()
	return out
}

// LastSweep returns a copy of the most recent sweep results.
func (p *Prober) LastSweep() (map[string]ProbeVerdict, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make(map[string]ProbeVerdict, len(p.lastSweep))
	for k, v := range p.lastSweep {
		cp[k] = v
	}
	return cp, p.sweptAt
}

// StartDailySweep runs the pre-market sweep at 08:30 IST every day: probe
// every user holding an active strategy, so a dead session is a known,
// queued CRITICAL 45 minutes before the bell — not a lost signal at 10:48
// (SHREEPUSHK, 2026-08-27). usersFn resolves the current active-user set at
// fire time; results land in LastSweep for the attention queue and are
// logged loudly per failure.
func (p *Prober) StartDailySweep(ctx context.Context, ist *time.Location, usersFn func(context.Context) ([]string, error)) {
	go func() {
		for {
			now := time.Now().In(ist)
			next := time.Date(now.Year(), now.Month(), now.Day(), 8, 30, 0, 0, ist)
			if !next.After(now) {
				next = next.Add(24 * time.Hour)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Until(next)):
			}

			users, err := usersFn(ctx)
			if err != nil {
				log.Printf("admin: pre-market sweep skipped — user list failed: %v", err)
				continue
			}
			results := p.Sweep(ctx, users)
			for _, v := range results {
				if v.Verdict != "WORKS" {
					log.Printf("⚠ PRE-MARKET CREDENTIAL SWEEP: %s → %s %s", v.UserID, v.Verdict, v.Detail)
				}
			}
			log.Printf("admin: pre-market credential sweep done — %d users probed", len(results))
		}
	}()
}

// sweepFailuresAsAttention converts stale sweep knowledge into queue items.
func (p *Prober) sweepFailuresAsAttention() []AttentionItem {
	results, at := p.LastSweep()
	if at.IsZero() {
		return nil
	}
	var items []AttentionItem
	for _, v := range results {
		if v.Verdict == "WORKS" {
			continue
		}
		items = append(items, AttentionItem{
			Severity: "CRITICAL", Kind: "CREDENTIAL_PROBE_FAILED", UserID: v.UserID,
			Detail: fmt.Sprintf("live probe %s at %s: %s", v.Verdict, at.Format("15:04"), v.Detail),
			Module: "M3",
		})
	}
	return items
}
