package linear

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/mlkem"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	LinearEventHeader              = "Nats-Event-Type"
	LinearEventType                = "Linear"
	LinearTTLHeader                = "Nats-Linear-TTL"
	LinearOutboxIDHeader           = "Nats-Linear-Outbox-Id"
	LinearDLQReasonHeader          = "Nats-Linear-DLQ-Reason"
	LinearDLQOriginalSubjectHeader = "Nats-Linear-Original-Subject"
	LinearPQCAlgorithmHeader       = "Nats-Linear-PQC-Alg"
	LinearPQCPublicKeyHeader       = "Nats-Linear-PQC-Public-Key"
	DPoPHeader                     = "DPoP"
	linearPQCAlgorithm             = "ML-KEM-768"
)

type Message struct {
	Subject string
	Reply   string
	Header  nats.Header

	mu        sync.Mutex
	payload   []byte
	destroyed bool
	timer     *time.Timer
	linear    bool
}

func (m *Message) Access() ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.destroyed {
		return nil, false
	}
	payload := append([]byte(nil), m.payload...)
	if m.linear {
		m.destroyLocked()
	}
	return payload, true
}

func (m *Message) Destroy() {
	m.mu.Lock()
	m.destroyLocked()
	m.mu.Unlock()
}

func (m *Message) destroyLocked() {
	if m.destroyed {
		return
	}
	if m.timer != nil {
		m.timer.Stop()
	}
	clear(m.payload)
	m.payload = nil
	m.destroyed = true
}

func Publish(nc *nats.Conn, subject string, payload []byte, ttl time.Duration) error {
	return PublishWithSecurity(nc, subject, payload, ttl, nil)
}

func PublishWithSecurity(nc *nats.Conn, subject string, payload []byte, ttl time.Duration, security *SecurityOptions) error {
	msg := nats.NewMsg(subject)
	msg.Header.Set(LinearEventHeader, LinearEventType)
	if ttl > 0 {
		msg.Header.Set(LinearTTLHeader, strconv.FormatInt(ttl.Milliseconds(), 10))
	}
	msg.Data = append([]byte(nil), payload...)
	if err := applySecurityHeaders(msg, security); err != nil {
		return err
	}
	return nc.PublishMsg(msg)
}

func Subscribe(nc *nats.Conn, subject string, cb func(*Message)) (*nats.Subscription, error) {
	return nc.Subscribe(subject, func(msg *nats.Msg) {
		isLinear := msg.Header.Get(LinearEventHeader) == LinearEventType
		lm := &Message{Subject: msg.Subject, Reply: msg.Reply, Header: msg.Header, payload: append([]byte(nil), msg.Data...), linear: isLinear}
		if isLinear {
			if ttl := parseTTL(msg.Header.Get(LinearTTLHeader)); ttl > 0 {
				lm.timer = time.AfterFunc(ttl, lm.Destroy)
			}
		}
		cb(lm)
	})
}

func parseTTL(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if ttl, err := time.ParseDuration(value); err == nil {
		return ttl
	}
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

type Publisher interface {
	PublishMsg(*nats.Msg) error
}

type SecurityOptions struct {
	TLSConfig  *tls.Config
	EnablePQC  bool
	DPoPKey    *ecdsa.PrivateKey
	DPoPIssuer string
}

type OutboxOptions struct {
	MaxAttempts int
	DLQSubject  string
	Security    *SecurityOptions
}

type OutboxEntry struct {
	ID       string
	Subject  string
	Payload  []byte
	TTL      time.Duration
	Attempts int
}

type Outbox struct {
	mu       sync.Mutex
	pub      Publisher
	dlq      string
	max      int
	security *SecurityOptions
	entries  []*OutboxEntry
	nextID   int64
}

func NewOutbox(pub Publisher, opts OutboxOptions) *Outbox {
	max := opts.MaxAttempts
	if max <= 0 {
		max = 3
	}
	return &Outbox{pub: pub, dlq: opts.DLQSubject, max: max, security: opts.Security}
}

func (o *Outbox) EnqueueLinear(subject string, payload []byte, ttl time.Duration) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.nextID++
	id := strconv.FormatInt(o.nextID, 10)
	o.entries = append(o.entries, &OutboxEntry{ID: id, Subject: subject, Payload: append([]byte(nil), payload...), TTL: ttl})
	return id
}

func (o *Outbox) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.entries)
}

func (o *Outbox) Flush() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.pub == nil {
		return nil
	}
	remaining := o.entries[:0]
	var firstErr error
	for _, entry := range o.entries {
		msg, buildErr := entry.message(o.security)
		if buildErr != nil {
			return buildErr
		}
		if err := o.pub.PublishMsg(msg); err != nil {
			entry.Attempts++
			if firstErr == nil {
				firstErr = err
			}
			if entry.Attempts >= o.max && o.dlq != "" {
				if dlqErr := o.pub.PublishMsg(entry.dlqMessage(o.dlq, err.Error())); dlqErr == nil {
					continue
				} else if firstErr == nil {
					firstErr = dlqErr
				}
			}
			remaining = append(remaining, entry)
		}
	}
	for i := len(remaining); i < len(o.entries); i++ {
		o.entries[i] = nil
	}
	o.entries = remaining
	return firstErr
}

func (e *OutboxEntry) message(security *SecurityOptions) (*nats.Msg, error) {
	msg := nats.NewMsg(e.Subject)
	msg.Header.Set(LinearEventHeader, LinearEventType)
	msg.Header.Set(LinearOutboxIDHeader, e.ID)
	if e.TTL > 0 {
		msg.Header.Set(LinearTTLHeader, strconv.FormatInt(e.TTL.Milliseconds(), 10))
	}
	msg.Data = append([]byte(nil), e.Payload...)
	if err := applySecurityHeaders(msg, security); err != nil {
		return nil, err
	}
	return msg, nil
}

func (e *OutboxEntry) dlqMessage(subject, reason string) *nats.Msg {
	msg := nats.NewMsg(subject)
	msg.Header.Set(LinearDLQReasonHeader, reason)
	msg.Header.Set(LinearDLQOriginalSubjectHeader, e.Subject)
	msg.Header.Set(LinearOutboxIDHeader, e.ID)
	msg.Data = append([]byte(nil), e.Payload...)
	return msg
}

func Connect(url string, security *SecurityOptions, opts ...nats.Option) (*nats.Conn, error) {
	if security != nil && security.TLSConfig != nil {
		opts = append([]nats.Option{nats.Secure(security.TLSConfig)}, opts...)
	}
	return nats.Connect(url, opts...)
}

func applySecurityHeaders(msg *nats.Msg, security *SecurityOptions) error {
	if security == nil {
		return nil
	}
	if security.EnablePQC {
		pub, err := ephemeralPQCLinearKey()
		if err != nil {
			return err
		}
		msg.Header.Set(LinearPQCAlgorithmHeader, linearPQCAlgorithm)
		msg.Header.Set(LinearPQCPublicKeyHeader, pub)
	}
	if security.DPoPKey != nil {
		token, err := dpopProof(security.DPoPKey, "NATS", msg.Subject, security.DPoPIssuer)
		if err != nil {
			return err
		}
		msg.Header.Set(DPoPHeader, token)
	}
	return nil
}

func ephemeralPQCLinearKey() (string, error) {
	key, err := mlkem.GenerateKey768()
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(key.EncapsulationKey().Bytes()), nil
}

func GenerateDPoPKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

type dpopHeader struct {
	Typ string  `json:"typ"`
	Alg string  `json:"alg"`
	JWK dpopJWK `json:"jwk"`
}

type dpopJWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type dpopClaims struct {
	HTM string `json:"htm"`
	HTU string `json:"htu"`
	IAT int64  `json:"iat"`
	JTI string `json:"jti"`
	ISS string `json:"iss,omitempty"`
}

func dpopProof(key *ecdsa.PrivateKey, method, uri, issuer string) (string, error) {
	header := dpopHeader{Typ: "dpop+jwt", Alg: "ES256", JWK: dpopJWK{Kty: "EC", Crv: "P-256", X: base64URLInt(key.X), Y: base64URLInt(key.Y)}}
	jti := make([]byte, 16)
	if _, err := rand.Read(jti); err != nil {
		return "", err
	}
	claims := dpopClaims{HTM: method, HTU: uri, IAT: time.Now().Unix(), JTI: base64.RawURLEncoding.EncodeToString(jti), ISS: issuer}
	hb, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	cb, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	unsigned := base64.RawURLEncoding.EncodeToString(hb) + "." + base64.RawURLEncoding.EncodeToString(cb)
	digest := sha256.Sum256([]byte(unsigned))
	r, ss, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		return "", err
	}
	sig := append(padInt(r, 32), padInt(ss, 32)...)
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func base64URLInt(v *big.Int) string {
	return base64.RawURLEncoding.EncodeToString(padInt(v, 32))
}

func padInt(v *big.Int, size int) []byte {
	b := v.Bytes()
	if len(b) >= size {
		return b[len(b)-size:]
	}
	out := make([]byte, size)
	copy(out[size-len(b):], b)
	return out
}

type LinearQueueConfig struct {
	URL            string
	Subject        string
	QueueGroup     string
	DestroySubject string
	ReconnectFor   time.Duration
	ReconnectEvery time.Duration
	Security       *SecurityOptions
}

func StartLinearQueue(ctx context.Context, cfg LinearQueueConfig, handler func(*Message)) error {
	wait := cfg.ReconnectEvery
	if wait <= 0 {
		wait = time.Second
	}
	maxReconnects := -1
	if cfg.ReconnectFor > 0 {
		maxReconnects = int(cfg.ReconnectFor / wait)
		if maxReconnects < 1 {
			maxReconnects = 1
		}
	}
	destroyed := make(chan struct{})
	closed := make(chan struct{})
	nc, err := Connect(cfg.URL, cfg.Security, nats.ReconnectWait(wait), nats.MaxReconnects(maxReconnects), nats.ClosedHandler(func(*nats.Conn) { closeOnce(closed) }))
	if err != nil {
		return err
	}
	if _, err := nc.QueueSubscribe(cfg.Subject, cfg.QueueGroup, func(msg *nats.Msg) {
		isLinear := msg.Header.Get(LinearEventHeader) == LinearEventType
		lm := &Message{Subject: msg.Subject, Reply: msg.Reply, Header: msg.Header, payload: append([]byte(nil), msg.Data...), linear: isLinear}
		if isLinear {
			if ttl := parseTTL(msg.Header.Get(LinearTTLHeader)); ttl > 0 {
				lm.timer = time.AfterFunc(ttl, lm.Destroy)
			}
		}
		handler(lm)
	}); err != nil {
		nc.Close()
		return err
	}
	if cfg.DestroySubject != "" {
		if _, err := nc.Subscribe(cfg.DestroySubject, func(*nats.Msg) { closeOnce(destroyed) }); err != nil {
			nc.Close()
			return err
		}
	}
	if err := nc.Flush(); err != nil {
		nc.Close()
		return err
	}
	select {
	case <-ctx.Done():
		nc.Close()
		return ctx.Err()
	case <-destroyed:
		nc.Drain()
		return nil
	case <-closed:
		return nats.ErrConnectionClosed
	}
}

func closeOnce(ch chan struct{}) {
	defer func() { recover() }()
	close(ch)
}
