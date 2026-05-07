package linear

import (
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
	headers := nats.Header{}
	headers.Set(LinearEventHeader, LinearEventType)
	if ttl > 0 {
		headers.Set(LinearTTLHeader, strconv.FormatInt(ttl.Milliseconds(), 10))
	}
	return nc.PublishMsg(&nats.Msg{Subject: subject, Header: headers, Data: payload})
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

type OutboxOptions struct {
	MaxAttempts int
	DLQSubject  string
}

type OutboxEntry struct {
	ID       string
	Subject  string
	Payload  []byte
	TTL      time.Duration
	Attempts int
}

type Outbox struct {
	mu      sync.Mutex
	pub     Publisher
	dlq     string
	max     int
	entries []*OutboxEntry
	nextID  int64
}

func NewOutbox(pub Publisher, opts OutboxOptions) *Outbox {
	max := opts.MaxAttempts
	if max <= 0 {
		max = 3
	}
	return &Outbox{pub: pub, dlq: opts.DLQSubject, max: max}
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
		if err := o.pub.PublishMsg(entry.message()); err != nil {
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

func (e *OutboxEntry) message() *nats.Msg {
	msg := nats.NewMsg(e.Subject)
	msg.Header.Set(LinearEventHeader, LinearEventType)
	msg.Header.Set(LinearOutboxIDHeader, e.ID)
	if e.TTL > 0 {
		msg.Header.Set(LinearTTLHeader, strconv.FormatInt(e.TTL.Milliseconds(), 10))
	}
	msg.Data = append([]byte(nil), e.Payload...)
	return msg
}

func (e *OutboxEntry) dlqMessage(subject, reason string) *nats.Msg {
	msg := nats.NewMsg(subject)
	msg.Header.Set(LinearDLQReasonHeader, reason)
	msg.Header.Set(LinearDLQOriginalSubjectHeader, e.Subject)
	msg.Header.Set(LinearOutboxIDHeader, e.ID)
	msg.Data = append([]byte(nil), e.Payload...)
	return msg
}
