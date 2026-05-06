package linear

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	LinearEventHeader = "Nats-Event-Type"
	LinearEventType   = "Linear"
	LinearTTLHeader   = "Nats-Linear-TTL"
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
