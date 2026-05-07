package linear

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestLinearAccessDestroysPayload(t *testing.T) {
	msg := &Message{payload: []byte("secret"), linear: true}

	payload, ok := msg.Access()
	if !ok {
		t.Fatalf("expected first access to succeed")
	}
	if !bytes.Equal(payload, []byte("secret")) {
		t.Fatalf("unexpected payload: %q", payload)
	}
	if payload, ok := msg.Access(); ok || payload != nil {
		t.Fatalf("expected linear payload to be destroyed after first access, got ok=%v payload=%q", ok, payload)
	}
}

func TestLinearTTLDestroysUnreadPayload(t *testing.T) {
	msg := &Message{payload: []byte("expires"), linear: true}
	msg.timer = time.AfterFunc(10*time.Millisecond, msg.Destroy)

	time.Sleep(50 * time.Millisecond)
	if payload, ok := msg.Access(); ok || payload != nil {
		t.Fatalf("expected payload to be destroyed by TTL, got ok=%v payload=%q", ok, payload)
	}
}

func TestNonLinearAccessDoesNotDestroyPayload(t *testing.T) {
	msg := &Message{payload: []byte("reusable")}

	for i := 0; i < 2; i++ {
		payload, ok := msg.Access()
		if !ok {
			t.Fatalf("expected access %d to succeed", i+1)
		}
		if !bytes.Equal(payload, []byte("reusable")) {
			t.Fatalf("unexpected payload on access %d: %q", i+1, payload)
		}
	}
}

func TestParseTTLAcceptsDurationAndMilliseconds(t *testing.T) {
	if ttl := parseTTL("25ms"); ttl != 25*time.Millisecond {
		t.Fatalf("expected 25ms, got %v", ttl)
	}
	if ttl := parseTTL("25"); ttl != 25*time.Millisecond {
		t.Fatalf("expected numeric TTL as milliseconds, got %v", ttl)
	}
	if ttl := parseTTL("bad"); ttl != 0 {
		t.Fatalf("expected invalid TTL to be ignored, got %v", ttl)
	}
}

type fakePublisher struct {
	failures int
	msgs     []*nats.Msg
}

func (f *fakePublisher) PublishMsg(msg *nats.Msg) error {
	f.msgs = append(f.msgs, msg)
	if f.failures > 0 {
		f.failures--
		return errors.New("publish failed")
	}
	return nil
}

func TestOutboxFlushPublishesAndRemovesEntry(t *testing.T) {
	pub := &fakePublisher{}
	outbox := NewOutbox(pub, OutboxOptions{})
	outbox.EnqueueLinear("linear.out", []byte("payload"), 25*time.Millisecond)

	if err := outbox.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if outbox.Len() != 0 {
		t.Fatalf("expected outbox to be empty, got %d", outbox.Len())
	}
	if len(pub.msgs) != 1 {
		t.Fatalf("expected one published message, got %d", len(pub.msgs))
	}
	msg := pub.msgs[0]
	if msg.Subject != "linear.out" || string(msg.Data) != "payload" {
		t.Fatalf("unexpected published message: subject=%q data=%q", msg.Subject, msg.Data)
	}
	if got := msg.Header.Get(LinearEventHeader); got != LinearEventType {
		t.Fatalf("expected linear header, got %q", got)
	}
	if got := msg.Header.Get(LinearTTLHeader); got != "25" {
		t.Fatalf("expected TTL in milliseconds, got %q", got)
	}
}

func TestOutboxMovesEntryToDLQAfterMaxAttempts(t *testing.T) {
	pub := &fakePublisher{failures: 1}
	outbox := NewOutbox(pub, OutboxOptions{MaxAttempts: 1, DLQSubject: "linear.dlq"})
	outbox.EnqueueLinear("linear.out", []byte("payload"), 0)

	if err := outbox.Flush(); err == nil {
		t.Fatalf("expected flush to report the original publish failure")
	}
	if outbox.Len() != 0 {
		t.Fatalf("expected failed entry to be removed after DLQ publish, got %d", outbox.Len())
	}
	if len(pub.msgs) != 2 {
		t.Fatalf("expected original publish and DLQ publish, got %d", len(pub.msgs))
	}
	dlq := pub.msgs[1]
	if dlq.Subject != "linear.dlq" {
		t.Fatalf("expected DLQ subject, got %q", dlq.Subject)
	}
	if got := dlq.Header.Get(LinearDLQOriginalSubjectHeader); got != "linear.out" {
		t.Fatalf("expected original subject header, got %q", got)
	}
	if got := dlq.Header.Get(LinearDLQReasonHeader); got == "" {
		t.Fatalf("expected DLQ reason header")
	}
}

func TestPublishWithSecurityAddsPQCAndDPoPHeaders(t *testing.T) {
	pub := &fakePublisher{}
	key, err := GenerateDPoPKey()
	if err != nil {
		t.Fatalf("generate dpop key: %v", err)
	}
	security := &SecurityOptions{EnablePQC: true, DPoPKey: key, DPoPIssuer: "issuer"}

	outbox := NewOutbox(pub, OutboxOptions{Security: security})
	outbox.EnqueueLinear("linear.secure", []byte("payload"), 0)
	if err := outbox.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if len(pub.msgs) != 1 {
		t.Fatalf("expected one published message, got %d", len(pub.msgs))
	}
	msg := pub.msgs[0]
	if got := msg.Header.Get(LinearPQCAlgorithmHeader); got != linearPQCAlgorithm {
		t.Fatalf("expected PQC algorithm header %q, got %q", linearPQCAlgorithm, got)
	}
	if got := msg.Header.Get(LinearPQCPublicKeyHeader); got == "" {
		t.Fatalf("expected ephemeral PQC public key header")
	}
	if got := msg.Header.Get(DPoPHeader); strings.Count(got, ".") != 2 {
		t.Fatalf("expected compact DPoP JWT, got %q", got)
	}
}
