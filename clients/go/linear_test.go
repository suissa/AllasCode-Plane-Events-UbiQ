package linear

import (
	"bytes"
	"testing"
	"time"
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
