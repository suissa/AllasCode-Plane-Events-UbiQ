package server

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestLinearEventDeliversToSingleSubscriber(t *testing.T) {
	s := RunRandClientPortServer(t)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sub1, err := nc.SubscribeSync("linear.single")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	sub2, err := nc.SubscribeSync("linear.single")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	msg := nats.NewMsg("linear.single")
	msg.Header.Set(LinearEventHeader, LinearEventType)
	msg.Data = []byte("only once")
	if err := nc.PublishMsg(msg); err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	received := 0
	if _, err := sub1.NextMsg(250 * time.Millisecond); err == nil {
		received++
	} else if err != nats.ErrTimeout {
		t.Fatalf("unexpected sub1 error: %v", err)
	}
	if _, err := sub2.NextMsg(250 * time.Millisecond); err == nil {
		received++
	} else if err != nats.ErrTimeout {
		t.Fatalf("unexpected sub2 error: %v", err)
	}
	if received != 1 {
		t.Fatalf("expected exactly one subscriber to receive the linear event, got %d", received)
	}
}

func TestNonLinearEventPreservesFanout(t *testing.T) {
	s := RunRandClientPortServer(t)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sub1, err := nc.SubscribeSync("linear.regular")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	sub2, err := nc.SubscribeSync("linear.regular")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if err := nc.Publish("linear.regular", []byte("fanout")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if _, err := sub1.NextMsg(time.Second); err != nil {
		t.Fatalf("expected sub1 to receive regular event: %v", err)
	}
	if _, err := sub2.NextMsg(time.Second); err != nil {
		t.Fatalf("expected sub2 to receive regular event: %v", err)
	}
}
