// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.

package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/quic-go/webtransport-go"
)

func TestQuicTransportReceivesCoreNATSCommands(t *testing.T) {
	cert, err := tls.LoadX509KeyPair("configs/certs/server.pem", "configs/certs/key.pem")
	if err != nil {
		t.Fatalf("Error loading test certificate: %v", err)
	}

	o := DefaultTestOptions
	o.Port = RANDOM_PORT
	o.Quic.Host = "127.0.0.1"
	o.Quic.Port = RANDOM_PORT
	o.Quic.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	s := RunServer(&o)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error creating TCP NATS client: %v", err)
	}
	defer nc.Close()

	d := webtransport.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	_, sess, err := d.Dial(context.Background(), fmt.Sprintf("https://127.0.0.1:%d/nats", o.Quic.Port), nil)
	if err != nil {
		t.Fatalf("Error dialing QUIC/WebTransport endpoint: %v", err)
	}
	defer sess.CloseWithError(0, "")

	str, err := sess.AcceptStream(context.Background())
	if err != nil {
		t.Fatalf("Error accepting server QUIC/WebTransport stream: %v", err)
	}
	defer str.Close()
	if err := str.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("Error setting stream deadline: %v", err)
	}
	r := bufio.NewReader(str)

	info, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading INFO: %v", err)
	}
	if !strings.HasPrefix(info, "INFO ") {
		t.Fatalf("Expected INFO, got %q", info)
	}

	if _, err := str.Write([]byte("CONNECT {\"verbose\":false}\r\nPING\r\n")); err != nil {
		t.Fatalf("Error writing CONNECT/PING: %v", err)
	}
	if line, err := r.ReadString('\n'); err != nil || line != "PONG\r\n" {
		t.Fatalf("Expected PONG, got %q, err=%v", line, err)
	}

	if _, err := str.Write([]byte("SUB quic.test 1\r\nPING\r\n")); err != nil {
		t.Fatalf("Error writing SUB/PING: %v", err)
	}
	if line, err := r.ReadString('\n'); err != nil || line != "PONG\r\n" {
		t.Fatalf("Expected subscription PONG, got %q, err=%v", line, err)
	}

	nc.Publish("quic.test", []byte("hello"))
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error flushing TCP publish: %v", err)
	}

	msgLine, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading MSG line: %v", err)
	}
	if msgLine != "MSG quic.test 1 5\r\n" {
		t.Fatalf("Expected MSG line, got %q", msgLine)
	}
	payload, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("Error reading MSG payload: %v", err)
	}
	if payload != "hello\r\n" {
		t.Fatalf("Expected payload, got %q", payload)
	}
}
