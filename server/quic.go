// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0.

package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

const defaultQuicPath = "/nats"

type srvQuic struct {
	server      *webtransport.Server
	packetConn  net.PacketConn
	listenerErr error
	host        string
	port        int
	path        string
}

type quicConn struct {
	*webtransport.Stream
	session *webtransport.Session
}

func (c *quicConn) LocalAddr() net.Addr {
	return c.session.LocalAddr()
}

func (c *quicConn) RemoteAddr() net.Addr {
	return c.session.RemoteAddr()
}

func (s *Server) QuicURL() string {
	opts := s.getOpts()
	var host string
	var port int
	path := opts.Quic.Path
	if path == _EMPTY_ {
		path = defaultQuicPath
	}
	s.mu.Lock()
	if s.quic.port != 0 {
		host = s.quic.host
		port = s.quic.port
	}
	s.mu.Unlock()
	if host == _EMPTY_ {
		host = opts.Quic.Host
	}
	if port == 0 {
		port = opts.Quic.Port
	}
	return fmt.Sprintf("nats+quic://%s%s", net.JoinHostPort(host, fmt.Sprintf("%d", port)), path)
}

func validateQuicOptions(o *Options) error {
	qo := &o.Quic
	if qo.Port == 0 {
		return nil
	}
	if qo.TLSConfig == nil {
		return errors.New("quic requires TLS configuration")
	}
	if qo.Path != _EMPTY_ && !strings.HasPrefix(qo.Path, "/") {
		return errors.New("quic path must start with /")
	}
	return nil
}

func (s *Server) startQuicServer() {
	if s.isShuttingDown() {
		return
	}
	sopts := s.getOpts()
	o := &sopts.Quic
	host := o.Host
	if host == _EMPTY_ {
		host = sopts.Host
	}
	path := o.Path
	if path == _EMPTY_ {
		path = defaultQuicPath
	}

	listenPort := o.Port
	if listenPort == RANDOM_PORT {
		listenPort = 0
	}
	hp := net.JoinHostPort(host, strconv.Itoa(listenPort))
	pc, err := net.ListenPacket("udp", hp)
	s.mu.Lock()
	s.quic.listenerErr = err
	if err != nil {
		s.mu.Unlock()
		s.Fatalf("Unable to listen for QUIC client connections: %v", err)
		return
	}

	port := pc.LocalAddr().(*net.UDPAddr).Port
	if o.Port == 0 || o.Port == RANDOM_PORT {
		o.Port = port
	}

	tlsConf := o.TLSConfig.Clone()
	tlsConf.NextProtos = appendNextProtoH3(tlsConf.NextProtos)
	h3 := &http3.Server{
		Addr:      net.JoinHostPort(host, strconv.Itoa(port)),
		TLSConfig: tlsConf,
	}
	qs := &webtransport.Server{
		H3:                h3,
		ReorderingTimeout: o.HandshakeTimeout,
	}
	webtransport.ConfigureHTTP3Server(h3)

	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		s.handleQuicUpgrade(qs, w, r)
	})
	h3.Handler = mux

	s.quic.server = qs
	s.quic.packetConn = pc
	s.quic.host = host
	s.quic.port = port
	s.quic.path = path
	s.mu.Unlock()

	s.Noticef("Listening for QUIC client connections on %s", net.JoinHostPort(host, strconv.Itoa(port)))
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		if err := qs.Serve(pc); err != nil && !s.isShuttingDown() {
			s.Errorf("QUIC listener error: %v", err)
		}
		s.Debugf("QUIC accept loop exiting..")
		s.done <- true
	})
}

func (s *Server) handleQuicUpgrade(qs *webtransport.Server, w http.ResponseWriter, r *http.Request) {
	sess, err := qs.Upgrade(w, r)
	if err != nil {
		s.Debugf("QUIC upgrade error from %s: %v", r.RemoteAddr, err)
		return
	}
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		s.createQuicSessionClient(sess)
	})
}

func (s *Server) createQuicSessionClient(sess *webtransport.Session) {
	str, err := sess.OpenStreamSync(context.Background())
	if err != nil {
		s.Debugf("QUIC stream open error from %s: %v", sess.RemoteAddr(), err)
		return
	}
	conn := &quicConn{Stream: str, session: sess}
	s.reloadMu.RLock()
	s.createQuicClient(conn)
	s.reloadMu.RUnlock()
	s.acceptQuicSession(sess)
}

func (s *Server) acceptQuicSession(sess *webtransport.Session) {
	for {
		str, err := sess.AcceptStream(context.Background())
		if err != nil {
			return
		}
		conn := &quicConn{Stream: str, session: sess}
		if !s.startGoRoutine(func() {
			s.reloadMu.RLock()
			s.createQuicClient(conn)
			s.reloadMu.RUnlock()
			s.grWG.Done()
		}) {
			conn.Close()
			return
		}
	}
}

func (s *Server) createQuicClient(conn net.Conn) *client {
	opts := s.getOpts()
	maxPay := int32(opts.MaxPayload)
	maxSubs := int32(opts.MaxSubs)
	if maxSubs == 0 {
		maxSubs = -1
	}
	now := time.Now().UTC()

	c := &client{srv: s, nc: conn, opts: defaultOpts, mpay: maxPay, msubs: maxSubs, start: now, last: now}
	c.registerWithAccount(s.globalAccount())

	var info Info
	var authRequired bool

	s.mu.Lock()
	info = s.copyInfo()
	// QUIC/WebTransport already provides TLS. Do not request an additional NATS TLS handshake.
	if info.TLSRequired {
		info.TLSRequired = false
		info.TLSAvailable = true
	}
	if s.nonceRequired() {
		var raw [nonceLen]byte
		nonce := raw[:]
		s.generateNonce(nonce)
		info.Nonce = string(nonce)
	}
	c.nonce = []byte(info.Nonce)
	authRequired = info.AuthRequired
	if info.AuthRequired && opts.NoAuthUser != _EMPTY_ && opts.NoAuthUser != s.sysAccOnlyNoAuthUser {
		info.AuthRequired = false
	}
	s.totalClients++
	s.mu.Unlock()

	c.mu.Lock()
	if authRequired {
		c.flags.set(expectConnect)
	}
	c.flags.set(handshakeComplete)
	c.initClient()
	c.Debugf("QUIC client connection created")
	c.sendProtoNow(c.generateClientInfoJSON(info, true))
	c.mu.Unlock()

	s.mu.Lock()
	if !s.isRunning() || s.ldm {
		if s.isShuttingDown() {
			conn.Close()
		}
		s.mu.Unlock()
		return c
	}
	if opts.MaxConn < 0 || (opts.MaxConn > 0 && len(s.clients) >= opts.MaxConn) {
		s.mu.Unlock()
		c.maxConnExceeded()
		return nil
	}
	s.clients[c.cid] = c
	s.mu.Unlock()

	c.mu.Lock()
	if c.isClosed() {
		c.mu.Unlock()
		c.closeConnection(WriteError)
		return nil
	}
	if authRequired {
		c.setAuthTimer(secondsToDuration(opts.AuthTimeout))
	}
	c.setPingTimer()
	s.startGoRoutine(func() { c.readLoop(nil) })
	s.startGoRoutine(func() { c.writeLoop() })
	c.mu.Unlock()

	return c
}

func (s *Server) closeQuicServer() int {
	qs := s.quic.server
	pc := s.quic.packetConn
	if qs != nil || pc != nil {
		s.quic.server = nil
		s.quic.packetConn = nil
	}
	if qs != nil {
		qs.Close()
	}
	if pc != nil {
		pc.Close()
	}
	if qs != nil || pc != nil {
		return 1
	}
	return 0
}

func appendNextProtoH3(protos []string) []string {
	for _, proto := range protos {
		if proto == http3.NextProtoH3 {
			return protos
		}
	}
	return append(protos, http3.NextProtoH3)
}
