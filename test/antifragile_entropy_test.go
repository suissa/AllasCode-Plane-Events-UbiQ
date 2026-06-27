// Copyright 2025 The AllasCode/UbiQ Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Antifragile Entropy Tests — Planes/Events/AllasCode-Plane-Events-UbiQ
//
// Implementa os três cenários descritos em:
//   Planes/Events/hyper-queues/tests/performance/Antifragile-Entropy.md
//
// Cenários:
//   1. Teste de Carga    (Load)        — TestUbiQ_LoadTest
//   2. Teste de Estresse (Stress)      — TestUbiQ_StressTest
//   3. Resiliência Antifrágil (Entropy)— TestUbiQ_AntifragileEntropyTest
//
// Pré-requisitos:
//   - Servidor UbiQ (NATS-compatible) rodando na porta definida por UBIQ_TEST_PORT
//     (padrão: porta aleatória inicializada pelo RunServer em memória).
//   - Para o teste Antifragile, o servidor embutido no próprio pacote é suficiente.
//
// Uso:
//   go test ./test/ -run TestUbiQ_LoadTest          -v -timeout 2m
//   go test ./test/ -run TestUbiQ_StressTest        -v -timeout 5m
//   go test ./test/ -run TestUbiQ_AntifragileEntropy -v -timeout 5m

package test

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Constantes e configuração compartilhada
// ─────────────────────────────────────────────────────────────────────────────

const (
	// Porta dedicada aos testes de performance/antifragilidade.
	entropyTestPort = 9422

	// Payload padrão de 256 bytes (solicitado para comparação).
	defaultEntropyPayloadSize = 256

	// Duração padrão do Load Test (segundos).
	loadTestDurationSec = 10

	// Duração do Stress Test (segundos).
	stressTestDurationSec = 15

	// Número de conexões simultâneas no Stress Test.
	stressConcurrency = 100

	// Duração do Antifragile Test (segundos).
	antifragileTestDurationSec = 20

	// Intervalo de injeção de caos (segundos).
	chaosIntervalSec = 3

	// Taxa máxima de erro aceitável no teste antifragile (10 %).
	maxErrorRatioAntifragile = 0.10
)

// entropyPayload gera um payload de tamanho fixo para os testes.
func entropyPayload(size int) string {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte('a' + (i % 26))
	}
	return string(b)
}

// userRegistrationPayload gera um JSON realista de cadastro de usuário.
func userRegistrationPayload() string {
	return `{
  "name": "Jean Carlo Suissa",
  "email": "suissa@allascode.com",
  "username": "suissa",
  "password": "$2b$12$KIXp.vU9mO2.9K6P1/N.O.X7F6yv2U6yv2U6yv2U6yv2U6yv2U6y",
  "birthDate": "1984-06-15",
  "address": {
    "street": "Rua da Antifragilidade, 42",
    "city": "Ribeirão Preto",
    "state": "SP",
    "country": "Brasil",
    "zip": "14000-000"
  },
  "phone": "+5516999999999",
  "metadata": {
    "role": "architect",
    "plan": "premium",
    "entropy_level": 0.85
  }
}`
}

// runEntropyServer inicia o servidor UbiQ embutido para os testes de entropia.
func runEntropyServer(t testing.TB, port int) *interface{ Shutdown() } {
	t.Helper()
	opts := DefaultTestOptions
	opts.Port = port
	opts.DisableShortFirstPing = true
	s := RunServer(&opts)
	// Retorna como interface anônima para não vazar o tipo server.Server.
	type shutdowner interface{ Shutdown() }
	return (*interface{ Shutdown() })(&[]interface{ Shutdown() }{s}[0])
}

// natsConnect realiza o handshake NATS mínimo e retorna a conexão pronta.
func natsConnect(t testing.TB, port int) net.Conn {
	t.Helper()
	c := createClientConn(t, "127.0.0.1", port)
	doDefaultConnect(t, c)
	return c
}

// natsPub envia uma mensagem PUB no protocolo NATS texto.
func natsPub(bw *bufio.Writer, subject string, payload string) {
	fmt.Fprintf(bw, "PUB %s %d\r\n%s\r\n", subject, len(payload), payload)
}

// natsSub envia um SUB no protocolo NATS texto.
func natsSub(c net.Conn, subject, sid string) {
	fmt.Fprintf(c, "SUB %s %s\r\n", subject, sid)
}

// drainEntropyConn conta mensagens recebidas até receber sinal de parada.
func drainEntropyConn(c net.Conn, counter *int64, stop <-chan struct{}) {
	buf := make([]byte, 32768)
	for {
		select {
		case <-stop:
			return
		default:
		}
		c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := c.Read(buf)
		c.SetReadDeadline(time.Time{})
		if err != nil || n == 0 {
			continue
		}
		// Conta ocorrências de "MSG " no bloco lido.
		data := buf[:n]
		for i := 0; i < len(data)-4; i++ {
			if data[i] == 'M' && data[i+1] == 'S' && data[i+2] == 'G' && data[i+3] == ' ' {
				atomic.AddInt64(counter, 1)
				i += 3
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Teste de Carga — Load Test
//    Mede a vazão máxima (MPS e MiB/s) com 1 Publisher e 1 Subscriber.
// ─────────────────────────────────────────────────────────────────────────────

func TestUbiQ_LoadTest(t *testing.T) {
	// ── Setup ─────────────────────────────────────────────────────────────
	opts := DefaultTestOptions
	opts.Port = entropyTestPort
	opts.DisableShortFirstPing = true
	s := RunServer(&opts)
	defer s.Shutdown()

	payload := userRegistrationPayload()
	payloadSize := len(payload)
	pubOp := []byte(fmt.Sprintf("PUB bench.load %d\r\n%s\r\n", payloadSize, payload))

	t.Logf("[CARGA] Usando Payload Realista (Cadastro de Usuário): %d bytes", payloadSize)

	sub := natsConnect(t, entropyTestPort)
	defer sub.Close()
	pub := natsConnect(t, entropyTestPort)
	defer pub.Close()

	// ── Subscriber ────────────────────────────────────────────────────────
	natsSub(sub, "bench.load", "1")

	var received int64
	stop := make(chan struct{})
	go drainEntropyConn(sub, &received, stop)

	time.Sleep(200 * time.Millisecond) // aguarda o SUB propagar

	// ── Publisher ─────────────────────────────────────────────────────────
	bw := bufio.NewWriterSize(pub, 65536)
	var published int64

	start := time.Now()
	deadline := start.Add(loadTestDurationSec * time.Second)

	for time.Now().Before(deadline) {
		bw.Write(pubOp)
		published++
		if published%1000 == 0 {
			bw.Flush()
		}
	}
	bw.Flush()

	// Aguarda drenagem
	time.Sleep(500 * time.Millisecond)
	close(stop)

	elapsed := time.Since(start).Seconds()
	rec := atomic.LoadInt64(&received)

	// ── Métricas ──────────────────────────────────────────────────────────
	mpsPub := float64(published) / elapsed
	mpsRec := float64(rec) / elapsed
	mbps := float64(published) * float64(payloadSize) / (1024 * 1024) / elapsed

	t.Logf("[CARGA] Duração: %.2fs", elapsed)
	t.Logf("[CARGA] Publicadas:  %d mensagens  (%.0f msg/s)", published, mpsPub)
	t.Logf("[CARGA] Recebidas:   %d mensagens  (%.0f msg/s)", rec, mpsRec)
	t.Logf("[CARGA] Bandwidth:   %.2f MiB/s", mbps)

	// ── Asserções ─────────────────────────────────────────────────────────
	if published == 0 {
		t.Fatal("Nenhuma mensagem foi publicada — servidor pode estar inacessível")
	}
	if mpsPub < 100 {
		t.Errorf("Vazão de publicação muito baixa: %.0f msg/s (mínimo esperado: 100)", mpsPub)
	}
	if rec == 0 {
		t.Error("Nenhuma mensagem foi recebida pelo Subscriber")
	}
	// A vazão de recepção deve estar dentro de 5× da publicação
	// (latência de bufferização pode causar defasagem no curto prazo).
	if mpsRec > mpsPub*5 {
		t.Errorf("Vazão de recepção anômala: %.0f msg/s > 5× publicado (%.0f)", mpsRec, mpsPub)
	}

	t.Logf("✅ Load Test PASSOU: servidor respondeu com %.0f msg/s publicados", mpsPub)
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Teste de Estresse — Stress Test
//    Valida a estabilidade com 100 conexões simultâneas e jitter aleatório.
// ─────────────────────────────────────────────────────────────────────────────

func TestUbiQ_StressTest(t *testing.T) {
	// ── Setup ─────────────────────────────────────────────────────────────
	opts := DefaultTestOptions
	opts.Port = entropyTestPort + 1 // porta distinta para não conflitar com Load
	opts.DisableShortFirstPing = true
	s := RunServer(&opts)
	defer s.Shutdown()

	port := entropyTestPort + 1
	payload := entropyPayload(defaultEntropyPayloadSize)

	var (
		totalPublished int64
		totalReceived  int64
		connErrors     int64
	)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// ── Criação das conexões concorrentes ─────────────────────────────────
	for i := 0; i < stressConcurrency; i++ {
		i := i // captura para goroutine

		subConn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
		if err != nil {
			atomic.AddInt64(&connErrors, 1)
			continue
		}
		doDefaultConnect(t, subConn)
		natsSub(subConn, fmt.Sprintf("bench.stress.%d", i), fmt.Sprintf("%d", i+100))

		go drainEntropyConn(subConn, &totalReceived, stop)

		pubConn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
		if err != nil {
			atomic.AddInt64(&connErrors, 1)
			subConn.Close()
			continue
		}
		doDefaultConnect(t, pubConn)

		wg.Add(1)
		go func(conn net.Conn, idx int) {
			defer wg.Done()
			defer conn.Close()

			bw := bufio.NewWriterSize(conn, 32768)
			subject := fmt.Sprintf("bench.stress.%d", idx)
			pubOp := fmt.Sprintf("PUB %s %d\r\n%s\r\n", subject, len(payload), payload)

			for {
				select {
				case <-stop:
					bw.Flush()
					return
				default:
				}

				if _, err := bw.WriteString(pubOp); err != nil {
					atomic.AddInt64(&connErrors, 1)
					return
				}
				atomic.AddInt64(&totalPublished, 1)

				// Flush periódico para não saturar o buffer
				if atomic.LoadInt64(&totalPublished)%500 == 0 {
					bw.Flush()
				}

				// Simula jitter aleatório (0–10 ms)
				jitter := time.Duration(rand.Intn(10)) * time.Millisecond
				time.Sleep(jitter)
			}
		}(pubConn, i)
	}

	// ── Aguarda duração do teste ───────────────────────────────────────────
	start := time.Now()
	time.Sleep(stressTestDurationSec * time.Second)
	close(stop)
	wg.Wait()

	elapsed := time.Since(start).Seconds()
	pub := atomic.LoadInt64(&totalPublished)
	rec := atomic.LoadInt64(&totalReceived)
	errs := atomic.LoadInt64(&connErrors)

	// ── Métricas ──────────────────────────────────────────────────────────
	t.Logf("[ESTRESSE] Duração:     %.2fs", elapsed)
	t.Logf("[ESTRESSE] Conexões c/erro: %d", errs)
	t.Logf("[ESTRESSE] Publicadas:  %d  (%.0f msg/s)", pub, float64(pub)/elapsed)
	t.Logf("[ESTRESSE] Recebidas:   %d  (%.0f msg/s)", rec, float64(rec)/elapsed)

	// ── Asserções ─────────────────────────────────────────────────────────
	maxAllowedErrors := int64(stressConcurrency / 5) // até 20% de falha de conexão
	if errs > maxAllowedErrors {
		t.Errorf("Muitos erros de conexão: %d (máx permitido: %d)", errs, maxAllowedErrors)
	}
	if pub == 0 {
		t.Fatal("Nenhuma mensagem publicada — possível colapso do servidor sob estresse")
	}

	t.Logf("✅ Stress Test PASSOU: %d conexões, %d erros, %.0f msg/s pub", stressConcurrency, errs, float64(pub)/elapsed)
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Teste de Resiliência Antifrágil — Antifragile Entropy
//    Publica constantemente enquanto injeta caos (fechamento de sockets).
//    Valida que o servidor sobrevive e que a taxa de erros fica abaixo de 10 %.
// ─────────────────────────────────────────────────────────────────────────────

func TestUbiQ_AntifragileEntropyTest(t *testing.T) {
	// ── Setup ─────────────────────────────────────────────────────────────
	opts := DefaultTestOptions
	opts.Port = entropyTestPort + 2 // porta distinta
	opts.DisableShortFirstPing = true
	s := RunServer(&opts)
	defer s.Shutdown()

	port := entropyTestPort + 2
	payload := entropyPayload(defaultEntropyPayloadSize)

	var (
		published  int64
		pubErrors  int64
		healCycles int64 // reconexões bem-sucedidas após falha
	)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// connPool mantém as conexões ativas para o injetor de caos destruir.
	var poolMu sync.Mutex
	connPool := make([]net.Conn, 0, 10)

	// addConn adiciona nova conexão ao pool de publicadores.
	addConn := func() net.Conn {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 2*time.Second)
		if err != nil {
			return nil
		}
		doDefaultConnect(t, c)
		poolMu.Lock()
		connPool = append(connPool, c)
		poolMu.Unlock()
		return c
	}

	// ── Goroutines de publicação (10 concorrentes com re-conexão) ─────────
	const antifragileConcurrency = 10
	for i := 0; i < antifragileConcurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			subject := fmt.Sprintf("bench.antifragile.%d", id)
			pubOp := fmt.Sprintf("PUB %s %d\r\n%s\r\n", subject, len(payload), payload)

			var conn net.Conn
			reconnect := func() bool {
				if conn != nil {
					conn.Close()
				}
				conn = addConn()
				if conn == nil {
					return false
				}
				atomic.AddInt64(&healCycles, 1)
				return true
			}

			if !reconnect() {
				return
			}

			bw := bufio.NewWriterSize(conn, 32768)
			for {
				select {
				case <-stop:
					bw.Flush()
					return
				default:
				}

				if _, err := bw.WriteString(pubOp); err != nil {
					atomic.AddInt64(&pubErrors, 1)
					// Auto-cura: tenta reconectar
					if !reconnect() {
						return
					}
					bw = bufio.NewWriterSize(conn, 32768)
					continue
				}

				atomic.AddInt64(&published, 1)

				if atomic.LoadInt64(&published)%200 == 0 {
					if err := bw.Flush(); err != nil {
						atomic.AddInt64(&pubErrors, 1)
						if !reconnect() {
							return
						}
						bw = bufio.NewWriterSize(conn, 32768)
					}
				}

				// Pequeno jitter
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
		}(i)
	}

	// ── Injetor de Caos ───────────────────────────────────────────────────
	// A cada chaosIntervalSec segundos, fecha metade aleatória das conexões
	// do pool para forçar o ciclo de auto-cura (Healer / HealingToolkit).
	go func() {
		ticker := time.NewTicker(chaosIntervalSec * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				poolMu.Lock()
				if len(connPool) > 0 {
					// Fecha até metade das conexões no pool
					killCount := len(connPool) / 2
					if killCount == 0 {
						killCount = 1
					}
					t.Logf("🔥 [CHAOS] Encerrando %d conexões para injetar entropia...", killCount)
					for k := 0; k < killCount && k < len(connPool); k++ {
						connPool[k].Close()
					}
					// Remove as destruídas do pool
					connPool = connPool[killCount:]
				}
				poolMu.Unlock()
			}
		}
	}()

	// ── Execução ──────────────────────────────────────────────────────────
	start := time.Now()
	time.Sleep(antifragileTestDurationSec * time.Second)
	close(stop)
	wg.Wait()

	elapsed := time.Since(start).Seconds()
	pub := atomic.LoadInt64(&published)
	errs := atomic.LoadInt64(&pubErrors)
	healed := atomic.LoadInt64(&healCycles)

	// ── Métricas ──────────────────────────────────────────────────────────
	t.Logf("[ANTIFRAGILE] Duração:           %.2fs", elapsed)
	t.Logf("[ANTIFRAGILE] Mensagens Publicadas: %d  (%.0f msg/s)", pub, float64(pub)/elapsed)
	t.Logf("[ANTIFRAGILE] Erros Encontrados:    %d", errs)
	t.Logf("[ANTIFRAGILE] Ciclos de Auto-Cura:  %d", healed)

	errorRatio := 0.0
	if pub+errs > 0 {
		errorRatio = float64(errs) / float64(pub+errs)
	}
	t.Logf("[ANTIFRAGILE] Taxa de Erro:         %.2f%%", errorRatio*100)

	// ── Asserções ─────────────────────────────────────────────────────────
	if pub == 0 {
		t.Fatal("❌ Nenhuma mensagem publicada — sistema colapsou completamente sob caos")
	}
	if errorRatio > maxErrorRatioAntifragile {
		t.Errorf(
			"❌ Taxa de erro %.2f%% excede o máximo aceitável de %.0f%% — sistema não demonstrou antifragilidade",
			errorRatio*100, maxErrorRatioAntifragile*100,
		)
	} else {
		t.Logf(
			"✅ Antifragile Test PASSOU: %.2f%% de erro (máx: %.0f%%), %d ciclos de auto-cura",
			errorRatio*100, maxErrorRatioAntifragile*100, healed,
		)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks correspondentes
// ─────────────────────────────────────────────────────────────────────────────

// Benchmark_UbiQ_LoadThroughput mede o throughput bruto em condições ideais
// (equivalente ao `npm run test:load` do hyper-queues).
func Benchmark_UbiQ_LoadThroughput(b *testing.B) {
	opts := DefaultTestOptions
	opts.Port = entropyTestPort + 10
	opts.DisableShortFirstPing = true
	s := RunServer(&opts)
	defer s.Shutdown()

	payload := entropyPayload(defaultEntropyPayloadSize)
	pubOp := []byte(fmt.Sprintf("PUB bench.load %d\r\n%s\r\n", len(payload), payload))

	c := createClientConn(b, "127.0.0.1", entropyTestPort+10)
	doDefaultConnect(b, c)
	bw := bufio.NewWriterSize(c, 65536)

	b.SetBytes(int64(len(pubOp)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bw.Write(pubOp)
	}
	bw.Flush()
	flushConnection(b, c)
	b.StopTimer()

	c.Close()
}

// Benchmark_UbiQ_StressConcurrent mede a vazão sob 10 publicadores simultâneos
// (versão reduzida do `npm run test:stress` adaptada para o framework Go bench).
func Benchmark_UbiQ_StressConcurrent(b *testing.B) {
	opts := DefaultTestOptions
	opts.Port = entropyTestPort + 11
	opts.DisableShortFirstPing = true
	s := RunServer(&opts)
	defer s.Shutdown()

	const numPublishers = 10
	payload := entropyPayload(defaultEntropyPayloadSize)

	conns := make([]net.Conn, numPublishers)
	writers := make([]*bufio.Writer, numPublishers)
	for i := 0; i < numPublishers; i++ {
		conns[i] = createClientConn(b, "127.0.0.1", entropyTestPort+11)
		doDefaultConnect(b, conns[i])
		writers[i] = bufio.NewWriterSize(conns[i], 65536)
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	pubOp := []byte(fmt.Sprintf("PUB bench.stress %d\r\n%s\r\n", len(payload), payload))

	b.SetBytes(int64(len(pubOp)) * numPublishers)
	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bw := writers[idx]
			for j := 0; j < b.N; j++ {
				bw.Write(pubOp)
			}
			bw.Flush()
		}(i)
	}
	wg.Wait()

	b.StopTimer()
}
