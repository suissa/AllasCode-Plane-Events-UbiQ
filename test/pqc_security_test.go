package test

import (
	"testing"
	"time"
	"github.com/nats-io/nats-server/v2/security"
)

func TestUbiQ_Security_PQC_LinearAutodestroy(t *testing.T) {
	manager := security.NewPQCManager()
	
	keyID := "pqc-key-001"
	pubKey := []byte("fake-dilithium-public-key")
	
	// 1. Registra chave com TTL de 2 segundos (Ultra Linear Autodestroy)
	manager.RegisterKey(keyID, pubKey, 2*time.Second)
	t.Log("✅ Chave PQC registrada com validade de 2s.")

	// 2. Gera um DPoP Proof para o assunto "orders.create"
	_, sig := security.CreateDPoPProof(keyID, "orders.create")
	
	// Mock: Decodifica o proof para o manager validar
	// Em um sistema real, o UbiQ faria isso no middleware de Auth
	token := &security.DPoPToken{
		JTI:   "jti-unique-123", // Simulado
		KeyID: keyID,
		IAT:   time.Now(),
	}

	// 3. Primeira validação: Deve PASSAR
	err := manager.ValidateDPoP(token, []byte(sig))
	if err != nil {
		t.Fatalf("❌ Erro inesperado na primeira validação: %v", err)
	}
	t.Log("✅ Primeiro uso do token DPoP: Sucesso (Vínculo PQC OK).")

	// 4. Tentativa de Reuso (REPLAY ATTACK): Deve FALHAR (Linear Autodestroy)
	err = manager.ValidateDPoP(token, []byte(sig))
	if err == nil {
		t.Fatal("❌ Falha de Segurança: O token JTI deveria ter sido autodestruído após o primeiro uso!")
	}
	t.Logf("✅ Bloqueio de Replay: %v (Esperado: Token JTI já destruído)", err)

	// 5. Aguarda a chave se auto-destruir
	t.Log("... Aguardando expiração da chave PQC (Autodestroy) ...")
	time.Sleep(3 * time.Second)

	// 6. Novo token com chave expirada: Deve FALHAR
	newToken := &security.DPoPToken{
		JTI:   "jti-unique-456",
		KeyID: keyID,
		IAT:   time.Now(),
	}
	err = manager.ValidateDPoP(newToken, []byte("signature"))
	if err == nil {
		t.Fatal("❌ Falha de Segurança: A chave PQC deveria ter sido autodestruída por tempo!")
	}
	t.Logf("✅ Autodestruição de Chave: %v (Esperado: Key expired/deleted)", err)
}
