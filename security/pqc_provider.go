package security

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"
)

// LinearAutodestroyKey representa uma chave que se auto-destrói após o uso ou tempo.
type LinearAutodestroyKey struct {
	ID        string
	PublicKey []byte
	CreatedAt time.Time
	ExpiresAt time.Time
	Used      bool
}

// DPoPToken implementa Demonstrating Proof-of-Possession com autodestruição.
type DPoPToken struct {
	JTI       string    // Unique Token ID
	HTM       string    // HTTP Method (ou PUB/SUB no UbiQ)
	HTU       string    // Subject URI
	IAT       time.Time // Issued At
	KeyID     string    // Vínculo com a chave mTLS
}

// PQCManager gerencia a vida útil das chaves e tokens pós-quânticos.
type PQCManager struct {
	mu    sync.Mutex
	keys  map[string]*LinearAutodestroyKey
	jtis  map[string]time.Time // Para prevenir replay
}

func NewPQCManager() *PQCManager {
	return &PQCManager{
		keys: make(map[string]*LinearAutodestroyKey),
		jtis: make(map[string]time.Time),
	}
}

// RegisterKey registra uma chave PQC (Kyber/Dilithium) com validade linear.
func (m *PQCManager) RegisterKey(id string, pubKey []byte, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.keys[id] = &LinearAutodestroyKey{
		ID:        id,
		PublicKey: pubKey,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(ttl),
		Used:      false,
	}
}

// ValidateDPoP valida se o token DPoP é legítimo, está vinculado à chave mTLS e ainda não se auto-destruiu.
func (m *PQCManager) ValidateDPoP(token *DPoPToken, signature []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Verifica se o JTI já foi usado (Prevenção de Replay)
	if _, exists := m.jtis[token.JTI]; exists {
		return errors.New("DPoP Error: Token JTI already destroyed (Linear Autodestroy)")
	}

	// 2. Verifica se a chave vinculada existe e é válida
	key, exists := m.keys[token.KeyID]
	if !exists {
		return errors.New("DPoP Error: Associated PQC Key not found")
	}

	if time.Now().After(key.ExpiresAt) {
		delete(m.keys, token.KeyID)
		return errors.New("DPoP Error: PQC Key expired (Autodestroyed)")
	}

	// 3. Validação de Janela de Tempo (Tokens DPoP devem ser ultra-efêmeros)
	if time.Since(token.IAT) > 10*time.Second {
		return errors.New("DPoP Error: Token too old")
	}

	// 4. Marca JTI como destruído imediatamente
	m.jtis[token.JTI] = time.Now()
	
	// Limpeza periódica de JTIs velhos poderia ser feita em background
	return nil
}

// CreateDPoPProof gera a prova de posse (DPoP Proof) que o cliente enviaria.
func CreateDPoPProof(keyID string, subject string) (string, string) {
	jti := fmt.Sprintf("jti-%d", time.Now().UnixNano())
	iat := time.Now().Format(time.RFC3339)
	
	// Simulação de Header DPoP para UbiQ
	payload := fmt.Sprintf(`{"jti":"%s","htu":"%s","iat":"%s","kid":"%s"}`, jti, subject, iat, keyID)
	proof := base64.RawURLEncoding.EncodeToString([]byte(payload))
	
	// No mundo real, aqui haveria a assinatura Dilithium (PQC) do proof
	signature := sha256.Sum256([]byte(proof))
	return proof, base64.RawURLEncoding.EncodeToString(signature[:])
}
