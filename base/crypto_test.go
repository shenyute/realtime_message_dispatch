package base

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"testing"
)

type mockUser struct {
	packetId  uint32
	rcvNonce  [16]byte
	sendNonce [16]byte
	userId    [3]byte
	cryptor   Encryptor
}

func interpretTestLoginPacket(payload []byte, serverKeys, clientKeys *KeySet) (*mockUser, error) {
	if len(payload) < 107 {
		return nil, fmt.Errorf("Too short to be a login-response packet: %v", payload)
	}
	sharedSecretFull := clientKeys.SharedSecret(&serverKeys.PublicKey)
	cryptor, err := NewEncryptor(sharedSecretFull[16:])
	if err != nil {
		return nil, err
	}
	user := new(mockUser)
	user.packetId = 0
	user.cryptor = cryptor
	copy(user.rcvNonce[:], payload[4:20])
	user.cryptor.EncryptPacket(user.rcvNonce[:], payload[52:], payload[52:])
	msgCode := binary.LittleEndian.Uint32(payload[52:56])
	if msgCode != 0 {
		return nil, fmt.Errorf("Not a login-reponse packet after decoding")
	}
	copy(user.sendNonce[:], payload[56:72])
	/* TODO: payload[72:88] is the base nonce used to derive the nonces used when messages in packet format 3 are sent or received.*/
	/* TODO: payload[88:104] is the key used when messages in packet format 3 are sent or received. */
	copy(user.userId[:], payload[104:107])
	return user, nil
}

func (user *mockUser) createDirectedPacket(payload []byte) []byte {
	result := make([]byte, 8)
	result[0] = 1
	copy(result[1:4], user.userId[:])
	binary.LittleEndian.PutUint32(result[4:8], user.packetId)
	result = append(result, payload...)

	carry := uint32(0)
	nonce := user.sendNonce // Copied, because this is an array, not a slice!
	for i := 0; i < 4; i++ {
		// Sign of i is different here because nonce is big-endian and packet values are little-endian
		newval := uint32(nonce[14-i]) + uint32(result[4+i]) + carry
		carry = newval >> 8
		nonce[14-i] = byte(newval & 0xff)
	}
	// Propagate carry the rest of the way through
	for i := 10; i >= 0; i-- {
		newval := uint32(nonce[i]) + carry
		carry = newval >> 8
		nonce[i] = byte(newval & 0xff)
		if carry == 0 {
			break
		}
	}
	user.cryptor.EncryptPacket(nonce[:], result[8:], result[8:])
	user.packetId++
	return result
}

func TestJWT(t *testing.T) {
	ks, err := NewKeySet()
	var token string
	if err != nil {
		t.Fatalf("Error creating keyset: %v", err)
	}
	// Good token, to be modified in various ways that make it illegal later
	claims := jwt.MapClaims{
		"sub":     "Test Account",
		"feed":    "Test Feed",
		"version": "1.0",
		"pubkey":  base64.StdEncoding.EncodeToString(ks.PublicKey[:])}
	token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(ks.HMACSecret[:])
	var result *JWTCreds

	if err == nil {
		result, err = ParseToken(token, ks)
	}
	if err != nil {
		t.Fatalf("Could not create baseline correct token: %v", err)
	} else if result.version != "1.0" {
		t.Fatalf("Could not get version claim")
	}
}

func TestBadJWTs(t *testing.T) {
	ks, err := NewKeySet()
	if err != nil {
		t.Fatalf("Error creating keyset: %v", err)
	}

	// Bad token 1: Missing claims
	token, err := jwt.New(jwt.SigningMethodHS256).SignedString(ks.HMACSecret[:])
	if err != nil {
		t.Fatalf("Error creating blank token: %v", err)
	}
	_, err = ParseToken(token, ks)
	if err == nil {
		t.Errorf("Expected an error when parsing a token with no claims")
	} else {
		t.Logf("Received expected error with no claims: %v", err)
	}

	// Good token, to be modified in various ways that make it illegal later
	claims := jwt.MapClaims{
		"sub":    "Test Account",
		"feed":   "Test Feed",
		"pubkey": base64.StdEncoding.EncodeToString(ks.PublicKey[:])}
	token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(ks.HMACSecret[:])
	if err == nil {
		_, err = ParseToken(token, ks)
	}
	if err != nil {
		t.Fatalf("Could not create baseline correct token: %v", err)
	}

	// Bad token 2: Signed with the wrong HMAC key (recycles from Good Token)
	ks2, err := NewKeySet()
	if err != nil {
		t.Fatalf("Error creating secondary keyset: %v", err)
	}
	_, err = ParseToken(token, ks2)
	if err == nil {
		t.Errorf("Expected an error when a token was mis-signed")
	} else {
		t.Logf("Received expected error with mis-signed token: %v", err)
	}

	// TODO: Bad token 3: Signed with ECDSA instead of HMAC (uses crypto/elliptic)

	// Good token 2: quotes around the public key
	claims["pubkey"] = "\"" + claims["pubkey"].(string) + "\""
	token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(ks.HMACSecret[:])
	if err == nil {
		_, err = ParseToken(token, ks)
	}
	if err != nil {
		t.Errorf("Rejected token with valid but quoted pubkey: %v", err)
	}

	// Bad token 4: Pubkey is wrong length
	fakeKey := append(ks.PublicKey[:], 1, 2, 3, 4)
	claims["pubkey"] = base64.StdEncoding.EncodeToString(fakeKey)
	token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(ks.HMACSecret[:])
	if err != nil {
		t.Fatalf("Could not make a token? %v", err)
	}
	_, err = ParseToken(token, ks)
	if err != nil {
		t.Logf("Received expected error when pubkey is wrong length: %v", err)
	} else {
		t.Errorf("Accepted a token with an invalid public key")
	}

	// Bad token 5: Pubkey is invalid base64
	claims["pubkey"] = "]" + base64.StdEncoding.EncodeToString(ks.PublicKey[:])[1:]
	token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(ks.HMACSecret[:])
	if err != nil {
		t.Fatalf("Could not make a token? %v", err)
	}
	_, err = ParseToken(token, ks)
	if err != nil {
		t.Logf("Received expected error when pubkey is bad base64: %v", err)
	} else {
		t.Errorf("Accepted a token with an invalid public key")
	}
}

func TestKeysets(t *testing.T) {
	ks, err := NewKeySet()
	if err != nil {
		t.Fatalf("Error creating keyset: %v", err)
	}
	if !ks.Ok() {
		t.Fatalf("Generated Keyset is not OK")
	}
	ks.PublicKey[0] = byte((ks.PublicKey[0] + 1) & 0xff)
	if ks.Ok() {
		t.Fatalf("Corrupted Keyset is incorrectly OK")
	}
}
