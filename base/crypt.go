package base

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"golang.org/x/crypto/curve25519"
)

type KeySet struct {
	PrivateKey [32]byte
	PublicKey  [32]byte
	HMACSecret [32]byte
}

type JWTCreds struct {
	account     string
	feed        string
	key         []byte
	version     string
	isModerator bool
}

type Encryptor struct {
	ourCipher cipher.Block
}

func NewKeySet() (*KeySet, error) {
	buf := make([]byte, 256)
	_, err := rand.Reader.Read(buf)
	if err != nil {
		return nil, err
	}
	result := new(KeySet)
	result.PrivateKey = sha256.Sum256(buf)
	result.PrivateKey[0] &= 248
	result.PrivateKey[31] &= 127
	result.PrivateKey[31] |= 64
	curve25519.ScalarBaseMult(&result.PublicKey, &result.PrivateKey)
	_, err = rand.Reader.Read(buf)
	if err != nil {
		return nil, err
	}
	result.HMACSecret = sha256.Sum256(buf)
	return result, nil
}

func NewEncryptor(key []byte) (Encryptor, error) {
	ourCipher, e := aes.NewCipher(key)
	return Encryptor{ourCipher}, e
}

func ParseToken(receivedToken string, keySet *KeySet) (*JWTCreds, error) {
	token, err := jwt.Parse(receivedToken, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}

		return keySet.HMACSecret[:], nil
	})
	if token != nil {
		if claims, ok := token.Claims.(jwt.MapClaims); ok {
			subRaw, sok := claims["sub"]
			feedRaw, feedok := claims["feed"]
			pubkeyRaw, keyok := claims["pubkey"]
			_, modok := claims["mod"]
			version, vok := claims["version"]
			var sub, feed, pubkeyStr, verStr string
			if sok {
				sub, sok = subRaw.(string)
			}
			if feedok {
				feed, feedok = feedRaw.(string)
			}
			if keyok {
				pubkeyStr, keyok = pubkeyRaw.(string)
			}
			if vok {
				verStr, vok = version.(string)
			}
			if sok && feedok && keyok {
				result := new(JWTCreds)
				result.account = sub
				result.feed = feed
				if vok {
					result.version = verStr
				}
				pubkey := []byte(pubkeyStr)
				if (pubkey[0] == '"') && (pubkey[len(pubkey)-1] == '"') {
					fmt.Printf("Stripping spurious quotation marks from the public key\n")
					pubkey = pubkey[1 : len(pubkey)-1]
				}
				decoded := make([]byte, base64.StdEncoding.DecodedLen(len(pubkey)))
				decodedlen, decodeerr := base64.StdEncoding.Decode(decoded, pubkey)
				if decodeerr != nil && err == nil {
					err = fmt.Errorf("Error decoding public key claim: %v\n", decodeerr)
				}
				result.key = decoded[:decodedlen]
				if len(result.key) != 32 && err == nil {
					err = fmt.Errorf("Public key is the wrong size\n")
				}
				result.isModerator = modok
				// This is the only possibly-successful case, and if the ticket's expired or mis-signed,
				// it's still not successful and error is non-nil
				return result, err
			} else {
				missing := ""
				if !sok {
					missing += " sub"
				}
				if !feedok {
					missing += " feed"
				}
				if !keyok {
					missing += " pubkey"
				}
				err = fmt.Errorf("Missing claims:%s", missing)
			}
		} else {
			if err == nil {
				err = fmt.Errorf("Could not find map claims")
			}
		}
	}
	return nil, err
}

func (ks *KeySet) SharedSecret(theirPublic *[32]byte) (result [32]byte) {
	curve25519.ScalarMult(&result, &ks.PrivateKey, theirPublic)
	return
}

func (ks *KeySet) Ok() bool {
	var tmp [32]byte
	curve25519.ScalarBaseMult(&tmp, &ks.PrivateKey)
	return ks.PublicKey == tmp
}

func (e Encryptor) EncryptPacket(nonce, dest, src []byte) {
	streamCipher := cipher.NewCTR(e.ourCipher, nonce)
	streamCipher.XORKeyStream(dest, src)
}
