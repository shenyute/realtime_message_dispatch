package utils

import (
	"encoding/base64"
	"errors"
	"github.com/shenyute/realtime_message_dispatch/base"
	"io"
	"os"
)

func fileContents(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	result := make([]byte, 0)
	inbuf := make([]byte, 4096)
	for {
		i, err := f.Read(inbuf)
		if i == 0 && err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, inbuf[:i]...)
	}
	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(result)))
	decodedlen, err := base64.StdEncoding.Decode(decoded, result)
	if err != nil {
		return nil, err
	}
	return decoded[:decodedlen], nil
}

func ReadKeySet(secFile, pubFile, hmacFile string) (*base.KeySet, error) {
	secret, err := fileContents(secFile)
	if err != nil {
		return nil, err
	}
	public, err := fileContents(pubFile)
	if err != nil {
		return nil, err
	}
	hmac, err := fileContents(hmacFile)
	if err != nil {
		return nil, err
	}
	if len(secret) != 32 {
		return nil, errors.New("Secret key is not 32 bytes long")
	}
	if len(public) != 32 {
		return nil, errors.New("Public key is not 32 bytes long")
	}
	if len(hmac) != 32 {
		return nil, errors.New("HMAC secret is not 32 bytes long")
	}
	result := new(base.KeySet)
	copy(result.PrivateKey[:], secret)
	copy(result.PublicKey[:], public)
	copy(result.HMACSecret[:], hmac)
	if !result.Ok() {
		return nil, errors.New("Public and secret keys do not match")
	}
	return result, nil
}
