package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
)

var (
	ErrInvalidKeySize     = errors.New("invalid encryption key size: must be 32 bytes for AES-256")
	ErrCiphertextTooShort = errors.New("ciphertext too short")
)

// Encrypt encrypts plain text string into base64 encoded ciphertext
func Encrypt(plaintext string, keyString string) (string, error) {
	key := []byte(keyString)
	if len(key) != 32 && len(key) != 16 && len(key) != 24 {
		return "", ErrInvalidKeySize
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt decrypts base64 encoded ciphertext into plain text string
func Decrypt(cryptoText string, keyString string) (string, error) {
	// If the text is empty, return empty
	if cryptoText == "" {
		return "", nil
	}
	
	key := []byte(keyString)
	if len(key) != 32 && len(key) != 16 && len(key) != 24 {
		return "", ErrInvalidKeySize
	}

	ciphertext, err := base64.StdEncoding.DecodeString(cryptoText)
	if err != nil {
		// Fallback: If it's not valid base64, it might be an unencrypted token
		// Return it as is to avoid breaking existing unencrypted tokens
		return cryptoText, nil
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	if len(ciphertext) < gcm.NonceSize() {
		// Fallback for unencrypted tokens
		return cryptoText, nil
	}

	nonce, ciphertextBytes := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertextBytes, nil)
	if err != nil {
		// Fallback for unencrypted tokens failing to decrypt (e.g. invalid MAC)
		return cryptoText, nil 
	}

	return string(plaintext), nil
}
