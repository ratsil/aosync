package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/x509"
	"encoding/pem"
)

var (
	_pPrivateKey *rsa.PrivateKey
	_pPublicKey *rsa.PublicKey
)

//PrivateKey .
func PrivateKey(priv []byte) (err error) {
	block, _ := pem.Decode(priv)
	enc := x509.IsEncryptedPEMBlock(block)
	b := block.Bytes
	if enc {
		b, err = x509.DecryptPEMBlock(block, nil)
		if err != nil {
			return
		}
	}
	key, err := x509.ParsePKCS1PrivateKey(b)
	if err == nil {
		_pPrivateKey = key
	}
	return
}

//PublicKey .
func PublicKey(pub []byte) (err error) {
	block, _ := pem.Decode(pub)
	enc := x509.IsEncryptedPEMBlock(block)
	b := block.Bytes
	if enc {
		b, err = x509.DecryptPEMBlock(block, nil)
		if err != nil {
			return
		}
	}
	ifc, err := x509.ParsePKIXPublicKey(b)
	if err != nil {
		return
	}
	key, ok := ifc.(*rsa.PublicKey)
	if ok {
		_pPublicKey = key
	}
	return
}

//Encrypt .
func Encrypt(a []byte) ([]byte, error) {
	return rsa.EncryptOAEP(sha512.New(), rand.Reader, _pPublicKey, a, nil)
}

//Decrypt .
func Decrypt(a []byte) ([]byte, error) {
	return rsa.DecryptOAEP(sha512.New(), rand.Reader, _pPrivateKey, a, nil)
} 
