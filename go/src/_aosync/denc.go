package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
)

var (
	_pPrivateKey *rsa.PrivateKey
	_pPublicKey  *rsa.PublicKey
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

//EncryptByKey .
func EncryptByKey(a []byte) ([]byte, error) {
	return rsa.EncryptOAEP(sha256.New(), rand.Reader, _pPublicKey, a, nil)
}

//DecryptByKey .
func DecryptByKey(a []byte) ([]byte, error) {
	return rsa.DecryptOAEP(sha256.New(), rand.Reader, _pPrivateKey, a, nil)
}

//Cipher .
type Cipher struct {
	Buffer  []byte
	iBlock  cipher.Block
	IV      []byte
	iStream cipher.Stream
}

//NewCipher .
func NewCipher(aKey, aIV []byte, nBufferSize int) (pRetVal *Cipher, err error) {
	pRetVal = new(Cipher)
	if pRetVal.iBlock, err = aes.NewCipher(aKey); nil != err {
		return
	}
	pRetVal.IV = aIV
	if nil == aIV {
		pRetVal.IV = make([]byte, pRetVal.iBlock.BlockSize())
		if _, err = rand.Read(pRetVal.IV); nil != err {
			return
		}
	}
	pRetVal.iStream = cipher.NewCTR(pRetVal.iBlock, pRetVal.IV)
	pRetVal.Buffer = make([]byte, int(nBufferSize/16)*16)
	return
}

//Do .
func (th *Cipher) Do(n int) []byte {
	th.iStream.XORKeyStream(th.Buffer, th.Buffer[:n])
	return th.Buffer[:n]
}
