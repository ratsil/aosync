package main

import (
	"crypto/rand"
	"errors"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	s "strings"
	"sync"
	t "time"
)

var (
	_sLocal      string
	_sRemote     string
	_nBufferSize int
)

//Denc .
type Denc struct {
	Cipher  *Cipher
	Decrypt bool
}

func main() {
	var err error
	pLocal := flag.String("local", "", "path to the local folder. flag is mandatory")
	pRemote := flag.String("remote", "", "path to the remote folder. flag is mandatory")
	pPublicKey := flag.String("public", "", "path to the public key. flag is optional")
	pPrivateKey := flag.String("private", "", "path to the private key. flag is optional but requires the public flag")
	pBuffer := flag.String("buffer", "10240", "buffer size in KB. default is 10240KB (i.e. 10MB)")
	flag.Parse()
	sUsage := "usage: aosync -local=/path/to/local/folder -remote=/path/to/remote/folder [-public=/path/to/public/key/file [-private=/path/to/private/key/file]] [-buffer=10240]"
	if nil == pLocal || nil == pRemote {
		log.Fatal("you should specify local and remote folders! " + sUsage)
	}
	_sLocal = *pLocal
	_sRemote = *pRemote

	if nil != pPublicKey && 0 < len(*pPublicKey) {
		a, err := ioutil.ReadFile(*pPublicKey)
		if nil != err {
			log.Fatal("cannot find public key file:" + *pPublicKey)
		}
		if err = PublicKey(a); nil != err {
			log.Fatal(err)
		}
	}
	if nil != pPrivateKey && 0 < len(*pPrivateKey) {
		if nil == _pPublicKey {
			log.Fatal("missing public key! " + sUsage)
		}
		a, err := ioutil.ReadFile(*pPrivateKey)
		if nil != err {
			log.Fatal("cannot find private key file:" + *pPrivateKey)
		}
		if err = PrivateKey(a); nil != err {
			log.Fatal(err)
		}
	}
	if _nBufferSize, err = strconv.Atoi(*pBuffer); nil != err {
		log.Fatal("buffer should be an integer value! " + sUsage)
	}

	bFound := false
	// 1. copy from nas/tape with decrypt
	// 2. copy/move to nas/tape with encrypt
	// 3. move from nas to tape
	// 4. copy between nas and tape
	// 5. encrypt nas/tape
	sSource := _sLocal
	sTarget := _sRemote
	var wg sync.WaitGroup
	for n := 0; 2 > n; n++ {
		if !exists(sSource) {
			log.Fatal("folder '" + sSource + "' does not exists!")
		}
		if exists(sSource + "/.copy") {
			if !exists(sSource + "/.copy/.done") {
				log.Fatal("folder '" + sSource + "/.copy/.done' does not exists!")
			}
			bFound = true
			wg.Add(1)
			go func(sSource, sTarget string, b bool) {
				defer wg.Done()
				var pDenc *Denc
				if nil != _pPrivateKey {
					pDenc = &Denc{Decrypt: b}
				}
				if err := cp(sSource, sTarget, pDenc); nil != err {
					log.Println(err)
				}
			}(sSource, sTarget, 0 < n)
		}
		if exists(sSource + "/.move") {
			bFound = true
			wg.Add(1)
			go func(sSource, sTarget string, b bool) {
				defer wg.Done()
				var pDenc *Denc
				if nil != _pPrivateKey {
					pDenc = &Denc{Decrypt: b}
				}
				if err := mv(sSource, sTarget, pDenc); nil != err {
					log.Println(err)
				}
			}(sSource, sTarget, 0 < n)
		}
		if nil != _pPublicKey && exists(sSource+"/.encrypt") {
			if !exists(sSource + "/.encrypt/.done") {
				log.Fatal("folder '" + sSource + "/.encrypt/.done' does not exists!")
			}
			if !exists(sSource + "/.encrypt/.encrypted") {
				log.Fatal("folder '" + sSource + "/.encrypt/.encrypted' does not exists!")
			}
			bFound = true
			wg.Add(1)
			go func(sSource string) {
				defer wg.Done()
				if err := cpd(sSource+"/.encrypt", sSource+"/.encrypt/.encrypted", sSource+"/.encrypt/.done", []string{".done", ".encrypted"}, new(Denc)); nil != err {
					log.Println(err)
				}
			}(sSource)
		}
		sSource = sTarget
		sTarget = _sLocal
	}
	if !bFound {
		log.Fatal("there are no any .copy or .move subfolders. nothing to do. bye!")
	}
	wg.Wait()
}
func cp(sSource, sTarget string, pDenc *Denc) error {
	return cpd(sSource+"/.copy", sTarget, sSource+"/.copy/.done", []string{".done"}, pDenc)
}
func cpd(sSource, sTarget, sDone string, aExcludes []string, pDenc *Denc) (err error) {
	aEntries, err := ioutil.ReadDir(sSource)
	if nil != err {
		log.Print(sSource + ":" + sTarget)
		debug.PrintStack()
		return
	}
	var oFileInfo os.FileInfo
	for _, oEntry := range aEntries {
		sSourceEntry := oEntry.Name()
		for _, sExclude := range aExcludes {
			if sExclude == sSourceEntry {
				sSourceEntry = "."
				break
			}
		}
		if "." == sSourceEntry || ".." == sSourceEntry {
			continue
		}
		sTargetEntry := filepath.Join(sTarget, sSourceEntry)
		sDoneEntry := filepath.Join(sDone, sSourceEntry)
		sSourceEntry = filepath.Join(sSource, sSourceEntry)

		if oFileInfo, err = os.Stat(sSourceEntry); nil != err {
			return
		}

		switch oFileInfo.Mode() & os.ModeType {
		case os.ModeDir:
			if err = os.MkdirAll(sTargetEntry, 0777); nil != err {
				return
			}
			if err = os.MkdirAll(sDoneEntry, 0777); nil != err {
				return
			}
			if err = cpd(sSourceEntry, sTargetEntry, sDoneEntry, nil, pDenc); nil != err {
				return
			}
			if err = os.Remove(sSourceEntry); nil != err {
				return
			}
		case os.ModeSymlink:
			log.Println("symlink ignored:" + sSourceEntry)
		default:
			if nil != pDenc {
				if !pDenc.Decrypt {
					sTargetEntry += ".aose"
				} else if s.HasSuffix(s.ToLower(sTargetEntry), ".aose") {
					sTargetEntry = sTargetEntry[:len(sTargetEntry)-5]
				}
			}
			if err = cpf(sSourceEntry, sTargetEntry, pDenc); nil != err {
				return
			}
			if err = os.Rename(sSourceEntry, sDoneEntry); nil != err {
				return
			}
		}
	}
	return nil
}
func cpf(sSource, sTarget string, pDenc *Denc) error {
	if exists(sTarget) {
		log.Println("'" + sTarget + "' exists")
		return nil
	}
	pSource, err := os.Open(sSource)
	defer pSource.Close()
	if nil != err {
		return err
	}
	pTarget, err := os.Create(sTarget)
	defer pTarget.Close()
	if nil != err {
		return err
	}
	log.Print("start copy: " + sSource)
	if nil != pDenc && pDenc.Decrypt && !s.HasSuffix(s.ToLower(sSource), ".aose") {
		return copy(pTarget, pSource, nil)
	}
	return copy(pTarget, pSource, pDenc)
}

func mv(sSource, sTarget string, pDenc *Denc) error {
	return mvd(sSource+"/.move", sTarget, pDenc)
}
func mvd(sSource, sTarget string, pDenc *Denc) (err error) {
	aEntries, err := ioutil.ReadDir(sSource)
	if nil != err {
		return
	}
	var oFileInfo os.FileInfo
	for _, oEntry := range aEntries {
		sSourceEntry := oEntry.Name()
		if "." == sSourceEntry || ".." == sSourceEntry {
			continue
		}
		sTargetEntry := filepath.Join(sTarget, sSourceEntry)
		sSourceEntry = filepath.Join(sSource, sSourceEntry)

		if oFileInfo, err = os.Stat(sSourceEntry); nil != err {
			return
		}

		switch oFileInfo.Mode() & os.ModeType {
		case os.ModeDir:
			if err = os.MkdirAll(sTargetEntry, 0777); nil != err {
				return
			}
			if err = mvd(sSourceEntry, sTargetEntry, pDenc); nil != err {
				return
			}
			if err = os.Remove(sSourceEntry); nil != err {
				log.Print(err.Error())
			}
		case os.ModeSymlink:
			log.Println("symlink ignored:" + sSourceEntry)
		default:
			if nil != pDenc {
				if !pDenc.Decrypt {
					sTargetEntry += ".aose"
				} else if s.HasSuffix(s.ToLower(sTargetEntry), ".aose") {
					sTargetEntry = sTargetEntry[:len(sTargetEntry)-5]
				}
			}
			if err = mvf(sSourceEntry, sTargetEntry, pDenc); nil != err {
				return
			}
		}
	}
	return nil
}
func mvf(sSource, sTarget string, pDenc *Denc) (err error) {
	if exists(sTarget) {
		log.Println("'" + sTarget + "' exists")
		return
	}
	pSource, err := os.Open(sSource)
	defer pSource.Close()
	if nil != err {
		return
	}
	pTarget, err := os.Create(sTarget)
	defer pTarget.Close()
	if nil != err {
		return
	}

	log.Print("start move: " + sSource)
	if err = copy(pTarget, pSource, pDenc); nil != err {
		return
	}
	if err = pSource.Close(); nil != err {
		return
	}
	return os.Remove(sSource)
}

func copy(pTarget, pSource *os.File, pDenc *Denc) (err error) {
	nBytes := int64(0)
	tStart := t.Now()
	if nil != pDenc {
		if !pDenc.Decrypt {
			aPassword := make([]byte, 32)
			if _, err = rand.Read(aPassword); nil != err {
				return err
			}

			pDenc.Cipher, err = NewCipher(aPassword, nil, _nBufferSize*1024)
			if nil != err {
				return err
			}
			if aPassword, err = EncryptByKey(aPassword); nil != err {
				return err
			}
			if _, err = pTarget.Write([]byte("aose")); nil != err {
				return err
			}
			if _, err = pTarget.Write(aPassword); nil != err {
				return err
			}
			if _, err = pTarget.Write(pDenc.Cipher.IV); nil != err {
				return err
			}
		} else {
			aHeader := make([]byte, 4+256+16)
			if _, err = pSource.Read(aHeader); nil != err {
				return err
			}
			if "aose" != string(aHeader[:4]) {
				return errors.New("wrong encrypted format for " + pSource.Name())
			}
			aPassword, err := DecryptByKey(aHeader[4:260])
			if nil != err {
				return err
			}
			pDenc.Cipher, err = NewCipher(aPassword, aHeader[260:], _nBufferSize*1024)
		}
		for {
			n, err := pSource.Read(pDenc.Cipher.Buffer)
			if 0 < n {
				if _, err = pTarget.Write(pDenc.Cipher.Do(n)); nil != err {
					return err
				}
				nBytes += int64(n)
			}
			if io.EOF == err {
				break
			}
		}
	} else if nBytes, err = io.CopyBuffer(pTarget, pSource, make([]byte, _nBufferSize*1024)); nil != err {
		return
	}
	dElapsed := t.Since(tStart)
	nSpeed := int64(dElapsed / t.Second)
	if 0 < nSpeed {
		nSpeed = (((nBytes * 8) / nSpeed) / 1024 / 1024)
	}
	log.Printf("finished. elapsed:"+dElapsed.String()+"; ~%dMbit/s.", nSpeed)
	return
}

func exists(sPath string) bool {
	if _, err := os.Stat(sPath); os.IsNotExist(err) {
		return false
	}
	return true
}
