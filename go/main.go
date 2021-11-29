package main

import (
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	s "strings"
	"sync"
	"time"
	t "time"

	"github.com/ratsil/go-helpers/log"
)

var (
	_sLocalInstance  string
	_sLocalFolder    string
	_sRemoteInstance string
	_sRemoteFolder   string
	_nBufferSize     int
	_aBuffers        [][]byte
	_cBuffersQueue   chan *Buffer
	_dLog            t.Duration
)

//Buffer .
type Buffer struct {
	Index  int
	Length int
}

//Denc .
type Denc struct {
	Cipher  *Cipher
	Decrypt bool
}

func main() {
	var err error
	pLocal := flag.String("local", "", "path to the local folder along with local instance name separated by @. latter will be used to determine target control folders. flag is mandatory")
	pRemote := flag.String("remote", "", "path to the remote folder along with target instance name separated by @. latter will be used to determine target control folders. flag is mandatory")
	pPublicKey := flag.String("public", "", "path to the public key. flag is optional")
	pPrivateKey := flag.String("private", "", "path to the private key. flag is optional but requires the public flag")
	pBuffer := flag.String("buffer", "102400@10", "buffer size in KB and qty of subbuffers. default is 102400KB (i.e. 100MB) in total and 10 subbuffers (i.e. 10 buffers of 10MB each")
	pLog := flag.String("log", "./aosync@300", "log path with file prefix and period in seconds separated by @ default is a ./aosync@300")
	flag.Parse()
	sUsage := "usage: aosync -local=/path/to/local/folder@some_local_instance_name -remote=/path/to/remote/folder@some_remote_instance_name [-public=/path/to/public/key/file [-private=/path/to/private/key/file]] [-buffer=102400[@10]] [-log=[/path/to/log/folder/]prefix[@300]]"

	nLogPeriod := 300
	a := s.Split(*pLog, "@")
	if 2 == len(a) {
		if nLogPeriod, err = strconv.Atoi(a[1]); nil != err {
			panic("log period should be an integer value! " + sUsage)
		}
	}
	sLogPath := "."
	if sLogPath, err = filepath.Abs(filepath.Dir(a[0])); nil != err {
		panic("wrong log path:" + err.Error())
	}
	log.Default(sLogPath, filepath.Base(a[0]))
	_dLog = t.Second * t.Duration(nLogPeriod)

	log.Notice("********* START:")

	sAOSync, err := os.Executable()
	if err == nil {
		log.Notice(sAOSync)
	} else {
		log.Error(err)
	}

	err = errors.New("you should specify local and remote folders along with instance names! " + sUsage)
	if nil != pLocal && nil != pRemote {
		a := s.Split(*pLocal, "@")
		if 2 == len(a) && exists(a[0]) && 0 < len(a[1]) {
			_sLocalFolder, _sLocalInstance = a[0], a[1]
			a = s.Split(*pRemote, "@")
			if 2 == len(a) && exists(a[0]) && 0 < len(a[1]) {
				_sRemoteFolder, _sRemoteInstance = a[0], a[1]
				for _, p := range []*string{&_sLocalFolder, &_sRemoteFolder} {
					if !s.HasPrefix(*p, `\\?\`) {
						if s.HasPrefix(*p, `\\`) {
							*p = `UNC` + (*p)[1:]
						}
						*p = `\\?\` + *p
					}
				}
				err = nil
			}
		}

	}
	log.Fatal(err)

	sControl := filepath.Join(_sLocalFolder, ".aosync")

	if 0 < len(sAOSync) {
		sUpdate := filepath.Join(sControl, ".update", filepath.Base(sAOSync))
		if !exists(sUpdate) {
			sUpdate = filepath.Join(_sRemoteFolder, ".aosync", _sLocalInstance, ".update", filepath.Base(sAOSync))
		}
		if exists(sUpdate) {
			sBackup := sAOSync + ".bkp"
			if exists(sBackup) {
				log.Notice("backup exists:" + sBackup + ". removing")
				log.Error(os.Remove(sBackup))
			}
			log.Notice("create backup:" + sBackup)
			errBackup := log.Error(os.Rename(sAOSync, sBackup))
			log.Notice("updating")
			var aBytes []byte
			if aBytes, err = ioutil.ReadFile(sUpdate); nil == err {
				if nil == log.Error(ioutil.WriteFile(sAOSync, aBytes, 0777)) {
					if nil == log.Error(os.Remove(sUpdate)) {
						log.Notice("updated. exiting")
						t.Sleep(t.Second)
						return
					}
				}
			}
			log.Notice("update failed")
			if nil == errBackup {
				log.Notice("trying to restore from backup")
				log.Error(os.Rename(sBackup, sAOSync))
			}
		}
	}

	if nil != pPublicKey && 0 < len(*pPublicKey) {
		a, err := ioutil.ReadFile(*pPublicKey)
		if nil != err {
			log.Fatal(errors.New("cannot find public key file:" + *pPublicKey))
		}
		if err = PublicKey(a); nil != err {
			log.Fatal(err)
		}
	}
	if nil != pPrivateKey && 0 < len(*pPrivateKey) {
		if nil == _pPublicKey {
			log.Fatal(errors.New("missing public key! " + sUsage))
		}
		a, err := ioutil.ReadFile(*pPrivateKey)
		if nil != err {
			log.Fatal(errors.New("cannot find private key file:" + *pPrivateKey))
		}
		if err = PrivateKey(a); nil != err {
			log.Fatal(err)
		}
	}

	sSource := filepath.Join(sControl, ".stop")
	if exists(sSource) {
		log.Fatal(errors.New("found " + sSource + ". please remove to continue. now exiting"))
	}
	sSource = filepath.Join(sControl, ".stop!")
	if exists(sSource) {
		log.Fatal(errors.New("found " + sSource + ". please remove to continue. now exiting"))
	}
	go func(s string) {
		for {
			t.Sleep(t.Minute)
			if exists(s) {
				log.Fatal(errors.New("found " + s + ". please remove to continue. now exiting"))
				t.Sleep(t.Second)
				panic("to be sure")
			}
		}
	}(sSource)

	sSource = filepath.Join(sControl, ".restart")
	if exists(sSource) {
		if err = os.Remove(sSource); nil != err {
			log.Fatal(err)
		}
		log.Notice("found " + sSource + " on start. removed")
	}

	a = s.Split(*pBuffer, "@")
	if 1 == len(a) || 1 > len(a[1]) {
		a = append(a, "")
		a[1] = "10"
	}

	if _nBufferSize, err = strconv.Atoi(a[0]); nil != err {
		log.Fatal(errors.New("buffer size should be an integer value! " + sUsage))
	}
	nBuffers := 10
	if nBuffers, err = strconv.Atoi(a[1]); nil != err {
		log.Fatal(errors.New("subbuffers qty should be an integer value! " + sUsage))
	}
	_aBuffers = make([][]byte, nBuffers)
	_nBufferSize *= 1024 / nBuffers
	_cBuffersQueue = make(chan *Buffer, nBuffers)
	for n := 0; nBuffers > n; n++ {
		_aBuffers[n] = make([]byte, _nBufferSize)
		_cBuffersQueue <- &Buffer{n, _nBufferSize}
	}
	debug.SetGCPercent(5)
	bFound := false
	// 1. copy from nas/tape with decrypt
	// 2. copy/move to nas/tape with encrypt
	// 3. move from nas to tape
	// 4. copy between nas and tape
	// 5. encrypt nas/tape
	sSource = filepath.Join(sControl, _sRemoteInstance)
	sTarget := _sRemoteFolder
	var wg sync.WaitGroup
	for n := 0; 2 > n; n++ {
		if exists(sSource) {
			if exists(filepath.Join(sSource, ".copy")) {
				if !exists(filepath.Join(sSource, ".copy", ".done")) {
					log.Fatal(errors.New("folder '" + filepath.Join(sSource, ".copy", ".done") + "' does not exists!"))
				}
				bFound = true
				wg.Add(1)
				go func(sSource, sTarget string, b bool) {
					defer wg.Done()
					var pDenc *Denc
					if nil != _pPublicKey {
						pDenc = &Denc{Decrypt: nil != _pPrivateKey && b}
					}
					if err := cpd(filepath.Join(sSource, ".copy"), sTarget, filepath.Join(sSource, ".copy", ".done"), []string{".done"}, pDenc); nil != err {
						log.Error(err)
					}
				}(sSource, sTarget, 0 < n)
			}
			if exists(sSource + "/.move") {
				bFound = true
				wg.Add(1)
				go func(sSource, sTarget string, b bool) {
					defer wg.Done()
					var pDenc *Denc
					if nil != _pPublicKey {
						pDenc = &Denc{Decrypt: nil != _pPrivateKey && b}
					}
					if err := cpd(filepath.Join(sSource, ".move"), sTarget, "", []string{""}, pDenc); nil != err {
						log.Error(err)
					}
				}(sSource, sTarget, 0 < n)
			}
			if nil != _pPublicKey && nil == _pPrivateKey {
				sEncrypt := filepath.Join(sSource, "..", ".encrypt")
				if exists(sEncrypt) {
					sEncrypted := filepath.Join(sEncrypt, ".encrypted")
					if !exists(sEncrypted) {
						log.Fatal(errors.New("folder '" + sEncrypted + "' does not exists!"))
					}
					sDone := filepath.Join(sEncrypt, ".done")
					if !exists(sDone) {
						sDone = ""
					}
					bFound = true
					wg.Add(1)
					go func(sEncrypt, sEncrypted, sDone string) {
						defer wg.Done()
						if 1 > len(sDone) {
							if err := cpd(sEncrypt, sEncrypted, "", []string{".encrypted"}, new(Denc)); nil != err {
								log.Error(err)
							}
						} else {
							if err := cpd(sEncrypt, sEncrypted, sDone, []string{".done", ".encrypted"}, new(Denc)); nil != err {
								log.Error(err)
							}
						}
					}(sEncrypt, sEncrypted, sDone)
				}
			}
		} else {
			log.Warning(errors.New("folder '" + sSource + "' does not exists!"))
		}
		sSource = filepath.Join(_sRemoteFolder, ".aosync", _sLocalInstance)
		sTarget = _sLocalFolder
	}
	if !bFound {
		log.Fatal(errors.New("there are no any .copy, .move or .encrypt subfolders. nothing to do. bye"))
	}
	wg.Wait()
	log.Notice("********* STOP")
	t.Sleep(t.Second)
}

func cpd(sSource, sTarget, sDone string, aExcludes []string, pDenc *Denc) (err error) {
	aEntries, err := ioutil.ReadDir(sSource)
	if nil != err {
		log.Notice(sSource + ":" + sTarget)
		debug.PrintStack()
		return
	}
	bCopy := 0 < len(sDone)
	var oFileInfo os.FileInfo
	for _, oEntry := range aEntries {
		sSourceEntry := oEntry.Name()

		sControl := filepath.Join(_sLocalFolder, ".aosync", ".restart")
		err = errors.New("found " + sControl + " before " + sSourceEntry)
		if !exists(sControl) {
			sControl = filepath.Join(_sLocalFolder, ".aosync", ".stop")
			if exists(sControl) {
				err = errors.New("found " + sControl + " before " + sSourceEntry)
			} else {
				err = nil
			}
		}
		if nil != err {
			log.Warning(err)
			return nil
		}

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
		var sDoneEntry string
		if bCopy {
			sDoneEntry = filepath.Join(sDone, sSourceEntry)
		}
		sSourceEntry = filepath.Join(sSource, sSourceEntry)
		if oFileInfo, err = os.Stat(sSourceEntry); nil != err {
			return
		}

		switch oFileInfo.Mode() & os.ModeType {
		case os.ModeDir:
			if err = os.MkdirAll(sTargetEntry, 0777); nil != err {
				return
			}
			if bCopy {
				if err = os.MkdirAll(sDoneEntry, 0777); nil != err {
					return
				}
			}
			if err = cpd(sSourceEntry, sTargetEntry, sDoneEntry, nil, pDenc); nil != err {
				return
			}
			log.Error(os.Remove(sSourceEntry))
		case os.ModeSymlink:
			log.Notice("symlink ignored:" + sSourceEntry)
		default:
			pDENC := pDenc
			if nil != pDENC {
				bAose := s.HasSuffix(s.ToLower(sSourceEntry), ".aose")
				if (pDENC.Decrypt && !bAose) || (!pDENC.Decrypt && bAose) {
					pDENC = nil
				} else if !pDENC.Decrypt {
					sTargetEntry += ".aose"
				} else if s.HasSuffix(s.ToLower(sTargetEntry), ".aose") {
					sTargetEntry = sTargetEntry[:len(sTargetEntry)-5]
				}
			}

			if !exists(sTargetEntry) {
				var pSource, pTarget *os.File
				if pSource, err = os.Open(sSourceEntry); nil != err {
					return
				}
				defer pSource.Close()

				if pTarget, err = os.Create(sTargetEntry); nil != err {
					return
				}
				defer pTarget.Close()
				sPrecision := "0f"
				nSize := float64(oFileInfo.Size())
				if 1024 < nSize {
					sPrecision = "2f KB"
					nSize /= 1024
					if 1024 < nSize {
						sPrecision = "2f MB"
						nSize /= 1024
						if 1024 < nSize {
							sPrecision = "2f GB"
							nSize /= 1024
						}
					}
				}
				log.Notice(fmt.Sprintf(" ***** start copy (%."+sPrecision+"): %s => %s", nSize, sSourceEntry, sTargetEntry))
				if err = copy(pTarget, pSource, pDENC); nil != err {
					return
				}
				log.Error(pSource.Close())
				if !bCopy {
					if err = os.Remove(sSourceEntry); nil != err {
						log.Error(err)
						t.Sleep(t.Second)
						log.Error(os.Remove(sSourceEntry))
					}
				}
			} else {
				log.Notice("'" + sTargetEntry + "' exists")
			}
			if bCopy {
				if err = os.Rename(sSourceEntry, sDoneEntry); nil != err {
					log.Error(err)
					t.Sleep(t.Second)
					log.Error(os.Rename(sSourceEntry, sDoneEntry))
				}
			}
		}
	}
	return nil
}

func copy(pTarget, pSource *os.File, pDenc *Denc) (err error) {
	pSpeedRead := new(Speed)
	pSpeedRead.Start()
	pSpeedDenc := new(Speed)
	pSpeedWrite := new(Speed)
	pSpeedWrite.Start()
	oFI, err := pSource.Stat()
	if nil != err {
		return err
	}
	sFilename := filepath.Base(oFI.Name())
	nSizeTotalWrite := oFI.Size()
	cWriteQueue := make(chan *Buffer, len(_aBuffers))

	if nil != pDenc {
		pSpeedDenc.Start()
		nSizeHeader := int64(4 + 256 + 16)
		if !pDenc.Decrypt {
			nSizeTotalWrite += nSizeHeader
			aPassword := make([]byte, 32)
			if _, err = rand.Read(aPassword); nil != err {
				return err
			}

			pDenc.Cipher, err = NewCipher(aPassword, nil)
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
			nSizeTotalWrite -= nSizeHeader
			aHeader := make([]byte, nSizeHeader)
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
			pDenc.Cipher, err = NewCipher(aPassword, aHeader[260:])
		}
	}
	go func() {
		defer close(cWriteQueue)
		tLog := t.Now()
		pSpeedReadPop := new(Speed)
		pSpeedReadPop.Start()
		pSpeedReadPush := new(Speed)
		pSpeedReadPush.Start()
		nRetries := 0
		for {
			var (
				pBuffer   *Buffer
				c         chan *Buffer
				bContinue bool
				n         int
			)

			if _dLog < t.Since(tLog) {
				tLog = t.Now()
				log.Printf("read (%s) %s. Along with POP for %s; PUSH for %s", sFilename, pSpeedRead.String(), pSpeedReadPop.Average().String(), pSpeedReadPush.Average().String())
				if nil != pDenc {
					log.Printf("denc (%s) %s", sFilename, pSpeedDenc.String())
				}
				pSpeedRead.Restart()
				pSpeedReadPop.Restart()
				pSpeedReadPush.Restart()
				pSpeedDenc.Restart()
			}

			pSpeedReadPop.Wake()
			select {
			case pBuffer, bContinue = <-_cBuffersQueue:
				nRetries = 0
				pSpeedReadPop.Sleep(1)
				if 1 > pBuffer.Length {
					continue
				}

				pSpeedRead.Wake()
				n, err = pSource.Read(_aBuffers[pBuffer.Index])
				pSpeedRead.Sleep(int64(n))
				if 0 < n {
					if nil != pDenc {
						pSpeedDenc.Wake()
						pDenc.Cipher.Do(_aBuffers[pBuffer.Index][:n])
						pSpeedDenc.Sleep(int64(n))
					}
					pBuffer.Length = n
					c = cWriteQueue
				} else {
					c = _cBuffersQueue
				}
				pSpeedReadPush.Wake()
				select {
				case c <- pBuffer:
				case <-time.After(5 * time.Minute):
					log.Fatal(errors.New("timeout on queue full buffer or re-queue unused one after file read for " + sFilename))
				}
				pSpeedReadPush.Sleep(1)
			case <-time.After(5 * time.Minute):
				nRetries++
				if err := log.Error(fmt.Errorf("timeout on get free buffer before read for %s (retries:%d of 5)", sFilename, nRetries)); 4 < nRetries {
					panic(err)
				}
				continue
			default:
				t.Sleep(t.Second)
				continue
			}
			if nil != err || !bContinue {
				if io.EOF == err {
					err = nil
				}
				break
			}
		}
		log.Error(err)
		//log.Notice("READ (" + sFilename + ") RETURNED!")
	}()
	var pBuffer *Buffer
	tLog := t.Now()
	nBytesUsed := 0
	pSpeedWritePop := new(Speed)
	pSpeedWritePop.Start()
	pSpeedWritePush := new(Speed)
	pSpeedWritePush.Start()
	nRetries := 0
	for {
		if _dLog < t.Since(tLog) {
			tLog = t.Now()

			log.Printf("WRITE (%s:%d%%) %s. Along with POP for %s and PUSH for %s", sFilename, int64(pSpeedWrite.OverallQty*100/nSizeTotalWrite), pSpeedWrite.String(), pSpeedWritePop.Average().String(), pSpeedWritePush.Average().String())
			pSpeedWrite.Restart()
			pSpeedWritePop.Restart()
			pSpeedWritePush.Restart()
		}
		bContinue := false
		n := 0
		pSpeedWritePop.Wake()
		select {
		case pBuffer, bContinue = <-cWriteQueue:
			nRetries = 0
			if nil != pBuffer && 0 < pBuffer.Length {
				pSpeedWritePop.Sleep(1)
				pSpeedWrite.Wake()
				n, err = pTarget.Write(_aBuffers[pBuffer.Index][:pBuffer.Length])
				pSpeedWrite.Sleep(int64(n))
				if nil != err {
					return
				}
				if n != pBuffer.Length {
					log.Fatal(errors.New("n != pBuffer.Length for " + sFilename))
				}
				nBytesUsed += n
				if 1024*1024*1024 < nBytesUsed {
					nBytesUsed = 0
					go debug.FreeOSMemory()
				}
				pBuffer.Length = _nBufferSize
				select {
				case _cBuffersQueue <- pBuffer:
				case <-time.After(5 * time.Minute):
					log.Fatal(errors.New("timeout on re-queue the buffer after file write for " + sFilename))
				}

			}
		case <-time.After(5 * time.Minute):
			nRetries++
			if err := log.Error(fmt.Errorf("timeout on read full buffer before write for %s (retries:%d of 5)", sFilename, nRetries)); 4 < nRetries {
				panic(err)
			}
			continue
		default:
			t.Sleep(t.Second)
			continue
		}
		if !bContinue {
			break
		}
	}
	log.Printf(" ***** copy (%s) finished with %s", sFilename, pSpeedWrite.String())
	log.Error(pTarget.Sync())
	log.Error(pTarget.Close())
	return os.Chtimes(pTarget.Name(), oFI.ModTime(), oFI.ModTime())
}

func exists(sPath string) bool {
	if _, err := os.Stat(sPath); os.IsNotExist(err) {
		return false
	}
	return true
}
