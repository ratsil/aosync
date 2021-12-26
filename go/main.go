package main

import (
	. "aosync/lib/preferences"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/debug"
	s "strings"

	//"sync"
	"time"
	t "time"

	. "github.com/ratsil/go-helpers/common"

	"github.com/google/uuid"
	"github.com/ratsil/go-helpers/log"
)

const (
	TASKS_MAX_BY_PRIORITY = 1024 * 1024
)

var (
	_pPreferences *Preferences
	_aFilesTasks  [int(FilesTaskPriorityMax) + 1]chan *FilesTask
	_mFilesTasks  map[string]bool
	_bControlStop bool
	_sAOSync      string

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
	Index  byte
	Length int
}

//Denc .
type Denc struct {
	Cipher  *Cipher
	Decrypt *bool
}

func main() {
	var err error
	pPrefs := flag.String("prefs", "./preferences.json", "path to the preferences file. flag is optional")
	flag.Parse()
	//sUsage := "usage: aosync [-prefs=/path/to/preferences/file]"

	if _pPreferences, err = PreferencesRead(*pPrefs); nil != err {
		if nil == _pPreferences {
			log.Fatal(err)
		}
		log.Error(err)
	}
	for _, sFolder := range []string{_pPreferences.Folder, _pPreferences.Log.Folder} {
		if !exists(sFolder) {
			log.Fatal(errors.New("folder '" + sFolder + "' does not exist!"))
		}
	}
	log.Default(_pPreferences.Log.Folder, _pPreferences.Log.Prefix)
	_dLog = t.Second * t.Duration(_pPreferences.Log.Period)

	for n := 0; int(FilesTaskPriorityMax) >= n; n++ {
		_aFilesTasks[n] = make(chan *FilesTask, TASKS_MAX_BY_PRIORITY)
	}
	_mFilesTasks = make(map[string]bool)

	if nil != _pPreferences.Denc.Keys {
		if nil != _pPreferences.Denc.Keys.Public && 0 < len(*_pPreferences.Denc.Keys.Public) {
			a, err := ioutil.ReadFile(*_pPreferences.Denc.Keys.Public)
			if nil != err {
				log.Fatal(errors.New("cannot find public key file:" + *_pPreferences.Denc.Keys.Public))
			}
			if err = PublicKey(a); nil != err {
				log.Fatal(err)
			}
		}
		if nil != _pPreferences.Denc.Keys.Private && 0 < len(*_pPreferences.Denc.Keys.Private) {
			if nil == _pPublicKey {
				log.Fatal(errors.New("missing public key! to use a private key, there should be a public one"))
			}
			a, err := ioutil.ReadFile(*_pPreferences.Denc.Keys.Private)
			if nil != err {
				log.Fatal(errors.New("cannot find private key file:" + *_pPreferences.Denc.Keys.Private))
			}
			if err = PrivateKey(a); nil != err {
				log.Fatal(err)
			}
		}
	}

	_nBufferSize = int(_pPreferences.Buffers.Size)

	_aBuffers = make([][]byte, _pPreferences.Buffers.Qty)
	_cBuffersQueue = make(chan *Buffer, _pPreferences.Buffers.Qty)
	for n := byte(0); _pPreferences.Buffers.Qty > n; n++ {
		_aBuffers[n] = make([]byte, _nBufferSize)
		_cBuffersQueue <- &Buffer{n, _nBufferSize}
	}
	debug.SetGCPercent(5)

	log.Notice("********* START:")
	_sAOSync, err = os.Executable()
	if err == nil {
		log.Notice(_sAOSync)
	} else {
		log.Error(err)
	}

	controls()
	if _bControlStop {
		log.Fatal(errors.New("stop task found. please remove to continue. now exiting"))
		return
	}
	go func() {
		for {
			t.Sleep(t.Minute)
			controls()
		}
	}()

	var pTask *FilesTask
	sTasksDone := filepath.Join(_pPreferences.Folder, "tasks", ".done")
	sTasksFailed := filepath.Join(_pPreferences.Folder, "tasks", ".failed")
	for {
		tasks()
		for n := FilesTaskPriorityMax; FilesTaskPrioritySkip < n; n-- {
			select {
			case pTask = <-_aFilesTasks[n]:
				var pDenc *Denc
				if nil != pTask.Denc {
					if FilesTaskDencDecrypt == *pTask.Denc && nil == _pPrivateKey {
						err = errors.New("FILES TASK ERROR (" + pTask.File + "): cannot decrypt without private key")
					} else if FilesTaskDencEncrypt == *pTask.Denc && nil == _pPublicKey {
						err = errors.New("FILES TASK ERROR (" + pTask.File + "): cannot encrypt without public key")
					} else {
						pDenc = &Denc{}
						if FilesTaskDencAuto != *pTask.Denc {
							pDenc.Decrypt = new(bool)
							*pDenc.Decrypt = (FilesTaskDencDecrypt == *pTask.Denc)
						}
					}
				}
				if nil == err {
					switch pTask.Type {
					case FilesTaskTypeCopy, FilesTaskTypeMove:
						var oFI fs.FileInfo
						if oFI, err = os.Stat(pTask.Source); err == nil {
							if oFI.IsDir() {
								err = cpd(pTask.Source, pTask.Target, (FilesTaskTypeCopy == pTask.Type), pDenc, pTask.Exist)
							} else {
								err = cpf(pTask.Source, pTask.Target, (FilesTaskTypeCopy == pTask.Type), pDenc, pTask.Exist)
							}
						}
					case FilesTaskTypeEncode:
					case FilesTaskTypeDecode:
					case FilesTaskTypeDate:
					}
				}
				sFilename := filepath.Base(pTask.File)
				if nil != err {
					log.Error(errors.New("FILES TASK ERROR (" + pTask.File + "): " + err.Error()))
					log.Error(os.MkdirAll(sTasksFailed, 0777))
					log.Error(os.Rename(pTask.File, filepath.Join(sTasksFailed, sFilename)))
				} else {
					log.Notice("FILES TASK SUCCESS (" + pTask.File + ")")
					log.Error(os.MkdirAll(sTasksDone, 0777))
					log.Error(os.Rename(pTask.File, filepath.Join(sTasksDone, sFilename)))
				}
				n = FilesTaskPrioritySkip
			default:
			}
		}
		t.Sleep(t.Second)
	}

	// 1. copy from nas/tape with decrypt
	// 2. copy/move to nas/tape with encrypt
	// 3. move from nas to tape
	// 4. copy between nas and tape
	// 5. encrypt nas/tape
	// sSource = filepath.Join(sControl, _sRemoteInstance)
	// sTarget := _sRemoteFolder
	// var wg sync.WaitGroup
	// for n := 0; 2 > n; n++ {
	// 	if exists(sSource) {
	// 		if exists(filepath.Join(sSource, ".copy")) {
	// 			if !exists(filepath.Join(sSource, ".copy", ".done")) {
	// 				log.Fatal(errors.New("folder '" + filepath.Join(sSource, ".copy", ".done") + "' does not exist!"))
	// 			}
	// 			bFound = true
	// 			wg.Add(1)
	// 			go func(sSource, sTarget string, b bool) {
	// 				defer wg.Done()
	// 				var pDenc *Denc
	// 				if nil != _pPublicKey {
	// 					pDenc = &Denc{Decrypt: nil != _pPrivateKey && b}
	// 				}
	// 				if err := cpd(filepath.Join(sSource, ".copy"), sTarget, filepath.Join(sSource, ".copy", ".done"), []string{".done"}, pDenc); nil != err {
	// 					log.Error(err)
	// 				}
	// 			}(sSource, sTarget, 0 < n)
	// 		}
	// 		if exists(sSource + "/.move") {
	// 			bFound = true
	// 			wg.Add(1)
	// 			go func(sSource, sTarget string, b bool) {
	// 				defer wg.Done()
	// 				var pDenc *Denc
	// 				if nil != _pPublicKey {
	// 					pDenc = &Denc{Decrypt: nil != _pPrivateKey && b}
	// 				}
	// 				if err := cpd(filepath.Join(sSource, ".move"), sTarget, "", []string{""}, pDenc); nil != err {
	// 					log.Error(err)
	// 				}
	// 			}(sSource, sTarget, 0 < n)
	// 		}
	// 		if nil != _pPublicKey && nil == _pPrivateKey {
	// 			sEncrypt := filepath.Join(sSource, "..", ".encrypt")
	// 			if exists(sEncrypt) {
	// 				sEncrypted := filepath.Join(sEncrypt, ".encrypted")
	// 				if !exists(sEncrypted) {
	// 					log.Fatal(errors.New("folder '" + sEncrypted + "' does not exist!"))
	// 				}
	// 				sDone := filepath.Join(sEncrypt, ".done")
	// 				if !exists(sDone) {
	// 					sDone = ""
	// 				}
	// 				bFound = true
	// 				wg.Add(1)
	// 				go func(sEncrypt, sEncrypted, sDone string) {
	// 					defer wg.Done()
	// 					if 1 > len(sDone) {
	// 						if err := cpd(sEncrypt, sEncrypted, "", []string{".encrypted"}, new(Denc)); nil != err {
	// 							log.Error(err)
	// 						}
	// 					} else {
	// 						if err := cpd(sEncrypt, sEncrypted, sDone, []string{".done", ".encrypted"}, new(Denc)); nil != err {
	// 							log.Error(err)
	// 						}
	// 					}
	// 				}(sEncrypt, sEncrypted, sDone)
	// 			}
	// 		}
	// 	} else {
	// 		log.Warning(errors.New("folder '" + sSource + "' does not exist!"))
	// 	}
	// 	sSource = filepath.Join(_sRemoteFolder, ".aosync", _sLocalInstance)
	// 	sTarget = _sLocalFolder
	// }
	// if !bFound {
	// 	log.Fatal(errors.New("there are no any .copy, .move or .encrypt subfolders. nothing to do. bye"))
	// }
	// wg.Wait()
	// log.Notice("********* STOP")
	// t.Sleep(t.Second)
}

func cpd(sSource, sTarget string, bCopy bool, pDenc *Denc, pExist *FilesTaskExist) (err error) {
	aEntries, err := ioutil.ReadDir(sSource)
	if nil != err {
		log.Notice(sSource + ":" + sTarget)
		debug.PrintStack()
		return
	}
	var oFileInfo os.FileInfo
	for _, oEntry := range aEntries {
		sSourceEntry := oEntry.Name()

		if "." == sSourceEntry || ".." == sSourceEntry {
			continue
		}
		if _bControlStop {
			log.Warning(errors.New("stopped before " + sSourceEntry))
			return nil
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
			if err = cpd(sSourceEntry, sTargetEntry, bCopy, pDenc, pExist); nil != err {
				return
			}
			log.Error(os.Remove(sSourceEntry))
		case os.ModeSymlink:
			log.Notice("symlink ignored:" + sSourceEntry)
		default:
			log.Error(cpf(sSourceEntry, sTargetEntry, bCopy, pDenc, pExist))
			// pDENC := pDenc
			// if nil != pDENC {
			// 	bAose := s.HasSuffix(s.ToLower(sSourceEntry), ".aose")
			// 	if (pDENC.Decrypt && !bAose) || (!pDENC.Decrypt && bAose) {
			// 		pDENC = nil
			// 	} else if !pDENC.Decrypt {
			// 		sTargetEntry += ".aose"
			// 	} else if s.HasSuffix(s.ToLower(sTargetEntry), ".aose") {
			// 		sTargetEntry = sTargetEntry[:len(sTargetEntry)-5]
			// 	}
			// }

			// if !exists(sTargetEntry) {
			// 	var pSource, pTarget *os.File
			// 	if pSource, err = os.Open(sSourceEntry); nil != err {
			// 		return
			// 	}
			// 	defer pSource.Close()

			// 	if pTarget, err = os.Create(sTargetEntry); nil != err {
			// 		return
			// 	}
			// 	defer pTarget.Close()
			// 	sPrecision := "0f"
			// 	nSize := float64(oFileInfo.Size())
			// 	if 1024 < nSize {
			// 		sPrecision = "2f KB"
			// 		nSize /= 1024
			// 		if 1024 < nSize {
			// 			sPrecision = "2f MB"
			// 			nSize /= 1024
			// 			if 1024 < nSize {
			// 				sPrecision = "2f GB"
			// 				nSize /= 1024
			// 			}
			// 		}
			// 	}
			// 	log.Notice(fmt.Sprintf(" ***** start copy (%."+sPrecision+"): %s => %s", nSize, sSourceEntry, sTargetEntry))
			// 	if err = copy(pTarget, pSource, pDENC); nil != err {
			// 		return
			// 	}
			// 	log.Error(pSource.Close())
			// 	if !bCopy {
			// 		if err = os.Remove(sSourceEntry); nil != err {
			// 			log.Error(err)
			// 			t.Sleep(t.Second)
			// 			log.Error(os.Remove(sSourceEntry))
			// 		}
			// 	}
			// } else {
			// 	log.Notice("'" + sTargetEntry + "' exists")
			// }
			// if bCopy {
			// 	if err = os.Rename(sSourceEntry, sDoneEntry); nil != err {
			// 		log.Error(err)
			// 		t.Sleep(t.Second)
			// 		log.Error(os.Rename(sSourceEntry, sDoneEntry))
			// 	}
			// }
		}
	}
	return nil
}

func cpf(sSource, sTarget string, bCopy bool, pDenc *Denc, pExist *FilesTaskExist) (err error) {
	if _bControlStop {
		log.Warning(errors.New("stopped before " + sSource))
		return nil
	}
	var oSource, oTarget os.FileInfo
	//log.Debug("cpf.1")
	if oSource, err = os.Stat(sSource); nil != err {
		//log.Debug("cpf.2")
		return
	}
	//log.Debug("cpf.3")
	if oSource.IsDir() || os.ModeSymlink == (oSource.Mode()&os.ModeType) {
		return errors.New(sSource + " should be a regular file and not a folder or a symlink")
	}
	//log.Debug("cpf.4")
	if oTarget, err = os.Stat(sTarget); nil == err {
		if oTarget.IsDir() {
			sTarget = filepath.Join(sTarget, oSource.Name())
			oTarget, _ = os.Stat(sTarget)
		}
	}
	//log.Debug("cpf.5")

	var pFileDENC *Denc
	if nil != pDenc {
		//log.Debug("cpf.6")
		pFileDENC = &Denc{pDenc.Cipher, pDenc.Decrypt}
		bAose := s.HasSuffix(s.ToLower(sSource), ".aose")
		if nil != pFileDENC.Decrypt {
			if *pFileDENC.Decrypt && !bAose {
				log.Warning(errors.New("going to decrypt " + sSource))
			} else if !*pFileDENC.Decrypt && bAose {
				log.Warning(errors.New("going to encrypt " + sSource))
			}
		} else {
			pFileDENC.Decrypt = &bAose
		}

		if !*pFileDENC.Decrypt {
			sTarget += ".aose"
		} else if s.HasSuffix(s.ToLower(sTarget), ".aose") {
			sTarget = sTarget[:len(sTarget)-5]
		}
	}
	//log.Debug("cpf.7")
	bExists := exists(sTarget)
	if !bExists || nil != pExist {
		//log.Debug("cpf.8")
		var pSource, pTarget *os.File
		if pSource, err = os.Open(sSource); nil != err {
			return
		}
		defer pSource.Close()
		//log.Debug("cpf.8.1")

		nFlags := os.O_WRONLY
		if bExists {
			//log.Debug("cpf.8.2")
			switch *pExist {
			case FilesTaskExistSkip:
				log.Notice("file exists:" + sTarget + ". skipped!")
				return
			case FilesTaskExistOverwrite:
				nFlags |= os.O_CREATE | os.O_TRUNC
			case FilesTaskExistResume:
				if nil != pFileDENC {
					log.Warning(errors.New("file append with cypher is not implemented yet. falling back to overwrite " + sTarget + " by " + sSource))
					nFlags |= os.O_CREATE | os.O_TRUNC
				} else {
					nFlags |= os.O_APPEND
					pSource.Seek(oTarget.Size(), 0)
				}
			}
		} else {
			nFlags |= os.O_CREATE
		}
		//log.Debug("cpf.8.3")
		if pTarget, err = os.OpenFile(sTarget, nFlags, 0755); nil != err {
			return
		}
		defer pTarget.Close()
		//log.Debug("cpf.8.4")

		sPrecision := "0f"
		nSize := float64(oSource.Size())
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
		log.Notice(fmt.Sprintf(" ***** start copy (%."+sPrecision+"): %s => %s", nSize, sSource, sTarget))
		if err = copy(pTarget, pSource, pFileDENC, bExists && nil != pExist && FilesTaskExistResume == *pExist); nil != err {
			return
		}
		log.Error(pSource.Close())
		if !bCopy {
			if err = os.Remove(sSource); nil != err {
				log.Error(err)
				t.Sleep(t.Second)
				if nil == log.Error(os.Remove(sSource)) {
					log.Notice(sSource + " has been removed successfuly!")
				}
			}
		}
	} else {
		//log.Debug("cpf.9")
		log.Error(errors.New("'" + sTarget + "' exists"))
	}
	return
}

func copy(pTarget, pSource *os.File, pDenc *Denc, bResume bool) (err error) {
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
		if nil != pDenc.Decrypt && !*pDenc.Decrypt {
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

func controls() {
	sTasks := filepath.Join(_pPreferences.Folder, "control")
	aFiles, err := ioutil.ReadDir(sTasks)
	if nil != err {
		log.Fatal(err)
	}
	sQueue := filepath.Join(sTasks, ".queue")
	for _, oFI := range aFiles {
		if oFI.IsDir() {
			continue
		}
		sSource := filepath.Join(sTasks, oFI.Name())
		aBytes, err := ioutil.ReadFile(sSource)
		if nil != log.Error(err) {
			continue
		}
		pTask := new(ControlTask)
		if nil != log.Error(json.Unmarshal(aBytes, pTask)) {
			continue
		}
		sTarget := filepath.Join(sQueue, uuid.New().String()+".json")
		if nil == log.Error(os.Rename(sSource, sTarget)) {
			log.Notice("CONTROL TASK NEW (" + string(pTask.Type) + "):" + sSource + " => " + sTarget)
		}
	}
	aFiles, err = ioutil.ReadDir(sQueue)
	if nil != err {
		log.Fatal(err)
	}
	sDone := filepath.Join(sTasks, ".done")
	sUpdate, sFatal := "", ""
	for _, oFI := range aFiles {
		if oFI.IsDir() {
			continue
		}
		sSource := filepath.Join(sQueue, oFI.Name())
		aBytes, err := ioutil.ReadFile(sSource)
		if nil != log.Error(err) {
			continue
		}
		pTask := new(ControlTask)
		if nil != log.Error(json.Unmarshal(aBytes, pTask)) {
			continue
		}
		sTarget := filepath.Join(sDone, oFI.Name())
		switch pTask.Type {
		case ControlTaskTypeUpdate:
			sUpdate = *pTask.Data
		case ControlTaskTypeKill:
			sFatal = filepath.Join(sFatal, oFI.Name())
			continue
		case ControlTaskTypeStop:
			_bControlStop = true
			continue
		case ControlTaskTypeRestart:
			_bControlStop = true
		case ControlTaskTypeReload:
			tasks()
		}
		log.Error(os.Rename(sSource, sTarget))
	}

	if 0 < len(sUpdate) && nil == update(sUpdate) {
		log.Fatal(errors.New("exiting"))
	}
	if 0 < len(sFatal) {
		log.Fatal(errors.New("kill control task found:" + sFatal))
	}
}

func tasks() {
	sTasks := filepath.Join(_pPreferences.Folder, "tasks")
	aFiles, err := ioutil.ReadDir(sTasks)
	if nil != err {
		log.Fatal(err)
	}
	sQueue := filepath.Join(sTasks, ".queue")
	for _, oFI := range aFiles {
		if oFI.IsDir() {
			continue
		}
		sSource := filepath.Join(sTasks, oFI.Name())
		aBytes, err := ioutil.ReadFile(sSource)
		if nil != log.Error(err) {
			continue
		}
		pTask := &FilesTask{Priority: FilesTaskPriorityNormal}
		sTarget := uuid.New().String() + ".json"
		if nil != log.Error(json.Unmarshal(aBytes, pTask)) {
			log.Fatal(os.MkdirAll(filepath.Join(sTasks, ".failed"), 0777))
			sTarget = filepath.Join(sTasks, ".failed", sTarget)
			if nil == log.Error(os.Rename(sSource, sTarget)) {
				log.Notice("FILES TASK FAILED: " + sSource + " => " + sTarget)
			}
			continue
		}
		sTarget = filepath.Join(sQueue, sTarget)
		if nil == log.Error(os.Rename(sSource, sTarget)) {
			log.Notice("FILES TASK NEW (" + string(pTask.Type) + ":" + string(pTask.Source) + " => " + string(pTask.Target) + "):" + sSource + " => " + sTarget)
			pTask.Initial = sSource
			if aBytes, err = json.Marshal(pTask); nil == log.Error(err) {
				log.Error(os.WriteFile(sTarget, aBytes, 0777))
			}
		}
	}
	aFiles, err = ioutil.ReadDir(sQueue)
	if nil != err {
		log.Fatal(err)
	}
	for _, oFI := range aFiles {
		if oFI.IsDir() {
			continue
		}
		sSource := filepath.Join(sQueue, oFI.Name())
		aBytes, err := ioutil.ReadFile(sSource)
		if nil != log.Error(err) {
			continue
		}
		pTask := &FilesTask{Priority: FilesTaskPriorityNormal}
		if nil != log.Error(json.Unmarshal(aBytes, pTask)) {
			continue
		}
		if !_mFilesTasks[sSource] && TASKS_MAX_BY_PRIORITY > len(_aFilesTasks[pTask.Priority]) {
			pTask.File = sSource
			_aFilesTasks[pTask.Priority] <- pTask
			_mFilesTasks[sSource] = true
			log.Notice("FILES TASK QUEUED (" + string(pTask.Type) + "@" + ToStr(pTask.Priority) + " as " + string(pTask.Source) + " => " + string(pTask.Target) + ")")
		}
		//switch pTask.Type {
		//case FilesTaskTypeCopy:
		//case FilesTaskTypeMove:
		//case FilesTaskTypeEncode:
		//case FilesTaskTypeDecode:
		//case FilesTaskTypeDate:
		//}
	}

}
func update(sUpdate string) (err error) {
	log.Notice("**** UPDATE STARTED:" + sUpdate)
	if !exists(sUpdate) {
		return errors.New("NO UPDATE FILE FOUND")
	}
	sBackup := _sAOSync + ".bkp"
	if exists(sBackup) {
		log.Notice("backup exists:" + sBackup + ". removing")
		log.Error(os.Remove(sBackup))
	}
	log.Notice("creating backup:" + sBackup)
	errBackup := log.Error(os.Rename(_sAOSync, sBackup))
	log.Notice("now updating")
	var aBytes []byte
	if aBytes, err = ioutil.ReadFile(sUpdate); nil == err {
		if nil == log.Error(ioutil.WriteFile(_sAOSync, aBytes, 0777)) {
			log.Notice("UPDATED! exiting")
			t.Sleep(t.Second)
			return
		}
	}
	log.Notice("UPDATE FAILED!")
	if nil == errBackup {
		log.Notice("trying to restore from backup")
		log.Error(os.Rename(sBackup, _sAOSync))
	}
	return
}
