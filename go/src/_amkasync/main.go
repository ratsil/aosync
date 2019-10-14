package main

import (
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	s "strings"
)

var (
	_aFolders []string
)

func main() {
	pFolders := flag.String("folders", "", "")
	flag.Parse()
	bFound := false
	if nil != pFolders {
		_aFolders = s.Split(s.Trim(*pFolders, "'\"`"), ",")
		if 2 == len(_aFolders) {
			bFound = true
		}
	}
	if !bFound {
		log.Fatal("you should specify two target existed folders! like: amkasync -folders='/path/to/first/folder,/path/to/second/folder'")
	}
	var err error
	bFound = false
	sTarget := _aFolders[1]
	for _, sSource := range _aFolders {
		if !exists(sSource) {
			log.Fatal("folder '" + sSource + "' does not exists!")
		}
		if exists(sSource + "/.copy") {
			if !exists(sSource + "/.copy/.done") {
				log.Fatal("folder '" + sSource + "/.copy/.done' does not exists!")
			}
			bFound = true
			err = cp(sSource, sTarget)
			if nil != err {
				log.Println(err)
			}
		}
		if exists(sSource + "/.move") {
			bFound = true
			if err = mv(sSource, sTarget); nil != err {
				log.Println(err)
			}
		}
		sTarget = sSource
	}
	if !bFound {
		log.Fatal("there are no any .copy or .move subfolders. nothing to do. bye!")
	}
}
func cp(sSource, sTarget string) error {
	return cpd(sSource+"/.copy", sTarget, sSource+"/.copy/.done", []string{".done"})
}
func cpd(sSource, sTarget, sDone string, aExcludes []string) (err error) {
	aEntries, err := ioutil.ReadDir(sSource)
	if nil != err {
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
			if err = cpd(sSourceEntry, sTargetEntry, sDoneEntry, nil); nil != err {
				return
			}
			if err = os.Remove(sSourceEntry); nil != err {
				return
			}
		case os.ModeSymlink:
			log.Println("symlink ignored:" + sSourceEntry)
		default:
			if err = cpf(sSourceEntry, sTargetEntry); nil != err {
				return
			}
			if err = os.Rename(sSourceEntry, sDoneEntry); nil != err {
				return
			}
		}
	}
	return nil
}
func cpf(sSource, sTarget string) error {
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
	_, err = io.Copy(pTarget, pSource)
	return err
}

func mv(sSource, sTarget string) error {
	return mvd(sSource+"/.move", sTarget)
}
func mvd(sSource, sTarget string) (err error) {
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
			if err = mvd(sSourceEntry, sTargetEntry); nil != err {
				return
			}
			if err = os.Remove(sSourceEntry); nil != err {
				log.Print(err.Error())
			}
		case os.ModeSymlink:
			log.Println("symlink ignored:" + sSourceEntry)
		default:
			if err = mvf(sSourceEntry, sTargetEntry); nil != err {
				return
			}
		}
	}
	return nil
}
func mvf(sSource, sTarget string) (err error) {
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
	if _, err = io.Copy(pTarget, pSource); nil != err {
		return
	}
	if err = pSource.Close(); nil != err {
		return
	}
	return os.Remove(sSource)
}

func exists(sPath string) bool {
	if _, err := os.Stat(sPath); os.IsNotExist(err) {
		return false
	}
	return true
}
