package preferences

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
	s "strings"

	"github.com/ratsil/go-helpers/email"
)

//Preferences .
type Preferences struct {
	Folder string `json:"folder"` //path to the control folder. optional, default is current folder
	Denc   *struct {
		Keys *struct {
			Public  *string `json:"public"`  //path to the public key. optional
			Private *string `json:"private"` //path to the private key. optional but requires the public
		} `json:"keys,omitempty"`
	} `json:"denc"`
	Buffers *struct {
		Size uint `json:"size"` //each buffer size in KB
		Qty  byte `json:"qty"`  //qty of buffers
	} `json:"buffers"`
	Log *struct {
		Folder string `json:"folder"`
		Prefix string `json:"prefix"`
		Period uint   `json:"period"`
	} `json:"log"`
	SMTP *struct {
		Client *email.Mailer `json:"client"`
		Server *struct {
			Host  string `json:"host"`
			Port  string `json:"port"`
			SSL   bool   `json:"ssl"`
			Users []*struct {
				Name     string `json:"name"`
				Password string `json:"password"`
			} `json:"users"`
		} `json:"server,omitempty"`
	} `json:"smtp,omitempty"`
}

func PreferencesRead(sFile string) (pRetVal *Preferences, err error) {
	aBytes, err := ioutil.ReadFile(sFile)
	if nil != err {
		if s.Contains(os.Args[0], "debug") {
			aBytes, err = ioutil.ReadFile("../../bin/preferences.json")
		} else {
			aBytes, err = ioutil.ReadFile(path.Join(path.Dir(os.Args[0]), "preferences.json"))
		}
		if nil != err {
			return nil, errors.New("cannot read preferences")
		}
	}
	pRetVal = new(Preferences)
	if err = json.Unmarshal(aBytes, pRetVal); nil != err {
		return nil, errors.New("preferences parse error")
	}

	if 1 > len(pRetVal.Folder) {
		pRetVal.Folder = "./"
	}

	if 1 > len(pRetVal.Log.Folder) {
		pRetVal.Log.Folder = path.Join(pRetVal.Folder, "logs")
	}
	if 1 > len(pRetVal.Log.Prefix) {
		pRetVal.Log.Prefix = "aosync"
	}
	if 1 > pRetVal.Log.Period {
		pRetVal.Log.Period = 300
	}
	return
}
