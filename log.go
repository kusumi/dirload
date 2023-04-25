package main

import (
	"fmt"
	"log"
	"os"
	"os/user"
	"path"
	"strings"
)

var (
	linit bool = false
	lfp   *os.File
)

func initLog(name string) error {
	if !optDebug {
		return nil
	}

	u, err := user.Current()
	if err != nil {
		return err
	}

	f := path.Join(u.HomeDir, "."+name+".log")
	lfp, err := os.OpenFile(f, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(lfp)

	linit = true
	dbg(strings.Repeat("=", 20))
	dbg(lfp.Name())
	if optVerbose {
		fmt.Println(lfp.Name())
	}

	return nil
}

func cleanupLog() {
	if !optDebug {
		return
	}

	lfp.Close()
	linit = false
}

func dbg(args ...interface{}) {
	if !optDebug {
		return
	}

	assert(linit)
	globalLock()
	log.Println(args...)
	globalUnlock()
}

func dbgf(f string, args ...interface{}) {
	if !optDebug {
		return
	}

	assert(linit)
	globalLock()
	log.Printf(f, args...)
	globalUnlock()
}
