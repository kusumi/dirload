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
	lfd   *os.File
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
	lfd, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.SetOutput(lfd)

	linit = true
	dbg(strings.Repeat("=", 20))
	dbg(lfd.Name())
	if optVerbose {
		fmt.Println(lfd.Name())
	}

	return nil
}

func cleanupLog() {
	if !optDebug {
		return
	}

	lfd.Close()
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
