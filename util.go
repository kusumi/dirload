package main

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"runtime"
	"strings"
)

var (
	globalCh = make(chan int, 1)
)

func initLock() {
	globalCh <- 1
}

func cleanupLock() {
	close(globalCh)
}

func globalLock() {
	<-globalCh
}

func globalUnlock() {
	globalCh <- 1
}

func isLinux() bool {
	return runtime.GOOS == "linux"
}

func isWindows() bool {
	return runtime.GOOS == "windows"
}

func getPathSeparator() string {
	return string(os.PathSeparator)
}

type fileType int

const (
	typeDir fileType = iota
	typeReg
	typeDevice
	typeSymlink
	typeUnsupported
	typeInvalid
	typeLink // hardlink
)

func getRawFileType(f string) (fileType, error) {
	info, err := os.Lstat(f)
	if err != nil {
		return typeInvalid, err
	}

	return getModeType(info.Mode()), nil
}

func getFileType(f string) (fileType, error) {
	info, err := os.Stat(f)
	if err != nil {
		return typeInvalid, err
	}

	return getModeType(info.Mode()), nil
}

func getModeType(m fs.FileMode) fileType {
	if m.IsDir() {
		return typeDir
	} else if m.IsRegular() {
		return typeReg
	} else if m&fs.ModeDevice != 0 {
		// XXX assuming blk on Linux, chr on *BSD
		return typeDevice
	} else if m&fs.ModeSymlink != 0 {
		return typeSymlink
	}

	return typeUnsupported
}

func pathExists(f string) (bool, error) {
	if _, err := os.Lstat(f); err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func isDotPath(f string) bool {
	// XXX really ?
	return strings.HasPrefix(path.Base(f), ".") || strings.Contains(f, "/.")
}

func isDirWritable(f string) (bool, error) {
	if t, err := getRawFileType(f); err != nil {
		return false, err
	} else if t != typeDir {
		return false, fmt.Errorf("%s not directory", f)
	}

	if dir, err := os.MkdirTemp(f, "dirload_write_test_"); err != nil {
		return false, nil // assume readonly
	} else if err := os.Remove(dir); err != nil {
		return false, err
	} else {
		return true, nil // read+write
	}
}

func removeDupString(input []string) []string {
	var l []string
	for _, a := range input {
		exists := false
		for _, b := range l {
			if a == b {
				exists = true
			}
		}
		if !exists {
			l = append(l, a)
		}
	}
	return l
}

func assert(c bool) {
	kassert(c, "Assert failed")
}

func kassert(c bool, err interface{}) {
	if !c {
		panic(err)
	}
}

func panicFileType(f string, how string, t fileType) {
	var s string
	if len(f) != 0 {
		s = fmt.Sprintf("%s has %s file type %d", f, how, t)
	} else {
		s = fmt.Sprintf("%s file type %d", how, t)
	}
	panic(s)
}
