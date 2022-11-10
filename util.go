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
	gl_ch = make(chan int, 1)
)

func initLock() {
	gl_ch <- 1
}

func cleanupLock() {
	close(gl_ch)
}

func globalLock() {
	<-gl_ch
}

func globalUnlock() {
	gl_ch <- 1
}

func isWindows() bool {
	return runtime.GOOS == "windows"
}

func getPathSeparator() string {
	return string(os.PathSeparator)
}

type fileType int

const (
	DIR fileType = iota
	REG
	DEVICE
	SYMLINK
	UNSUPPORTED
	INVALID
)

func getRawFileType(f string) (fileType, error) {
	info, err := os.Lstat(f)
	if err != nil {
		return INVALID, err
	}

	return getModeType(info.Mode())
}

func getFileType(f string) (fileType, error) {
	info, err := os.Stat(f)
	if err != nil {
		return INVALID, err
	}

	return getModeType(info.Mode())
}

func getModeType(m fs.FileMode) (fileType, error) {
	if m.IsDir() {
		return DIR, nil
	} else if m.IsRegular() {
		return REG, nil
	} else if m&fs.ModeDevice != 0 {
		// XXX assuming blk on Linux, chr on *BSD
		return DEVICE, nil
	} else if m&fs.ModeSymlink != 0 {
		return SYMLINK, nil
	}

	return UNSUPPORTED, nil
}

func pathExists(f string) (bool, error) {
	if _, err := os.Stat(f); err == nil {
		return true, nil
	} else {
		return false, err
	}
}

func isDotPath(f string) bool {
	// XXX really ?
	return strings.HasPrefix(path.Base(f), ".") || strings.Contains(f, "/.")
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
	if f != "" {
		s = fmt.Sprintf("%s has %s file type %d", f, how, t)
	} else {
		s = fmt.Sprintf("%s file type %d", how, t)
	}
	panic(s)
}
