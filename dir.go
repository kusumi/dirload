package main

import (
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
)

var (
	readBuffer [][]byte
)

func initReadBuffer(n int, bufsiz int) {
	assert(n > 0)
	assert(bufsiz > 0)
	readBuffer = make([][]byte, n)
	assert(len(readBuffer) == n)

	for i := 0; i < len(readBuffer); i++ {
		readBuffer[i] = make([]byte, bufsiz)
	}
}

func assertFilePath(f string) {
	// must always handle file as abs
	assert(filepath.IsAbs(f))

	// file must not end with "/"
	assert(!strings.HasSuffix(f, "/"))
}

func readEntry(gid int, f string, d fs.DirEntry, err error) error {
	if err != nil {
		return err
	}

	assertFilePath(f)
	t, err := getRawFileType(f)
	if err != nil {
		return err
	}
	// stats by dirwalk itself are not counted
	incNumStat(gid)

	// ignore . entries if specified
	if optIgnoreDot {
		// XXX want retval to ignore children for .directory
		if t != DIR {
			if isDotPath(f) {
				return nil
			}
		}
	}

	// beyond this is for read or readlink
	if optStatOnly {
		return nil
	}

	// find target if symlink
	switch t {
	case SYMLINK:
		l := f
		f, err = os.Readlink(f)
		if err != nil {
			return err
		}
		addNumReadBytes(gid, len(f))
		if !filepath.IsAbs(f) {
			f = filepath.Join(filepath.Dir(l), f)
			assert(filepath.IsAbs(f))
		}
		t, err = getFileType(f)
		if err != nil {
			return err
		}
		incNumStat(gid)      // count twice for symlink
		assert(t != SYMLINK) // symlink chains resolved
		if optLstat {
			return nil
		}
	}

	switch t {
	case DIR:
		return nil
	case REG:
		if err := readFile(gid, f); err != nil {
			return err
		}
		return nil
	case DEVICE:
		return nil
	case UNSUPPORTED:
		return nil
	case INVALID:
		panicFileType(f, "invalid", t)
	default:
		panicFileType(f, "unknown", t)
	}

	assert(false)
	return nil
}

func readFile(gid int, f string) error {
	fp, err := os.Open(f)
	if err != nil {
		return err
	}
	defer fp.Close()

	b := readBuffer[gid]
	resid := optReadSize
	if resid == 0 {
		resid = rand.Intn(len(b)) + 1
		assert(resid > 0)
		assert(resid <= len(b))
	}

	for {
		// cut slice size if > positive residual
		if resid > 0 {
			if len(b) > resid {
				b = b[:resid]
			}
		}

		siz, err := fp.Read(b)
		incNumRead(gid) // count only when siz > 0 ?
		if err == io.EOF {
			addNumReadBytes(gid, siz)
			break
		} else if err != nil {
			return err
		}
		addNumReadBytes(gid, siz)

		// end if positive residual becomes <= 0
		if resid > 0 {
			resid -= siz
			if resid <= 0 {
				if optDebug {
					assert(resid == 0)
				}
				break
			}
		}
	}

	return nil
}

func writeEntry(gid int, f string, d fs.DirEntry, err error) error {
	if err != nil {
		return err
	}

	assertFilePath(f)
	t, err := getRawFileType(f)
	if err != nil {
		return err
	}
	// stats by dirwalk itself are not counted
	incNumStat(gid)

	switch t {
	case DIR:
		if err := writeFile(gid, f); err != nil {
			return err
		}
		return nil
	case REG:
		if err := writeFile(gid, filepath.Dir(f)); err != nil {
			return err
		}
		return nil
	case DEVICE:
		return nil
	case SYMLINK:
		return nil
	case UNSUPPORTED:
		return nil
	case INVALID:
		panicFileType(f, "invalid", t)
	default:
		panicFileType(f, "unknown", t)
	}

	assert(false)
	return nil
}

func writeFile(gid int, f string) error {
	if t, err := getFileType(f); err != nil {
		return err
	} else {
		assert(t == DIR)
	}

	switch rand.Intn(2) {
	case 0: // mkdir
		d := filepath.Join(f, "dirload_mkdir")
		if err := os.Mkdir(d, 0644); err != nil {
			incNumWrite(gid)
			return nil // expected
		} else {
			return fmt.Errorf("mkdir %s expected to fail", d)
		}
	case 1: // creat
		f := filepath.Join(f, "dirload_creat")
		if _, err := os.Create(f); err != nil {
			incNumWrite(gid)
			return nil // expected
		} else {
			return fmt.Errorf("creat %s expected to fail", f)
		}
	default:
		// XXX rmdir / unlink ???
		panic("invalid")
	}
}
