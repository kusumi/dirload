package main

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

var (
	readBuffer [][]byte
)

func initDir(n int, bufsiz int) {
	assert(n > 0)
	assert(bufsiz > 0)
	readBuffer = make([][]byte, n)
	assert(len(readBuffer) == n)

	for i := 0; i < len(readBuffer); i++ {
		readBuffer[i] = make([]byte, bufsiz)
	}
}

func initFileList(input string) ([]string, error) {
	var l []string
	err := filepath.WalkDir(input,
		func(f string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			assertFilePath(f)
			t, err := getRawFileType(f)
			if err != nil {
				return err
			}

			// ignore . entries if specified
			if optIgnoreDot {
				// XXX want retval to ignore children for .directory
				if t != DIR {
					if isDotPath(f) {
						return nil
					}
				}
			}

			switch t {
			case DIR:
				return nil
			case REG:
				l = append(l, f)
			case DEVICE:
				return nil
			case SYMLINK:
				l = append(l, f)
			case UNSUPPORTED:
				return nil
			case INVALID:
				panicFileType(f, "invalid", t)
			default:
				panicFileType(f, "unknown", t)
			}
			return nil
		})
	return l, err
}

func handleEntry(gid int, f string, d fs.DirEntry, err error) error {
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

	for {
		siz, err := fp.Read(readBuffer[gid])
		incNumRead(gid) // count only when siz > 0 ?
		if err == io.EOF {
			addNumReadBytes(gid, siz)
			break
		} else if err != nil {
			return err
		}
		addNumReadBytes(gid, siz)
	}

	return nil
}

func assertFilePath(f string) {
	// must always handle file as abs
	assert(filepath.IsAbs(f))

	// file must not end with "/"
	assert(!strings.HasSuffix(f, "/"))
}
