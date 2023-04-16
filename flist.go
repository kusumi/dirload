package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

func initFlist(input string, ignore_dot bool) ([]string, error) {
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
			if ignore_dot {
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

func loadFlistFile(flist_file string) ([]string, error) {
	file, err := os.Open(flist_file)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var fl []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fl = append(fl, scanner.Text())
	}

	return fl, scanner.Err()
}

func createFlistFile(input []string, flist_file string, ignore_dot bool) error {
	if _, err := os.Stat(flist_file); err == nil {
		return fmt.Errorf("%s exists", flist_file)
	}

	var fl []string
	for _, f := range input {
		if l, err := initFlist(f, ignore_dot); err != nil {
			return err
		} else {
			fmt.Println(len(l), "files scanned from", f)
			fl = append(fl, l...)
		}
	}
	sort.Strings(fl)

	file, err := os.OpenFile(flist_file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, s := range fl {
		assert(filepath.IsAbs(s))
		if _, err := w.WriteString(s + "\n"); err != nil {
			return err
		}
	}
	w.Flush()

	return nil
}
