package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

func initFlist(input string, ignoreDot bool) ([]string, error) {
	var l []string
	if err := filepath.WalkDir(input,
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
			if ignoreDot {
				// XXX want retval to ignore children for .directory
				if t != typeDir {
					if isDotPath(f) {
						return nil
					}
				}
			}

			switch t {
			case typeDir:
				return nil
			case typeReg:
				l = append(l, f)
			case typeDevice:
				return nil
			case typeSymlink:
				l = append(l, f)
			case typeUnsupported:
				return nil
			case typeInvalid:
				panicFileType(f, "invalid", t)
			default:
				panicFileType(f, "unknown", t)
			}
			return nil
		}); err != nil {
		return nil, err
	}
	return l, nil
}

func loadFlistFile(flistFile string) ([]string, error) {
	fp, err := os.Open(flistFile)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	var fl []string
	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		fl = append(fl, scanner.Text())
	}

	return fl, scanner.Err()
}

func createFlistFile(input []string, flistFile string, ignoreDot bool, force bool) error {
	if _, err := os.Stat(flistFile); err == nil {
		if force {
			if err := os.Remove(flistFile); err != nil {
				return err
			} else {
				fmt.Println("Removed", flistFile)
			}
		} else {
			return fmt.Errorf("%s exists", flistFile)
		}
	}

	var fl []string
	for _, f := range input {
		if l, err := initFlist(f, ignoreDot); err != nil {
			return err
		} else {
			fmt.Println(len(l), "files scanned from", f)
			fl = append(fl, l...)
		}
	}
	sort.Strings(fl)

	fp, err := os.OpenFile(flistFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fp.Close()

	w := bufio.NewWriter(fp)
	for _, s := range fl {
		assert(filepath.IsAbs(s))
		if _, err := w.WriteString(s + "\n"); err != nil {
			return err
		}
	}
	w.Flush()

	return nil
}
