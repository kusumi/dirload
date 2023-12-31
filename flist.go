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
		}); err != nil {
		return nil, err
	}
	return l, nil
}

func loadFlistFile(flist_file string) ([]string, error) {
	fp, err := os.Open(flist_file)
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

func createFlistFile(input []string, flist_file string, ignore_dot bool, force bool) error {
	if _, err := os.Stat(flist_file); err == nil {
		if force {
			if err := os.Remove(flist_file); err != nil {
				return err
			} else {
				fmt.Println("Removed", flist_file)
			}
		} else {
			return fmt.Errorf("%s exists", flist_file)
		}
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

	fp, err := os.OpenFile(flist_file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
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
