package main

import (
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var (
	maxBufferSize     uint
	readBuffer        [][]byte
	writeBuffer       [][]byte
	randomWriteData   []byte
	writePaths        [][]string
	writePathsCounter []uint64
	writePathsTs      string
	writePathsPrefix  string
	writePathsType    []fileType
)

func init() {
	maxBufferSize = 128 * 1024
	writePathsPrefix = "dirload"
}

func initReadBuffer(n uint, bufsiz uint) {
	assert(bufsiz > 0)
	readBuffer = make([][]byte, n)
	assert(uint(len(readBuffer)) == n)

	for i := 0; i < len(readBuffer); i++ {
		readBuffer[i] = make([]byte, bufsiz)
	}
}

func initWriteBuffer(n uint, bufsiz uint, random bool) {
	assert(bufsiz > 0)
	writeBuffer = make([][]byte, n)
	assert(uint(len(writeBuffer)) == n)

	for i := 0; i < len(writeBuffer); i++ {
		writeBuffer[i] = make([]byte, bufsiz)
		if i == 0 {
			for j := uint(0); j < bufsiz; j++ {
				writeBuffer[i][j] = 0x41
			}
		} else {
			copy(writeBuffer[i], writeBuffer[0])
		}
	}

	if random {
		randomWriteData = make([]byte, maxBufferSize*2) // doubled
		for i := 0; i < len(randomWriteData); i++ {
			randomWriteData[i] = byte(rand.Intn(127-32) + 32)
		}
	}
}

func initWritePaths(n uint, write_paths_type string) {
	writePaths = make([][]string, n)
	writePathsCounter = make([]uint64, n)
	assert(uint(len(writePaths)) == n)
	assert(uint(len(writePathsCounter)) == n)

	for i := 0; i < len(writePaths); i++ {
		writePaths[i] = make([]string, 0)
		writePathsCounter[i] = 0
	}

	writePathsTs = time.Now().Format("20060102150405")
	assert(len(writePathsPrefix) != 0)

	writePathsType = make([]fileType, len(write_paths_type))
	for i, x := range write_paths_type {
		var t fileType
		switch x {
		case 'd':
			t = DIR
		case 'r':
			t = REG
		case 's':
			t = SYMLINK
		case 'l':
			t = LINK
		default:
			assert(false)
		}
		writePathsType[i] = t
	}
}

func cleanupWritePaths(keep_write_paths bool) (int, error) {
	var l []string
	for i := 0; i < len(writePaths); i++ {
		l = append(l, writePaths[i]...)
	}

	num_remain := 0
	if keep_write_paths {
		num_remain += len(l)
	} else {
		if l, err := unlinkWritePaths(l, -1); err != nil {
			return -1, err
		} else {
			num_remain += len(l)
		}
	}
	return num_remain, nil
}

func unlinkWritePaths(l []string, count int) ([]string, error) {
	n := len(l) // unlink all by default
	if count > 0 {
		n = count
		if n > len(l) {
			n = len(l)
		}
	}
	fmt.Println("Unlink", n, "write paths")
	sort.Strings(l)

	for n > 0 {
		f := l[len(l)-1]
		if t, err := getRawFileType(f); err != nil {
			return l, err
		} else if t == DIR || t == REG || t == SYMLINK {
			if exists, err := pathExists(f); err != nil {
				return l, err
			} else if !exists {
				continue
			}
			if err := os.Remove(f); err != nil {
				return l, err
			}
			l = l[:len(l)-1]
			n--
		} else {
			assert(false)
		}
	}
	return l, nil
}

func assertFilePath(f string) {
	// must always handle file as abs
	assert(filepath.IsAbs(f))

	// file must not end with "/"
	assert(!strings.HasSuffix(f, "/"))
}

func readEntry(gid uint, f string) error {
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

	// beyond this is for file read
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

func readFile(gid uint, f string) error {
	fp, err := os.Open(f)
	if err != nil {
		return err
	}
	defer fp.Close()

	b := readBuffer[gidToRid(gid)]
	resid := optReadSize // negative resid means read until EOF
	if resid == 0 {
		resid = rand.Intn(len(b)) + 1
		assert(resid > 0)
		assert(resid <= len(b))
	}
	assert(resid == -1 || resid > 0)

	for {
		// cut slice size if > positive residual
		if resid > 0 {
			if len(b) > resid {
				b = b[:resid]
			}
		}

		siz, err := fp.Read(b)
		if err == io.EOF {
			incNumRead(gid)
			addNumReadBytes(gid, siz)
			break
		} else if err != nil {
			return err
		}
		incNumRead(gid)
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

func writeEntry(gid uint, f string) error {
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

	switch t {
	case DIR:
		if err := writeFile(gid, f, f); err != nil {
			return err
		}
		return nil
	case REG:
		if err := writeFile(gid, filepath.Dir(f), f); err != nil {
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

func writeFile(gid uint, d string, f string) error {
	if isWriteDone(gid) {
		return nil
	}

	// construct a write path
	wid := gidToWid(gid)
	newb := fmt.Sprintf("%s_wid%d_%s_%d",
		getWritePathsBase(), wid, writePathsTs, writePathsCounter[wid])
	writePathsCounter[wid]++
	newf := filepath.Join(d, newb)

	// create an inode
	t := writePathsType[rand.Intn(len(writePathsType))]
	if err := creatInode(f, newf, t); err != nil {
		return err
	}
	if optFsyncWritePaths {
		if err := fsyncInode(newf); err != nil {
			return err
		}
	}
	if optDirsyncWritePaths {
		if err := fsyncInode(d); err != nil {
			return err
		}
	}

	// register the write path, and return unless regular file
	writePaths[wid] = append(writePaths[wid], newf)
	if t != REG {
		incNumWrite(gid)
		return nil
	}

	// open the write path and start writing
	fp, err := os.OpenFile(newf, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fp.Close()

	b := writeBuffer[wid]
	resid := optWriteSize // negative resid means no write
	if resid < 0 {
		incNumWrite(gid)
		return nil
	} else if resid == 0 {
		resid = rand.Intn(len(b)) + 1
		assert(resid > 0)
		assert(resid <= len(b))
	}
	assert(resid > 0)

	if optTruncateWritePaths {
		if err := fp.Truncate(int64(resid)); err != nil {
			return err
		}
		incNumWrite(gid)
	} else {
		for {
			// cut slice size if > residual
			if len(b) > resid {
				b = b[:resid]
			}
			if optRandomWriteData {
				i := rand.Intn(len(randomWriteData) / 2)
				copy(b, randomWriteData[i:i+len(b)])
			}

			siz, err := fp.Write(b)
			if err != nil {
				return err
			}
			incNumWrite(gid)
			addNumWriteBytes(gid, siz)

			// end if residual becomes <= 0
			resid -= siz
			if resid <= 0 {
				if optDebug {
					assert(resid == 0)
				}
				break
			}
		}
	}

	if optFsyncWritePaths {
		if err := fp.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func creatInode(oldf string, newf string, t fileType) error {
	if t == LINK {
		if t, err := getRawFileType(oldf); err != nil {
			return err
		} else if t == REG {
			if err := os.Link(oldf, newf); err != nil {
				return err
			}
			return nil
		}
		t = DIR // create a directory instead
	}

	if t == DIR {
		if err := os.Mkdir(newf, 0644); err != nil {
			return err
		}
	} else if t == REG {
		if fp, err := os.Create(newf); err != nil {
			return err
		} else {
			defer fp.Close()
		}
	} else if t == SYMLINK {
		if err := os.Symlink(oldf, newf); err != nil {
			return err
		}
	}
	return nil
}

func fsyncInode(f string) error {
	if fp, err := os.Open(f); err != nil {
		return err
	} else {
		defer fp.Close()
		if err := fp.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func isWriteDone(gid uint) bool {
	if !isWriter(gid) {
		return false
	} else if optNumWritePaths <= 0 {
		return false
	} else {
		return len(writePaths[gidToWid(gid)]) >= optNumWritePaths
	}
}

func getWritePathsBase() string {
	return fmt.Sprintf("%s_%s", writePathsPrefix, optWritePathsBase)
}

func collectWritePaths(input []string) ([]string, error) {
	b := getWritePathsBase()
	var l []string
	for _, f := range removeDupString(input) {
		if err := filepath.WalkDir(f,
			func(f string, d fs.DirEntry, err error) error {
				assertFilePath(f)
				if t, err := getRawFileType(f); err != nil {
					return err
				} else if t == DIR || t == REG || t == SYMLINK {
					if strings.HasPrefix(path.Base(f), b) {
						l = append(l, f)
					}
				}
				return nil
			}); err != nil {
			return nil, err
		}
	}
	return l, nil
}
