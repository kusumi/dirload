package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	version               [3]int = [3]int{0, 4, 8}
	optNumSet             uint
	optNumReader          uint
	optNumWriter          uint
	optNumRepeat          int
	optTimeMinute         uint
	optTimeSecond         uint
	optMonitorIntMinute   uint
	optMonitorIntSecond   uint
	optStatOnly           bool
	optIgnoreDot          bool
	optFollowSymlink      bool
	optReadBufferSize     uint
	optReadSize           int
	optWriteBufferSize    uint
	optWriteSize          int
	optRandomWriteData    bool
	optNumWritePaths      int
	optTruncateWritePaths bool
	optFsyncWritePaths    bool
	optDirsyncWritePaths  bool
	optKeepWritePaths     bool
	optCleanWritePaths    bool
	optWritePathsBase     string
	optWritePathsType     []fileType
	optPathIter           uint
	optFlistFile          string
	optFlistFileCreate    bool
	optForce              bool
	optVerbose            bool
	optDebug              bool
)

func getVersionString() string {
	return fmt.Sprintf("%d.%d.%d", version[0], version[1], version[2])
}

func printVersion() {
	fmt.Println(getVersionString())
}

func usage(progname string) {
	fmt.Fprintln(os.Stderr, "usage: "+progname+": [<options>] <paths>")
	flag.PrintDefaults()
}

func main() {
	progname := path.Base(os.Args[0])

	optNumSetAddr := flag.Int("num_set", 1, "Number of sets to run")
	optNumReaderAddr := flag.Int("num_reader", 0,
		"Number of reader Goroutines")
	optNumWriterAddr := flag.Int("num_writer", 0,
		"Number of writer Goroutines")
	optNumRepeatAddr := flag.Int("num_repeat", -1,
		"Exit Goroutines after specified iterations if > 0")
	optTimeMinuteAddr := flag.Int("time_minute", 0,
		"Exit Goroutines after sum of this and -time_second option if > 0")
	optTimeSecondAddr := flag.Int("time_second", 0,
		"Exit Goroutines after sum of this and -time_minute option if > 0")
	optMonitorIntMinuteAddr := flag.Int("monitor_interval_minute", 0,
		"Monitor Goroutines every sum of this and -monitor_interval_second option if > 0")
	optMonitorIntSecondAddr := flag.Int("monitor_interval_second", 0,
		"Monitor Goroutines every sum of this and -monitor_interval_minute option if > 0")
	optStatOnlyAddr := flag.Bool("stat_only", false,
		"Do not read file data")
	optIgnoreDotAddr := flag.Bool("ignore_dot", false,
		"Ignore entries start with .")
	optFollowSymlinkAddr := flag.Bool("follow_symlink", false,
		"Follow symbolic links for read unless directory")
	optReadBufferSizeAddr := flag.Int("read_buffer_size", 1<<16,
		"Read buffer size")
	optReadSizeAddr := flag.Int("read_size", -1,
		"Read residual size per file read, use < read_buffer_size random size if 0")
	optWriteBufferSizeAddr := flag.Int("write_buffer_size", 1<<16,
		"Write buffer size")
	optWriteSizeAddr := flag.Int("write_size", -1,
		"Write residual size per file write, use < write_buffer_size random size if 0")
	optRandomWriteDataAddr := flag.Bool("random_write_data", false,
		"Use pseudo random write data")
	optNumWritePathsAddr := flag.Int("num_write_paths", 1<<10,
		"Exit writer Goroutines after creating specified files or directories if > 0")
	optTruncateWritePathsAddr := flag.Bool("truncate_write_paths", false,
		"ftruncate(2) write paths for regular files instead of write(2)")
	optFsyncWritePathsAddr := flag.Bool("fsync_write_paths", false,
		"fsync(2) write paths")
	optDirsyncWritePathsAddr := flag.Bool("dirsync_write_paths", false,
		"fsync(2) parent directories of write paths")
	optKeepWritePathsAddr := flag.Bool("keep_write_paths", false,
		"Do not unlink write paths after writer Goroutines exit")
	optCleanWritePathsAddr := flag.Bool("clean_write_paths", false,
		"Unlink existing write paths and exit")
	optWritePathsBaseAddr := flag.String("write_paths_base", "x",
		"Base name for write paths")
	optWritePathsTypeAddr := flag.String("write_paths_type", "dr",
		"File types for write paths [d|r|s|l]")
	optPathIterAddr := flag.String("path_iter", "ordered",
		"<paths> iteration type [walk|ordered|reverse|random]")
	optFlistFileAddr := flag.String("flist_file", "", "Path to flist file")
	optFlistFileCreateAddr := flag.Bool("flist_file_create", false,
		"Create flist file and exit")
	optForceAddr := flag.Bool("force", false, "Enable force mode")
	optVerboseAddr := flag.Bool("verbose", false, "Enable verbose print")
	optDebugAddr := flag.Bool("debug", false,
		"Create debug log file under home directory")
	optVersionAddr := flag.Bool("v", false, "Print version and exit")
	optHelpAddr := flag.Bool("h", false, "Print usage and exit")

	flag.Parse()
	args := flag.Args()
	optNumSet = uint(*optNumSetAddr)
	optNumReader = uint(*optNumReaderAddr)
	optNumWriter = uint(*optNumWriterAddr)
	optNumRepeat = *optNumRepeatAddr
	if optNumRepeat == 0 || optNumRepeat < -1 {
		optNumRepeat = -1
	}
	optTimeMinute = uint(*optTimeMinuteAddr)
	optTimeSecond = uint(*optTimeSecondAddr)
	optTimeSecond += optTimeMinute * 60
	optTimeMinute = 0
	optMonitorIntMinute = uint(*optMonitorIntMinuteAddr)
	optMonitorIntSecond = uint(*optMonitorIntSecondAddr)
	optMonitorIntSecond += optMonitorIntMinute * 60
	optMonitorIntMinute = 0
	optStatOnly = *optStatOnlyAddr
	optIgnoreDot = *optIgnoreDotAddr
	optFollowSymlink = *optFollowSymlinkAddr
	optReadBufferSize = uint(*optReadBufferSizeAddr)
	if optReadBufferSize > maxBufferSize {
		fmt.Println("Invalid read buffer size", optReadBufferSize)
		os.Exit(1)
	}
	optReadSize = *optReadSizeAddr
	if optReadSize < -1 {
		optReadSize = -1
	} else if optReadSize > int(maxBufferSize) {
		fmt.Println("Invalid read size", optReadSize)
		os.Exit(1)
	}
	optWriteBufferSize = uint(*optWriteBufferSizeAddr)
	if optWriteBufferSize > maxBufferSize {
		fmt.Println("Invalid write buffer size", optWriteBufferSize)
		os.Exit(1)
	}
	optWriteSize = *optWriteSizeAddr
	if optWriteSize < -1 {
		optWriteSize = -1
	} else if optWriteSize > int(maxBufferSize) {
		fmt.Println("Invalid write size", optWriteSize)
		os.Exit(1)
	}
	optRandomWriteData = *optRandomWriteDataAddr
	optNumWritePaths = *optNumWritePathsAddr
	if optNumWritePaths < -1 {
		optNumWritePaths = -1
	}
	optTruncateWritePaths = *optTruncateWritePathsAddr
	optFsyncWritePaths = *optFsyncWritePathsAddr
	optDirsyncWritePaths = *optDirsyncWritePathsAddr
	optKeepWritePaths = *optKeepWritePathsAddr
	optCleanWritePaths = *optCleanWritePathsAddr
	optWritePathsBase = *optWritePathsBaseAddr
	if len(optWritePathsBase) == 0 {
		fmt.Println("Empty write paths base")
		os.Exit(1)
	}
	if n, err := strconv.Atoi(optWritePathsBase); err == nil {
		optWritePathsBase = strings.Repeat("x", n)
		fmt.Println("Using base name", optWritePathsBase, "for write paths")
	}
	if s := *optWritePathsTypeAddr; len(s) == 0 {
		fmt.Println("Empty write paths type")
		os.Exit(1)
	} else {
		optWritePathsType = make([]fileType, len(s))
		for i, x := range s {
			var t fileType
			switch x {
			case 'd':
				t = typeDir
			case 'r':
				t = typeReg
			case 's':
				t = typeSymlink
			case 'l':
				t = typeLink
			default:
				fmt.Println("Invalid write paths type", string(x))
				os.Exit(1)
			}
			optWritePathsType[i] = t
		}
	}
	switch *optPathIterAddr {
	case "walk":
		optPathIter = pathIterWalk
	case "ordered":
		optPathIter = pathIterOrdered
	case "reverse":
		optPathIter = pathIterReverse
	case "random":
		optPathIter = pathIterRandom
	default:
		fmt.Println("Invalid path iteration type", *optPathIterAddr)
		os.Exit(1)
	}
	optFlistFile = *optFlistFileAddr
	// using flist file means not walking input directories
	if len(optFlistFile) != 0 && optPathIter == pathIterWalk {
		optPathIter = pathIterOrdered
		fmt.Println("Using flist, force -path_iter=ordered")
	}
	optFlistFileCreate = *optFlistFileCreateAddr
	optForce = *optForceAddr
	optVerbose = *optVerboseAddr
	optDebug = *optDebugAddr

	if *optVersionAddr {
		printVersion()
		os.Exit(1)
	}

	if *optHelpAddr {
		usage(progname)
		os.Exit(1)
	}

	if len(args) < 1 {
		usage(progname)
		os.Exit(1)
	}

	if isWindows() {
		fmt.Println("Windows unsupported")
		os.Exit(1)
	}

	if s := getPathSeparator(); s != "/" {
		fmt.Println("Invalid path separator", s)
		os.Exit(1)
	}

	defer cleanupLock()
	initLock()

	defer cleanupLog()
	if err := initLog(progname); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	dbg(os.Args)
	flag.VisitAll(func(f *flag.Flag) {
		dbgf("option \"%s\" -> %s\n", f.Name, f.Value)
	})

	// only allow directories since now that write is supported
	var input []string
	for _, f := range args {
		absf, err := filepath.Abs(f)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		assert(!strings.HasSuffix(absf, "/"))
		if t, err := getRawFileType(absf); err != nil {
			fmt.Println(err)
			os.Exit(1)
		} else if t != typeDir {
			fmt.Println(absf, "not directory")
			os.Exit(1)
		}
		if !optForce {
			count := 0
			for _, x := range absf {
				if x == '/' {
					count++
				}
			}
			// /path/to/dir is allowed, but /path/to is not
			if count < 3 {
				fmt.Println(absf, "not allowed, use -force option to proceed")
				os.Exit(1)
			}
		}
		input = append(input, absf)
	}
	dbg("input", input)

	// and the directories should be writable
	if optDebug && optNumWriter > 0 {
		for _, f := range input {
			if writable, err := isDirWritable(f); err != nil {
				fmt.Println(err)
				os.Exit(1)
			} else {
				dbgf("%s writable %t", f, writable)
			}
		}
	}

	// create flist and exit
	if optFlistFileCreate {
		if len(optFlistFile) == 0 {
			fmt.Println("Empty flist file path")
			os.Exit(1)
		}
		if err := createFlistFile(input, optFlistFile, optIgnoreDot, optForce); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if info, err := os.Stat(optFlistFile); err != nil {
			fmt.Println(err)
			os.Exit(1)
		} else {
			fmt.Printf("%+v\n", info)
		}
		os.Exit(0)
	}
	// clean write paths and exit
	if optCleanWritePaths {
		if l, err := collectWritePaths(input); err != nil {
			fmt.Println(err)
			os.Exit(1)
		} else if rl, err := unlinkWritePaths(l, -1); err != nil {
			fmt.Println(err)
			os.Exit(1)
		} else {
			fmt.Println("Unlinked", len(l)-len(rl), "/", len(l), "write paths")
			if len(rl) != 0 {
				fmt.Println(len(rl), "/", len(l), "write paths remaining")
				os.Exit(1)
			}
		}
		os.Exit(0)
	}

	// ready to dispatch workers
	for i := uint(0); i < optNumSet; i++ {
		if optNumSet != 1 {
			fmt.Println(strings.Repeat("=", 80))
			s := fmt.Sprintf("Set %d/%d", i+1, optNumSet)
			fmt.Println(s)
			dbg(s)
		}
		rand.Seed(time.Now().UnixNano())
		_, numInterrupted, numError, numRemain, tsv, err := dispatchWorker(input)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if numInterrupted > 0 {
			var s string
			if numInterrupted > 1 {
				s = "s"
			}
			fmt.Printf("%d worker%s interrupted\n", numInterrupted, s)
		}
		if numError > 0 {
			var s string
			if numError > 1 {
				s = "s"
			}
			fmt.Printf("%d worker%s failed\n", numError, s)
		}
		if numRemain > 0 {
			var s string
			if numRemain > 1 {
				s = "s"
			}
			fmt.Printf("%d write path%s remaining\n", numRemain, s)
		}
		printStat(tsv)
		if numInterrupted > 0 {
			break
		} else if optNumSet != 1 && i != optNumSet-1 {
			fmt.Println()
		}
	}
}
