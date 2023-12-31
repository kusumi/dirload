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
	version            [3]int = [3]int{0, 4, 0}
	optNumReader       int
	optNumWriter       int
	optNumRepeat       int
	optTimeMinute      int
	optTimeSecond      int
	optStatOnly        bool
	optIgnoreDot       bool
	optLstat           bool
	optReadBufferSize  int
	optReadSize        int
	optWriteBufferSize int
	optWriteSize       int
	optNumWritePaths   int
	optKeepWritePaths  bool
	optCleanWritePaths bool
	optWritePathsBase  string
	optPathIter        int
	optFlistFile       string
	optFlistFileCreate bool
	optForce           bool
	optVerbose         bool
	optDebug           bool
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

	opt_num_reader := flag.Int("num_reader", 0, "Number of reader Goroutines")
	opt_num_writer := flag.Int("num_writer", 0, "Number of writer Goroutines")
	opt_num_repeat := flag.Int("num_repeat", -1, "Exit Goroutines after specified iterations if > 0")
	opt_time_minute := flag.Int("time_minute", 0, "Exit Goroutines after sum of this and -time_second option if > 0")
	opt_time_second := flag.Int("time_second", 0, "Exit Goroutines after sum of this and -time_minute option if > 0")
	opt_stat_only := flag.Bool("stat_only", false, "Do not read file data")
	opt_ignore_dot := flag.Bool("ignore_dot", false, "Ignore entries start with .")
	opt_lstat := flag.Bool("lstat", false, "Do not resolve symbolic links")
	opt_read_buffer_size := flag.Int("read_buffer_size", 1<<16, "Read buffer size")
	opt_read_size := flag.Int("read_size", -1, "Read residual size per file read, use < read_buffer_size random size if 0")
	opt_write_buffer_size := flag.Int("write_buffer_size", 1<<16, "Write buffer size")
	opt_write_size := flag.Int("write_size", -1, "Write residual size per file write, use < write_buffer_size random size if 0")
	opt_num_write_paths := flag.Int("num_write_paths", 1<<10, "Exit writer Goroutines after creating specified files or directories if > 0")
	opt_keep_write_paths := flag.Bool("keep_write_paths", false, "Do not unlink write paths after writer Goroutines exit")
	opt_clean_write_paths := flag.Bool("clean_write_paths", false, "Unlink existing write paths and exit")
	opt_write_paths_base := flag.String("write_paths_base", "", "Base name for write paths")
	opt_path_iter := flag.String("path_iter", "walk", "<paths> iteration type [walk|ordered|reverse|random]")
	opt_flist_file := flag.String("flist_file", "", "Path to flist file")
	opt_flist_file_create := flag.Bool("flist_file_create", false, "Create flist file and exit")
	opt_force := flag.Bool("force", false, "Enable force mode")
	opt_verbose := flag.Bool("verbose", false, "Enable verbose print")
	opt_debug := flag.Bool("debug", false, "Create debug log file under home directory")
	opt_version := flag.Bool("v", false, "Print version and exit")
	opt_help_h := flag.Bool("h", false, "Print usage and exit")

	flag.Parse()
	args := flag.Args()
	optNumReader = *opt_num_reader
	if optNumReader < 0 {
		optNumReader = 0
	}
	optNumWriter = *opt_num_writer
	if optNumWriter < 0 {
		optNumWriter = 0
	}
	optNumRepeat = *opt_num_repeat
	optTimeMinute = *opt_time_minute
	if optTimeMinute < 0 {
		optTimeMinute = 0
	}
	optTimeSecond = *opt_time_second
	if optTimeSecond < 0 {
		optTimeSecond = 0
	}
	optStatOnly = *opt_stat_only
	optIgnoreDot = *opt_ignore_dot
	optLstat = *opt_lstat
	optReadBufferSize = *opt_read_buffer_size
	optReadSize = *opt_read_size
	if optReadSize < -1 {
		optReadSize = -1
	}
	optWriteBufferSize = *opt_write_buffer_size
	optWriteSize = *opt_write_size
	if optWriteSize < -1 {
		optWriteSize = -1
	}
	optNumWritePaths = *opt_num_write_paths
	if optNumWritePaths < -1 {
		optNumWritePaths = -1
	}
	optKeepWritePaths = *opt_keep_write_paths
	optCleanWritePaths = *opt_clean_write_paths
	optWritePathsBase = *opt_write_paths_base
	if len(optWritePathsBase) == 0 {
		optWritePathsBase = "x"
	} else if n, err := strconv.Atoi(optWritePathsBase); err == nil {
		optWritePathsBase = strings.Repeat("x", n)
		fmt.Println("Using base name", optWritePathsBase, "for write paths")
	}
	switch *opt_path_iter {
	case "walk":
		optPathIter = PATH_ITER_WALK
	case "ordered":
		optPathIter = PATH_ITER_ORDERED
	case "reverse":
		optPathIter = PATH_ITER_REVERSE
	case "random":
		optPathIter = PATH_ITER_RANDOM
	default:
		fmt.Println("Invalid path iteration type", *opt_path_iter)
		os.Exit(1)
	}
	optFlistFile = *opt_flist_file
	optFlistFileCreate = *opt_flist_file_create
	optForce = *opt_force
	optVerbose = *opt_verbose
	optDebug = *opt_debug

	if *opt_version {
		printVersion()
		os.Exit(1)
	}

	if *opt_help_h {
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

	var input []string
	for _, f := range args {
		absf, err := filepath.Abs(f)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if _, err := pathExists(absf); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		input = append(input, absf)
	}
	dbg("input", input)
	if optDebug {
		for _, f := range input {
			if writable, err := isDirWritable(f); err != nil {
				fmt.Println(err)
				os.Exit(1)
			} else {
				dbgf("%s writable %t", f, writable)
			}
		}
	}

	if optFlistFileCreate {
		if optFlistFile == "" {
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

	rand.Seed(time.Now().UnixNano())
	_, num_interrupted, num_error, num_remain, err := dispatchWorker(input)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if num_interrupted > 0 {
		var s string
		if num_interrupted > 1 {
			s = "s"
		}
		fmt.Println() // ^C
		fmt.Printf("%d worker%s interrupted\n", num_interrupted, s)
	}
	if num_error > 0 {
		var s string
		if num_error > 1 {
			s = "s"
		}
		fmt.Printf("%d worker%s failed\n", num_error, s)
	}
	if num_remain > 0 {
		var s string
		if num_remain > 1 {
			s = "s"
		}
		fmt.Printf("%d write path%s remaining\n", num_remain, s)
	}

	printStat()
}
