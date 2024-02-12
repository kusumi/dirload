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
	version               [3]int = [3]int{0, 4, 6}
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
	optLstat              bool
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
	optWritePathsType     string
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

	opt_num_set := flag.Int("num_set", 1, "Number of sets to run")
	opt_num_reader := flag.Int("num_reader", 0, "Number of reader Goroutines")
	opt_num_writer := flag.Int("num_writer", 0, "Number of writer Goroutines")
	opt_num_repeat := flag.Int("num_repeat", -1, "Exit Goroutines after specified iterations if > 0")
	opt_time_minute := flag.Int("time_minute", 0, "Exit Goroutines after sum of this and -time_second option if > 0")
	opt_time_second := flag.Int("time_second", 0, "Exit Goroutines after sum of this and -time_minute option if > 0")
	opt_monitor_int_minute := flag.Int("monitor_interval_minute", 0, "Monitor Goroutines every sum of this and -monitor_interval_second option if > 0")
	opt_monitor_int_second := flag.Int("monitor_interval_second", 0, "Monitor Goroutines every sum of this and -monitor_interval_minute option if > 0")
	opt_stat_only := flag.Bool("stat_only", false, "Do not read file data")
	opt_ignore_dot := flag.Bool("ignore_dot", false, "Ignore entries start with .")
	opt_lstat := flag.Bool("lstat", false, "Do not resolve symbolic links")
	opt_read_buffer_size := flag.Int("read_buffer_size", 1<<16, "Read buffer size")
	opt_read_size := flag.Int("read_size", -1, "Read residual size per file read, use < read_buffer_size random size if 0")
	opt_write_buffer_size := flag.Int("write_buffer_size", 1<<16, "Write buffer size")
	opt_write_size := flag.Int("write_size", -1, "Write residual size per file write, use < write_buffer_size random size if 0")
	opt_random_write_data := flag.Bool("random_write_data", false, "Use pseudo random write data")
	opt_num_write_paths := flag.Int("num_write_paths", 1<<10, "Exit writer Goroutines after creating specified files or directories if > 0")
	opt_truncate_write_paths := flag.Bool("truncate_write_paths", false, "ftruncate(2) write paths for regular files instead of write(2)")
	opt_fsync_write_paths := flag.Bool("fsync_write_paths", false, "fsync(2) write paths")
	opt_dirsync_write_paths := flag.Bool("dirsync_write_paths", false, "fsync(2) parent directories of write paths")
	opt_keep_write_paths := flag.Bool("keep_write_paths", false, "Do not unlink write paths after writer Goroutines exit")
	opt_clean_write_paths := flag.Bool("clean_write_paths", false, "Unlink existing write paths and exit")
	opt_write_paths_base := flag.String("write_paths_base", "x", "Base name for write paths")
	opt_write_paths_type := flag.String("write_paths_type", "dr", "File types for write paths [d|r|s|l]")
	opt_path_iter := flag.String("path_iter", "ordered", "<paths> iteration type [walk|ordered|reverse|random]")
	opt_flist_file := flag.String("flist_file", "", "Path to flist file")
	opt_flist_file_create := flag.Bool("flist_file_create", false, "Create flist file and exit")
	opt_force := flag.Bool("force", false, "Enable force mode")
	opt_verbose := flag.Bool("verbose", false, "Enable verbose print")
	opt_debug := flag.Bool("debug", false, "Create debug log file under home directory")
	opt_version := flag.Bool("v", false, "Print version and exit")
	opt_help_h := flag.Bool("h", false, "Print usage and exit")

	flag.Parse()
	args := flag.Args()
	optNumSet = uint(*opt_num_set)
	optNumReader = uint(*opt_num_reader)
	optNumWriter = uint(*opt_num_writer)
	optNumRepeat = *opt_num_repeat
	if optNumRepeat == 0 || optNumRepeat < -1 {
		optNumRepeat = -1
	}
	optTimeMinute = uint(*opt_time_minute)
	optTimeSecond = uint(*opt_time_second)
	optTimeSecond += optTimeMinute * 60
	optTimeMinute = 0
	optMonitorIntMinute = uint(*opt_monitor_int_minute)
	optMonitorIntSecond = uint(*opt_monitor_int_second)
	optMonitorIntSecond += optMonitorIntMinute * 60
	optMonitorIntMinute = 0
	optStatOnly = *opt_stat_only
	optIgnoreDot = *opt_ignore_dot
	optLstat = *opt_lstat
	optReadBufferSize = uint(*opt_read_buffer_size)
	if optReadBufferSize > maxBufferSize {
		fmt.Println("Invalid read buffer size", optReadBufferSize)
		os.Exit(1)
	}
	optReadSize = *opt_read_size
	if optReadSize < -1 {
		optReadSize = -1
	} else if optReadSize > int(maxBufferSize) {
		fmt.Println("Invalid read size", optReadSize)
		os.Exit(1)
	}
	optWriteBufferSize = uint(*opt_write_buffer_size)
	if optWriteBufferSize > maxBufferSize {
		fmt.Println("Invalid write buffer size", optWriteBufferSize)
		os.Exit(1)
	}
	optWriteSize = *opt_write_size
	if optWriteSize < -1 {
		optWriteSize = -1
	} else if optWriteSize > int(maxBufferSize) {
		fmt.Println("Invalid write size", optWriteSize)
		os.Exit(1)
	}
	optRandomWriteData = *opt_random_write_data
	optNumWritePaths = *opt_num_write_paths
	if optNumWritePaths < -1 {
		optNumWritePaths = -1
	}
	optTruncateWritePaths = *opt_truncate_write_paths
	optFsyncWritePaths = *opt_fsync_write_paths
	optDirsyncWritePaths = *opt_dirsync_write_paths
	optKeepWritePaths = *opt_keep_write_paths
	optCleanWritePaths = *opt_clean_write_paths
	optWritePathsBase = *opt_write_paths_base
	if optWritePathsBase == "" {
		fmt.Println("Empty write paths base")
		os.Exit(1)
	}
	if n, err := strconv.Atoi(optWritePathsBase); err == nil {
		optWritePathsBase = strings.Repeat("x", n)
		fmt.Println("Using base name", optWritePathsBase, "for write paths")
	}
	optWritePathsType = *opt_write_paths_type
	if optWritePathsType == "" {
		fmt.Println("Empty write paths type")
		os.Exit(1)
	}
	for _, x := range optWritePathsType {
		if x != 'd' && x != 'r' && x != 's' && x != 'l' {
			fmt.Println("Invalid write paths type", string(x))
			os.Exit(1)
		}
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
	// using flist file means not walking input directories
	if optFlistFile != "" && optPathIter == PATH_ITER_WALK {
		optPathIter = PATH_ITER_ORDERED
		fmt.Println("Using flist, force -path_iter=ordered")
	}
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
		} else if t != DIR {
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
		_, num_interrupted, num_error, num_remain, tsv, err := dispatchWorker(input)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if num_interrupted > 0 {
			var s string
			if num_interrupted > 1 {
				s = "s"
			}
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
		printStat(tsv)
		if num_interrupted > 0 {
			break
		} else if optNumSet != 1 && i != optNumSet-1 {
			fmt.Println()
		}
	}
}
