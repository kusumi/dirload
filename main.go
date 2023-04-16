package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
)

const (
	PATH_ITER_WALK = iota
	PATH_ITER_ORDERED
	PATH_ITER_REVERSE
	PATH_ITER_RANDOM
)

var (
	version            [3]int = [3]int{0, 2, 0}
	optNumWorker       int
	optNumRepeat       int
	optTimeMinute      int
	optTimeSecond      int
	optStatOnly        bool
	optIgnoreDot       bool
	optLstat           bool
	optReadBufferSize  int
	optPathIter        int
	optFlistFile       string
	optFlistFileCreate bool
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

	opt_num_worker := flag.Int("num_worker", 1, "Number of worker Goroutines")
	opt_num_repeat := flag.Int("num_repeat", -1, "Exit Goroutines after specified iterations if > 0")
	opt_time_minute := flag.Int("time_minute", 0, "Exit Goroutines after sum of this and -time_second option if > 0")
	opt_time_second := flag.Int("time_second", 0, "Exit Goroutines after sum of this and -time_minute option if > 0")
	opt_stat_only := flag.Bool("stat_only", false, "Do not read file data")
	opt_ignore_dot := flag.Bool("ignore_dot", false, "Ignore entry starts with .")
	opt_lstat := flag.Bool("lstat", false, "Do not resolve symbolic link")
	opt_read_buffer_size := flag.Int("read_buffer_size", 1<<16, "Read buffer size")
	opt_path_iter := flag.String("path_iter", "walk", "<paths> iteration type [walk|ordered|reverse|random]")
	opt_flist_file := flag.String("flist_file", "", "Path to flist file")
	opt_flist_file_create := flag.Bool("flist_file_create", false, "Create flist file and exit")
	opt_verbose := flag.Bool("verbose", false, "Enable verbose print")
	opt_debug := flag.Bool("debug", false, "Create debug log file under home directory")
	opt_version := flag.Bool("v", false, "Print version and exit")
	opt_help_h := flag.Bool("h", false, "Print usage and exit")

	flag.Parse()
	args := flag.Args()
	optNumWorker = *opt_num_worker
	if optNumWorker < 1 {
		optNumWorker = 1
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

	if optFlistFileCreate {
		if optFlistFile == "" {
			fmt.Println("Empty flist file path")
			os.Exit(1)
		}
		if err := createFlistFile(input, optFlistFile, optIgnoreDot); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if info, err := os.Stat(optFlistFile); err != nil {
			fmt.Println(err)
			os.Exit(1)
		} else {
			fmt.Printf("%+v\n", info)
		}
		os.Exit(1)
	}

	_, num_interrupted, num_error, err := dispatchWorker(input)
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

	printStat()
}
