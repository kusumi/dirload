package main

import (
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	PATH_ITER_WALK = iota
	PATH_ITER_ORDERED
	PATH_ITER_REVERSE
	PATH_ITER_RANDOM
)

type workerInterrupt struct {
	err error
}

func (this *workerInterrupt) Error() string {
	return fmt.Sprint(this.err)
}

type workerTimer struct {
	err error
}

func (this *workerTimer) Error() string {
	return fmt.Sprint(this.err)
}

type gThread struct {
	gid             uint
	dir             threadDir
	stat            threadStat
	num_complete    uint
	num_interrupted uint
	num_error       uint
}

func newRead(gid uint, bufsiz uint) gThread {
	return gThread{
		gid:  gid,
		dir:  newReadDir(bufsiz),
		stat: newReadStat(),
	}
}

func newWrite(gid uint, bufsiz uint) gThread {
	return gThread{
		gid:  gid,
		dir:  newWriteDir(bufsiz),
		stat: newWriteStat(),
	}
}

func isReader(thr *gThread) bool {
	return thr.gid < optNumReader
}

func isWriter(thr *gThread) bool {
	return !isReader(thr)
}

func setupFlistImpl(input []string) ([][]string, error) {
	fls := make([][]string, len(input))
	if optFlistFile != "" {
		// load flist from flist file
		assert(optPathIter != PATH_ITER_WALK)
		fmt.Println("flist_file", optFlistFile)
		if l, err := loadFlistFile(optFlistFile); err != nil {
			return fls, err
		} else {
			for _, s := range l {
				found := false
				for i, f := range input {
					if strings.HasPrefix(s, f) {
						fls[i] = append(fls[i], s)
						found = true
						// no break, s can exist in multiple fls[i]
					}
				}
				if !found {
					return fls, fmt.Errorf("%s has no prefix in %s", s, input)
				}
			}
		}
	} else {
		// initialize flist by walking input directories
		for i, f := range input {
			if l, err := initFlist(f, optIgnoreDot); err != nil {
				return fls, err
			} else {
				fmt.Println(len(l), "files scanned from", f)
				fls[i] = l
			}
		}
	}

	// don't allow empty flist as it results in spinning loop
	for i, fl := range fls {
		if len(fl) != 0 {
			fmt.Println("flist", input[i], len(fl))
		} else {
			return fls, fmt.Errorf("empty flist %s", input[i])
		}
	}

	return fls, nil
}

func setupFlist(input []string) ([][]string, error) {
	// setup flist for non-walk iterations
	if optPathIter == PATH_ITER_WALK {
		for _, f := range input {
			fmt.Println("Walk", f)
		}
		return nil, nil
	} else {
		if fls, err := setupFlistImpl(input); err != nil {
			return nil, err
		} else {
			assert(len(input) == len(fls))
			return fls, nil
		}
	}
}

func debugPrintComplete(thr *gThread, repeat int, err error) {
	var t string
	if isReader(thr) {
		t = "reader"
	} else {
		t = "writer"
	}
	// golangci-lint warns use of %w for error
	msg := fmt.Sprintf("#%d %s complete - repeat %d iswritedone %t error %s",
		thr.gid, t, repeat, isWriteDone(thr), err)
	dbg(msg)
	if optDebug {
		fmt.Println(msg)
	}
}

func dispatchWorker(input []string) (int, int, int, int, []threadStat, error) {
	for _, f := range input {
		assert(filepath.IsAbs(f))
	}

	// number of readers and writers are 0 by default
	if optNumReader == 0 && optNumWriter == 0 {
		return 0, 0, 0, 0, nil, nil
	}

	// initialize common variables among goroutines
	signal_ch := make(chan int)
	interrupt_ch := make(chan int)

	var wg sync.WaitGroup
	signaled := false

	// initialize dir
	initDir(optRandomWriteData, optWritePathsType)

	// initialize thread structure
	num_thread := optNumReader + optNumWriter
	var thrv []gThread
	for i := uint(0); i < num_thread; i++ {
		if i < optNumReader {
			thrv = append(thrv, newRead(i, optReadBufferSize))
		} else {
			thrv = append(thrv, newWrite(i, optWriteBufferSize))
		}
	}
	assert(uint(len(thrv)) == num_thread)

	// setup flist
	fls, err := setupFlist(input)
	if err != nil {
		return -1, -1, -1, -1, nil, err
	}

	// signal handler goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT)
		for {
			select {
			case <-interrupt_ch:
				dbg("interrupt")
				return
			case s := <-ch:
				dbg("signal", s)
				switch s {
				case syscall.SIGINT:
					signaled = true
					signal_ch <- 1
				}
			}
		}
	}()

	// worker goroutines
	for i := 0; i < len(thrv); i++ {
		wg.Add(1)
		thr := &thrv[i]
		thr.stat.setTimeBegin()
		go func() {
			defer wg.Done()
			defer func() {
				// XXX possible race vs signal handler goroutine
				total := uint(0)
				for i := 0; i < len(thrv); i++ {
					total += thrv[i].num_complete
					total += thrv[i].num_interrupted
					total += thrv[i].num_error
				}
				if total == num_thread {
					if signaled {
						dbgf("%d+%d goroutines done", total, 1)
					} else {
						dbgf("%d goroutines done", total)
						signal_ch <- 1
					}
				}
				thr.stat.setTimeEnd()
			}()

			// set timer for this goroutine if specified
			var timer_ch <-chan time.Time
			if optTimeSecond > 0 || optTimeMinute > 0 {
				timer_ch = time.After(
					time.Duration(optTimeMinute)*time.Minute +
						time.Duration(optTimeSecond)*time.Second)
			}

			// start loop
			input_path := input[thr.gid%uint(len(input))]
			thr.stat.setInputPath(input_path)

			// Note that PATH_ITER_WALK can fall into infinite loop when used
			// in conjunction with writer or symlink.
			repeat := 0
			dbgf("#%d start", thr.gid)
			for {
				// either walk or select from input path
				var err error
				if optPathIter == PATH_ITER_WALK {
					err = filepath.WalkDir(input_path,
						func(f string, d fs.DirEntry, err error) error {
							select {
							case <-interrupt_ch:
								dbgf("#%d interrupt", thr.gid)
								return &workerInterrupt{}
							case <-timer_ch:
								dbgf("#%d timer", thr.gid)
								return &workerTimer{}
							default:
								assert(strings.HasPrefix(f, input_path))
								if err != nil {
									return err
								}
								if isReader(thr) {
									return readEntry(f, thr)
								} else {
									return writeEntry(f, thr)
								}
							}
						})
				} else {
					fl := fls[thr.gid%uint(len(fls))]
					for i := 0; i < len(fl); i++ {
						select {
						case <-interrupt_ch:
							dbgf("#%d interrupt", thr.gid)
							err = &workerInterrupt{}
						case <-timer_ch:
							dbgf("#%d timer", thr.gid)
							err = &workerTimer{}
						default:
							var idx int
							switch optPathIter {
							case PATH_ITER_ORDERED:
								idx = i
							case PATH_ITER_REVERSE:
								idx = len(fl) - 1 - i
							case PATH_ITER_RANDOM:
								idx = rand.Intn(len(fl))
							default:
								idx = -1
							}
							f := fl[idx]
							assert(strings.HasPrefix(f, input_path))
							if isReader(thr) {
								err = readEntry(f, thr)
							} else {
								err = writeEntry(f, thr)
							}
						}
						if err != nil {
							break
						}
					}
				}
				// exit goroutine if error type returned
				if err != nil {
					switch err.(type) {
					case *workerInterrupt:
						thr.num_interrupted++
					case *workerTimer:
						debugPrintComplete(thr, repeat, err)
						thr.num_complete++
					default:
						dbgf("#%d %s", thr.gid, err)
						fmt.Println(err)
						thr.num_error++
					}
					return // not break
				}
				// otherwise continue until optNumRepeat if specified
				thr.stat.incNumRepeat()
				repeat++
				if optNumRepeat > 0 && repeat >= optNumRepeat {
					break // usually only readers break from here
				}
				if isWriter(thr) && isWriteDone(thr) {
					break
				}
			}

			if isReader(thr) {
				assert(optNumRepeat > 0)
				assert(repeat >= optNumRepeat)
			} else {
				assert(isWriteDone(thr))
			}

			debugPrintComplete(thr, repeat, nil)
			thr.num_complete++
		}()
	}

	<-signal_ch
	close(interrupt_ch)

	wg.Wait()

	// collect result
	num_complete := uint(0)
	num_interrupted := uint(0)
	num_error := uint(0)
	for i := 0; i < len(thrv); i++ {
		num_complete += thrv[i].num_complete
		num_interrupted += thrv[i].num_interrupted
		num_error += thrv[i].num_error
	}
	assert(num_complete+num_interrupted+num_error == num_thread)

	var tdv []*threadDir
	var tsv []threadStat
	for i := 0; i < len(thrv); i++ {
		tdv = append(tdv, &thrv[i].dir)
		tsv = append(tsv, thrv[i].stat)
	}
	if num_remain, err := cleanupWritePaths(tdv, optKeepWritePaths); err != nil {
		return -1, -1, -1, -1, nil, err
	} else {
		return int(num_complete), int(num_interrupted), int(num_error), num_remain, tsv, nil
	}
}
