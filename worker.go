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
	"sync/atomic"
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

func gidToRid(gid int) int {
	return gid
}

func gidToWid(gid int) int {
	return gid - optNumReader
}

func isReader(gid int) bool {
	return !isWriter(gid)
}

func isWriter(gid int) bool {
	return gidToWid(gid) >= 0
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
	// using flist file means not walking input directories
	if optFlistFile != "" && optPathIter == PATH_ITER_WALK {
		optPathIter = PATH_ITER_ORDERED
		fmt.Println("Using flist, force -path_iter=ordered")
	}

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

func debugPrintComplete(gid int, repeat int, err error) {
	var t string
	if isReader(gid) {
		t = "reader"
	} else {
		t = "writer"
	}
	// golangci-lint warns use of %w for error
	msg := fmt.Sprintf("#%d %s complete - repeat %d iswritedone %t error %s",
		gid, t, repeat, isWriteDone(gid), err)
	dbg(msg)
	if optDebug {
		fmt.Println(msg)
	}
}

func dispatchWorker(input []string) (int, int, int, int, error) {
	for _, f := range input {
		assert(filepath.IsAbs(f))
	}

	// number of readers and writers are 0 by default
	if optNumReader == 0 && optNumWriter == 0 {
		return 0, 0, 0, 0, nil
	}

	// initialize common variables among goroutines
	signal_ch := make(chan int)
	interrupt_ch := make(chan int)

	var wg sync.WaitGroup
	signaled := false

	var num_complete int32
	var num_interrupted int32
	var num_error int32

	// initialize per goroutine variables
	initReadBuffer(optNumReader, optReadBufferSize)
	initWriteBuffer(optNumWriter, optWriteBufferSize)
	initWritePaths(optNumWriter, optWritePathsType)
	initStat(optNumReader, optNumWriter)

	fls, err := setupFlist(input)
	if err != nil {
		return -1, -1, -1, -1, err
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
	for i := 0; i < optNumReader+optNumWriter; i++ {
		wg.Add(1)
		gid := i
		setTimeBegin(gid)
		go func() {
			defer wg.Done()
			defer func() {
				// XXX possible race vs signal handler goroutine
				total := int(num_complete + num_interrupted + num_error)
				if total == optNumReader+optNumWriter {
					if signaled {
						dbgf("%d+%d reader goroutines done", total, 1)
					} else {
						dbgf("%d reader goroutines done", total)
						signal_ch <- 1
					}
				}
				setTimeEnd(gid)
			}()

			// set timer for this goroutine if specified
			var timer_ch <-chan time.Time
			if optTimeSecond > 0 || optTimeMinute > 0 {
				timer_ch = time.After(
					time.Duration(optTimeMinute)*time.Minute +
						time.Duration(optTimeSecond)*time.Second)
			}

			// set input path for this goroutine
			input_path := input[gid%len(input)]
			setInputPath(gid, input_path)

			// start loop
			repeat := 0
			dbgf("#%d start", gid)
			for {
				// either walk or select from input path
				var err error
				if optPathIter == PATH_ITER_WALK {
					err = filepath.WalkDir(input_path,
						func(f string, d fs.DirEntry, err error) error {
							select {
							case <-interrupt_ch:
								dbgf("#%d interrupt", gid)
								return &workerInterrupt{}
							case <-timer_ch:
								dbgf("#%d timer", gid)
								return &workerTimer{}
							default:
								assert(strings.HasPrefix(f, input_path))
								if err != nil {
									return err
								}
								if isReader(gid) {
									return readEntry(gid, f)
								} else {
									return writeEntry(gid, f)
								}
							}
						})
				} else {
					fl := fls[gid%len(fls)]
					for j := 0; j < len(fl); j++ {
						select {
						case <-interrupt_ch:
							dbgf("#%d interrupt", gid)
							err = &workerInterrupt{}
						case <-timer_ch:
							dbgf("#%d timer", gid)
							err = &workerTimer{}
						default:
							var idx int
							switch optPathIter {
							case PATH_ITER_ORDERED:
								idx = j
							case PATH_ITER_REVERSE:
								idx = len(fl) - 1 - j
							case PATH_ITER_RANDOM:
								idx = rand.Intn(len(fl))
							default:
								idx = -1
							}
							f := fl[idx]
							assert(strings.HasPrefix(f, input_path))
							if isReader(gid) {
								err = readEntry(gid, f)
							} else {
								err = writeEntry(gid, f)
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
						atomic.AddInt32(&num_interrupted, 1)
					case *workerTimer:
						debugPrintComplete(gid, repeat, err)
						atomic.AddInt32(&num_complete, 1)
					default:
						dbgf("#%d %s", gid, err)
						fmt.Println(err)
						atomic.AddInt32(&num_error, 1)
					}
					return // not break
				}
				// otherwise continue until optNumRepeat if specified
				incNumRepeat(gid)
				repeat++
				if optNumRepeat > 0 && repeat >= optNumRepeat {
					break // usually only readers break from here
				}
				if isWriter(gid) && isWriteDone(gid) {
					break
				}
			}

			if isReader(gid) {
				assert(optNumRepeat > 0)
				assert(repeat >= optNumRepeat)
			} else {
				assert(isWriteDone(gid))
			}

			debugPrintComplete(gid, repeat, nil)
			atomic.AddInt32(&num_complete, 1)
		}()
	}

	<-signal_ch
	close(interrupt_ch)

	wg.Wait()
	assert(num_complete >= 0)
	assert(num_interrupted >= 0)
	assert(num_error >= 0)
	assert(int(num_complete+num_interrupted+num_error) == optNumReader+optNumWriter)

	if num_remain, err := cleanupWritePaths(optKeepWritePaths); err != nil {
		return -1, -1, -1, -1, err
	} else {
		return int(num_complete), int(num_interrupted), int(num_error), num_remain, nil
	}
}
