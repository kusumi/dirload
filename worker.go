package main

import (
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
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

func dispatchWorker(input []string) (int, int, int, error) {
	for _, f := range input {
		assert(filepath.IsAbs(f))
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
	initDir(optNumWorker, optReadBufferSize)
	initStat(optNumWorker)

	// initialize file list for non-walk iterations
	var flist [][]string
	if optPathIter == PATH_ITER_WALK {
		for _, f := range input {
			fmt.Println("walk", f)
		}
	} else {
		flist = make([][]string, len(input))
		for i, f := range input {
			if l, err := initFileList(f); err != nil {
				return -1, -1, -1, err
			} else {
				fmt.Println(len(l), "files scanned from", f)
				flist[i] = l
			}
		}
		if optPathIter == PATH_ITER_RANDOM {
			rand.Seed(time.Now().UnixNano())
		}
		assert(len(input) == len(flist))
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
	for i := 0; i < optNumWorker; i++ {
		wg.Add(1)
		n := i
		setTimeBegin(n)
		go func() {
			defer wg.Done()
			defer func() {
				// XXX possible race vs signal handler goroutine
				total := int(num_complete + num_interrupted + num_error)
				if total == optNumWorker {
					if signaled {
						dbgf("%d+%d goroutines done", total, 1)
					} else {
						dbgf("%d goroutines done", total)
						signal_ch <- 1
					}
				}
				setTimeEnd(n)
			}()

			// set timer for this goroutine if specified
			var timer_ch <-chan time.Time
			if optTimeSecond > 0 || optTimeMinute > 0 {
				timer_ch = time.After(
					time.Duration(optTimeMinute)*time.Minute +
						time.Duration(optTimeSecond)*time.Second)
			}

			// set input path for this goroutine
			input_path := input[n%len(input)]
			setInputPath(n, input_path)

			// start loop
			repeat := 0
			dbgf("#%d start", n)
			for {
				// either walk or select from input path
				var err error
				if optPathIter == PATH_ITER_WALK {
					err = filepath.WalkDir(input_path,
						func(f string, d fs.DirEntry, err error) error {
							select {
							case <-interrupt_ch:
								dbgf("#%d interrupt", n)
								return &workerInterrupt{}
							case <-timer_ch:
								dbgf("#%d timer", n)
								return &workerTimer{}
							default:
								return handleEntry(n, f, d, err)
							}
						})
				} else {
					fl := flist[n%len(flist)]
					for j := 0; j < len(fl); j++ {
						select {
						case <-interrupt_ch:
							dbgf("#%d interrupt", n)
							err = &workerInterrupt{}
						case <-timer_ch:
							dbgf("#%d timer", n)
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
							err = handleEntry(n, fl[idx], nil, nil)
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
						atomic.AddInt32(&num_complete, 1)
					default:
						dbgf("#%d %s", n, err)
						fmt.Println(err)
						atomic.AddInt32(&num_error, 1)
					}
					return // not break
				}
				// otherwise continue until optNumRepeat if specified
				incNumRepeat(n)
				repeat++
				if optNumRepeat > 0 && repeat >= optNumRepeat {
					break
				}
			}
			assert(optNumRepeat > 0)
			assert(repeat >= optNumRepeat)
			dbgf("#%d done %d", n, repeat)
			atomic.AddInt32(&num_complete, 1)
		}()
	}

	<-signal_ch
	close(interrupt_ch)

	wg.Wait()
	assert(num_complete >= 0)
	assert(num_interrupted >= 0)
	assert(num_error >= 0)
	assert(int(num_complete+num_interrupted+num_error) == optNumWorker)

	return int(num_complete), int(num_interrupted), int(num_error), nil
}
