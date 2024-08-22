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
	pathIterWalk = iota
	pathIterOrdered
	pathIterReverse
	pathIterRandom
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
	gid            uint
	dir            threadDir
	stat           threadStat
	numComplete    uint
	numInterrupted uint
	numError       uint
}

func (this *gThread) isReader() bool {
	return this.gid < optNumReader
}

func (this *gThread) isWriter() bool {
	return !this.isReader()
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

func setupFlistImpl(input []string) ([][]string, error) {
	fls := make([][]string, len(input))
	if len(optFlistFile) != 0 {
		// load flist from flist file
		assert(optPathIter != pathIterWalk)
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
	if optPathIter == pathIterWalk {
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
	if thr.isReader() {
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
	assert(optTimeMinute == 0)
	assert(optMonitorIntMinute == 0)

	// number of readers and writers are 0 by default
	if optNumReader == 0 && optNumWriter == 0 {
		return 0, 0, 0, 0, nil, nil
	}

	// initialize common variables among goroutines
	signalCh := make(chan int)
	interruptCh := make(chan int)

	var wg sync.WaitGroup
	signaled := false

	// initialize dir
	initDir(optRandomWriteData)

	// initialize thread structure
	numThread := optNumReader + optNumWriter
	var thrv []gThread
	for i := uint(0); i < numThread; i++ {
		if i < optNumReader {
			thrv = append(thrv, newRead(i, optReadBufferSize))
		} else {
			thrv = append(thrv, newWrite(i, optWriteBufferSize))
		}
	}
	assert(uint(len(thrv)) == numThread)

	// setup flist
	fls, err := setupFlist(input)
	if err != nil {
		return -1, -1, -1, -1, nil, err
	}
	if optPathIter == pathIterWalk {
		assert(len(fls) == 0)
	} else {
		assert(len(fls) != 0)
	}

	// signal handler goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT)
		label := "[signal]"
		for {
			select {
			case <-interruptCh:
				dbg(label, "interrupt")
				return
			case s := <-ch:
				dbg("signal", s)
				switch s {
				case syscall.SIGINT:
					signaled = true
					signalCh <- 1
				}
			}
		}
	}()

	// monitor goroutine
	if optMonitorIntSecond > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := time.Duration(time.Duration(optMonitorIntSecond) * time.Second)
			timerCh := time.After(d)
			label := "[monitor]"
			for {
				select {
				case <-interruptCh:
					dbg(label, "interrupt")
					return
				case <-timerCh:
					dbg(label, "timer")
					// ignore possible race
					var tsv []threadStat
					for i := 0; i < len(thrv); i++ {
						if thrv[i].numComplete == 0 {
							thrv[i].stat.setTimeEnd()
						}
						tsv = append(tsv, thrv[i].stat)
					}
					printStat(tsv)
					timerCh = time.After(d)
				}
			}
		}()
	}

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
					total += thrv[i].numComplete
					total += thrv[i].numInterrupted
					total += thrv[i].numError
				}
				if total == numThread {
					if signaled {
						dbgf("%d+%d goroutines done", total, 1)
					} else {
						dbgf("%d goroutines done", total)
						signalCh <- 1
					}
				}
				thr.stat.setTimeEnd()
			}()

			// set timer for this goroutine if specified
			var timerCh <-chan time.Time
			if optTimeSecond > 0 {
				timerCh = time.After(time.Duration(optTimeSecond) * time.Second)
			}

			// start loop
			inputPath := input[thr.gid%uint(len(input))]
			thr.stat.setInputPath(inputPath)

			// Note that pathIterWalk can fall into infinite loop when used
			// in conjunction with writer or symlink.
			repeat := 0
			dbgf("#%d start", thr.gid)
			for {
				// either walk or select from input path
				var err error
				if optPathIter == pathIterWalk {
					err = filepath.WalkDir(inputPath,
						func(f string, d fs.DirEntry, err error) error {
							select {
							case <-interruptCh:
								dbgf("#%d interrupt", thr.gid)
								return &workerInterrupt{}
							case <-timerCh:
								dbgf("#%d timer", thr.gid)
								return &workerTimer{}
							default:
								assert(strings.HasPrefix(f, inputPath))
								if err != nil {
									return err
								}
								if thr.isReader() {
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
						case <-interruptCh:
							dbgf("#%d interrupt", thr.gid)
							err = &workerInterrupt{}
						case <-timerCh:
							dbgf("#%d timer", thr.gid)
							err = &workerTimer{}
						default:
							var idx int
							switch optPathIter {
							case pathIterOrdered:
								idx = i
							case pathIterReverse:
								idx = len(fl) - 1 - i
							case pathIterRandom:
								idx = rand.Intn(len(fl))
							default:
								idx = -1
							}
							f := fl[idx]
							assert(strings.HasPrefix(f, inputPath))
							if thr.isReader() {
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
						thr.numInterrupted++
					case *workerTimer:
						debugPrintComplete(thr, repeat, err)
						thr.numComplete++
					default:
						dbgf("#%d %s", thr.gid, err)
						fmt.Println(err)
						thr.numError++
					}
					return // not break
				}
				// otherwise continue until optNumRepeat if specified
				thr.stat.incNumRepeat()
				repeat++
				if optNumRepeat > 0 && repeat >= optNumRepeat {
					break // usually only readers break from here
				}
				if thr.isWriter() && isWriteDone(thr) {
					break
				}
			}

			if thr.isReader() {
				assert(optNumRepeat > 0)
				assert(repeat >= optNumRepeat)
			}
			debugPrintComplete(thr, repeat, nil)
			thr.numComplete++
		}()
	}

	<-signalCh
	close(interruptCh)

	wg.Wait()

	// collect result
	numComplete := uint(0)
	numInterrupted := uint(0)
	numError := uint(0)
	for i := 0; i < len(thrv); i++ {
		numComplete += thrv[i].numComplete
		numInterrupted += thrv[i].numInterrupted
		numError += thrv[i].numError
	}
	assert(numComplete+numInterrupted+numError == numThread)

	var tdv []*threadDir
	var tsv []threadStat
	for i := 0; i < len(thrv); i++ {
		tdv = append(tdv, &thrv[i].dir)
		tsv = append(tsv, thrv[i].stat)
	}
	if numRemain, err := cleanupWritePaths(tdv, optKeepWritePaths); err != nil {
		return -1, -1, -1, -1, nil, err
	} else {
		return int(numComplete), int(numInterrupted), int(numError), numRemain, tsv, nil
	}
}
