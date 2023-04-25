package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	numReader    int
	inputPath    []string
	timeBegin    []time.Time
	timeEnd      []time.Time
	numRepeat    []uint64
	numStat      []uint64
	numRead      []uint64
	numReadBytes []uint64
	numWrite     []uint64
)

func initStat(nreader int, nwriter int) {
	n := nreader + nwriter
	assert(n > 0)
	numReader = nreader
	inputPath = make([]string, n)
	timeBegin = make([]time.Time, n)
	timeEnd = make([]time.Time, n)
	numRepeat = make([]uint64, n)
	numStat = make([]uint64, n)
	numRead = make([]uint64, n)
	numReadBytes = make([]uint64, n)
	numWrite = make([]uint64, n)
}

func setInputPath(gid int, f string) {
	inputPath[gid] = f
}

func setTimeBegin(gid int) {
	timeBegin[gid] = time.Now()
}

func setTimeEnd(gid int) {
	timeEnd[gid] = time.Now()
}

func incNumRepeat(gid int) {
	numRepeat[gid]++
}

func incNumStat(gid int) {
	numStat[gid]++
}

func incNumRead(gid int) {
	numRead[gid]++
}

func addNumReadBytes(gid int, siz int) {
	assert(siz >= 0)
	numReadBytes[gid] += uint64(siz)
}

func incNumWrite(gid int) {
	numWrite[gid]++
}

func printStat() {
	assert(len(inputPath) == len(timeBegin))
	assert(len(timeBegin) == len(timeEnd))
	assert(len(timeEnd) == len(numRepeat))
	assert(len(numRepeat) == len(numStat))
	assert(len(numStat) == len(numRead))
	assert(len(numRead) == len(numReadBytes))

	// repeat
	width_repeat := len("repeat")
	for i := 0; i < len(numRepeat); i++ {
		if s := strconv.Itoa(int(numRepeat[i])); len(s) > width_repeat {
			width_repeat = len(s)
		}
	}

	// stat
	width_stat := len("stat")
	for i := 0; i < len(numStat); i++ {
		if s := strconv.Itoa(int(numStat[i])); len(s) > width_stat {
			width_stat = len(s)
		}
	}

	// read
	width_read := len("read")
	for i := 0; i < len(numRead); i++ {
		if s := strconv.Itoa(int(numRead[i])); len(s) > width_read {
			width_read = len(s)
		}
	}

	// read[B]
	width_read_bytes := len("read[B]")
	for i := 0; i < len(numReadBytes); i++ {
		if s := strconv.Itoa(int(numReadBytes[i])); len(s) > width_read_bytes {
			width_read_bytes = len(s)
		}
	}

	// write
	width_write := len("write")
	for i := 0; i < len(numWrite); i++ {
		if s := strconv.Itoa(int(numWrite[i])); len(s) > width_write {
			width_write = len(s)
		}
	}

	// sec
	numSec := make([]float64, len(timeBegin))
	for i := 0; i < len(numSec); i++ {
		numSec[i] = timeEnd[i].Sub(timeBegin[i]).Seconds()
	}
	width_sec := len("sec")
	for i := 0; i < len(numSec); i++ {
		if s := fmt.Sprintf("%.2f", numSec[i]); len(s) > width_sec {
			width_sec = len(s)
		}
	}

	// MiB/sec
	numMibs := make([]float64, len(numReadBytes))
	for i := 0; i < len(numMibs); i++ {
		mib := float64(numReadBytes[i]) / (1 << 20)
		numMibs[i] = mib / numSec[i]
	}
	width_mibs := len("MiB/sec")
	for i := 0; i < len(numMibs); i++ {
		if s := fmt.Sprintf("%.2f", numMibs[i]); len(s) > width_mibs {
			width_mibs = len(s)
		}
	}

	// path
	width_path := len("path")
	for i := 0; i < len(inputPath); i++ {
		assert(inputPath[i] != "")
		if len(inputPath[i]) > width_path {
			width_path = len(inputPath[i])
		}
	}

	// index
	width_index := 1
	if n := len(numStat); n > 0 {
		n -= 1 // gid starts from 0
		width_index = len(strconv.Itoa(n))
	}

	tfmt := strings.Repeat(" ", 1+width_index+1)
	tfmt += fmt.Sprintf("%%-6s %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds\n",
		width_repeat, width_stat, width_read, width_read_bytes, width_write, width_sec, width_mibs, width_path)
	s := fmt.Sprintf(tfmt, "type", "repeat", "stat", "read", "read[B]", "write", "sec", "MiB/sec", "path")
	fmt.Print(s)
	fmt.Println(strings.Repeat("-", len(s)))

	sfmt := fmt.Sprintf("#%%-%ds %%-6s %%%dd %%%dd %%%dd %%%dd %%%dd %%%d.2f %%%d.2f %%-s\n",
		width_index, width_repeat, width_stat, width_read, width_read_bytes, width_write, width_sec, width_mibs)
	for i := 0; i < len(numStat); i++ {
		s := "reader"
		if i >= numReader {
			s = "writer"
		}
		fmt.Printf(sfmt, strconv.Itoa(i), s, numRepeat[i], numStat[i],
			numRead[i], numReadBytes[i], numWrite[i], numSec[i], numMibs[i], inputPath[i])
	}
}
