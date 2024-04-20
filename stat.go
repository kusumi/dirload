package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type threadStat struct {
	isReader      bool
	inputPath     string
	timeBegin     time.Time
	timeEnd       time.Time
	numRepeat     uint64
	numStat       uint64
	numRead       uint64
	numReadBytes  uint64
	numWrite      uint64
	numWriteBytes uint64
}

func newReadStat() threadStat {
	return threadStat{
		isReader: true,
	}
}

func newWriteStat() threadStat {
	return threadStat{
		isReader: false,
	}
}

func (this *threadStat) setInputPath(f string) {
	this.inputPath = f
}

func (this *threadStat) setTimeBegin() {
	this.timeBegin = time.Now()
}

func (this *threadStat) setTimeEnd() {
	this.timeEnd = time.Now()
}

func (this *threadStat) incNumRepeat() {
	this.numRepeat++
}

func (this *threadStat) incNumStat() {
	this.numStat++
}

func (this *threadStat) incNumRead() {
	this.numRead++
}

func (this *threadStat) addNumReadBytes(siz int) {
	assert(siz >= 0)
	this.numReadBytes += uint64(siz)
}

func (this *threadStat) incNumWrite() {
	this.numWrite++
}

func (this *threadStat) addNumWriteBytes(siz int) {
	assert(siz >= 0)
	this.numWriteBytes += uint64(siz)
}

func printStat(tsv []threadStat) {
	// repeat
	width_repeat := len("repeat")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numRepeat)); len(s) > width_repeat {
			width_repeat = len(s)
		}
	}

	// stat
	width_stat := len("stat")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numStat)); len(s) > width_stat {
			width_stat = len(s)
		}
	}

	// read
	width_read := len("read")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numRead)); len(s) > width_read {
			width_read = len(s)
		}
	}

	// read[B]
	width_read_bytes := len("read[B]")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numReadBytes)); len(s) > width_read_bytes {
			width_read_bytes = len(s)
		}
	}

	// write
	width_write := len("write")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numWrite)); len(s) > width_write {
			width_write = len(s)
		}
	}

	// write[B]
	width_write_bytes := len("write[B]")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numWriteBytes)); len(s) > width_write_bytes {
			width_write_bytes = len(s)
		}
	}

	// sec
	numSec := make([]float64, len(tsv))
	for i := 0; i < len(tsv); i++ {
		numSec[i] = tsv[i].timeEnd.Sub(tsv[i].timeBegin).Seconds()
	}
	width_sec := len("sec")
	for i := 0; i < len(numSec); i++ {
		if s := fmt.Sprintf("%.2f", numSec[i]); len(s) > width_sec {
			width_sec = len(s)
		}
	}

	// MiB/sec
	numMibs := make([]float64, len(tsv))
	for i := 0; i < len(tsv); i++ {
		mib := float64(tsv[i].numReadBytes+tsv[i].numWriteBytes) / (1 << 20)
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
	for i := 0; i < len(tsv); i++ {
		assert(len(tsv[i].inputPath) != 0)
		if len(tsv[i].inputPath) > width_path {
			width_path = len(tsv[i].inputPath)
		}
	}

	// index
	nlines := len(tsv)
	width_index := 1
	if n := nlines; n > 0 {
		n -= 1 // gid starts from 0
		width_index = len(strconv.Itoa(int(n)))
	}

	tfmt := strings.Repeat(" ", 1+width_index+1)
	tfmt += fmt.Sprintf("%%-6s %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds\n",
		width_repeat, width_stat, width_read, width_read_bytes, width_write, width_write_bytes, width_sec, width_mibs, width_path)
	s := fmt.Sprintf(tfmt, "type", "repeat", "stat", "read", "read[B]", "write", "write[B]", "sec", "MiB/sec", "path")
	fmt.Print(s)
	fmt.Println(strings.Repeat("-", len(s)-1)) // exclude 1 from \n

	sfmt := fmt.Sprintf("#%%-%ds %%-6s %%%dd %%%dd %%%dd %%%dd %%%dd %%%dd %%%d.2f %%%d.2f %%-s\n",
		width_index, width_repeat, width_stat, width_read, width_read_bytes, width_write, width_write_bytes, width_sec, width_mibs)
	for i := 0; i < nlines; i++ {
		s := "reader"
		if !tsv[i].isReader {
			s = "writer"
		}
		fmt.Printf(sfmt, strconv.Itoa(int(i)), s, tsv[i].numRepeat, tsv[i].numStat,
			tsv[i].numRead, tsv[i].numReadBytes, tsv[i].numWrite, tsv[i].numWriteBytes,
			numSec[i], numMibs[i], tsv[i].inputPath)
	}
}
