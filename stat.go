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
	widthRepeat := len("repeat")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numRepeat)); len(s) > widthRepeat {
			widthRepeat = len(s)
		}
	}

	// stat
	widthStat := len("stat")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numStat)); len(s) > widthStat {
			widthStat = len(s)
		}
	}

	// read
	widthRead := len("read")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numRead)); len(s) > widthRead {
			widthRead = len(s)
		}
	}

	// read[B]
	widthReadBytes := len("read[B]")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numReadBytes)); len(s) > widthReadBytes {
			widthReadBytes = len(s)
		}
	}

	// write
	widthWrite := len("write")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numWrite)); len(s) > widthWrite {
			widthWrite = len(s)
		}
	}

	// write[B]
	widthWriteBytes := len("write[B]")
	for i := 0; i < len(tsv); i++ {
		if s := strconv.Itoa(int(tsv[i].numWriteBytes)); len(s) > widthWriteBytes {
			widthWriteBytes = len(s)
		}
	}

	// sec
	numSec := make([]float64, len(tsv))
	for i := 0; i < len(tsv); i++ {
		numSec[i] = tsv[i].timeEnd.Sub(tsv[i].timeBegin).Seconds()
	}
	widthSec := len("sec")
	for i := 0; i < len(numSec); i++ {
		if s := fmt.Sprintf("%.2f", numSec[i]); len(s) > widthSec {
			widthSec = len(s)
		}
	}

	// MiB/sec
	numMibs := make([]float64, len(tsv))
	for i := 0; i < len(tsv); i++ {
		mib := float64(tsv[i].numReadBytes+tsv[i].numWriteBytes) / (1 << 20)
		numMibs[i] = mib / numSec[i]
	}
	widthMibs := len("MiB/sec")
	for i := 0; i < len(numMibs); i++ {
		if s := fmt.Sprintf("%.2f", numMibs[i]); len(s) > widthMibs {
			widthMibs = len(s)
		}
	}

	// path
	widthPath := len("path")
	for i := 0; i < len(tsv); i++ {
		assert(len(tsv[i].inputPath) != 0)
		if len(tsv[i].inputPath) > widthPath {
			widthPath = len(tsv[i].inputPath)
		}
	}

	// index
	nlines := len(tsv)
	widthIndex := 1
	if n := nlines; n > 0 {
		n -= 1 // gid starts from 0
		widthIndex = len(strconv.Itoa(int(n)))
	}

	tfmt := strings.Repeat(" ", 1+widthIndex+1)
	tfmt += fmt.Sprintf("%%-6s %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds %%-%ds\n",
		widthRepeat, widthStat, widthRead, widthReadBytes, widthWrite, widthWriteBytes, widthSec, widthMibs, widthPath)
	s := fmt.Sprintf(tfmt, "type", "repeat", "stat", "read", "read[B]", "write", "write[B]", "sec", "MiB/sec", "path")
	fmt.Print(s)
	fmt.Println(strings.Repeat("-", len(s)-1)) // exclude 1 from \n

	sfmt := fmt.Sprintf("#%%-%ds %%-6s %%%dd %%%dd %%%dd %%%dd %%%dd %%%dd %%%d.2f %%%d.2f %%-s\n",
		widthIndex, widthRepeat, widthStat, widthRead, widthReadBytes, widthWrite, widthWriteBytes, widthSec, widthMibs)
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
