package main

import (
	"testing"
	"time"
)

func Test_newReadStat(t *testing.T) {
	ts := newReadStat()
	if !ts.isReader {
		t.Error(ts.isReader)
	}
	if len(ts.inputPath) != 0 {
		t.Error(ts.inputPath)
	}
	if ts.numRepeat != 0 {
		t.Error(ts.numRepeat)
	}
	if ts.numStat != 0 {
		t.Error(ts.numStat)
	}
	if ts.numRead != 0 {
		t.Error(ts.numRead)
	}
	if ts.numReadBytes != 0 {
		t.Error(ts.numReadBytes)
	}
	if ts.numWrite != 0 {
		t.Error(ts.numWrite)
	}
	if ts.numWriteBytes != 0 {
		t.Error(ts.numWriteBytes)
	}
}

func Test_newWriteStat(t *testing.T) {
	ts := newWriteStat()
	if ts.isReader {
		t.Error(ts.isReader)
	}
	if len(ts.inputPath) != 0 {
		t.Error(ts.inputPath)
	}
	if ts.numRepeat != 0 {
		t.Error(ts.numRepeat)
	}
	if ts.numStat != 0 {
		t.Error(ts.numStat)
	}
	if ts.numRead != 0 {
		t.Error(ts.numRead)
	}
	if ts.numReadBytes != 0 {
		t.Error(ts.numReadBytes)
	}
	if ts.numWrite != 0 {
		t.Error(ts.numWrite)
	}
	if ts.numWriteBytes != 0 {
		t.Error(ts.numWriteBytes)
	}
}

func Test_setTime(t *testing.T) {
	ts := newReadStat()
	if ts.timeEnd.Sub(ts.timeBegin).Seconds() != 0 {
		t.Error(ts.timeBegin, ts.timeEnd)
	}
	if ts.timeEnd.Sub(ts.timeBegin).Milliseconds() != 0 {
		t.Error(ts.timeBegin, ts.timeEnd)
	}

	if ts.timeBegin.Sub(ts.timeEnd).Seconds() != 0 {
		t.Error(ts.timeEnd, ts.timeBegin)
	}
	if ts.timeBegin.Sub(ts.timeEnd).Milliseconds() != 0 {
		t.Error(ts.timeEnd, ts.timeBegin)
	}

	ts.setTimeBegin()
	time.Sleep(time.Second)
	ts.setTimeEnd()

	if ts.timeEnd.Sub(ts.timeBegin).Milliseconds() == 0 {
		t.Error(ts.timeBegin, ts.timeEnd)
	}
	if ts.timeEnd.Sub(ts.timeBegin).Microseconds() == 0 {
		t.Error(ts.timeBegin, ts.timeEnd)
	}
	if ts.timeEnd.Sub(ts.timeBegin).Nanoseconds() == 0 {
		t.Error(ts.timeBegin, ts.timeEnd)
	}

	if ts.timeBegin.Sub(ts.timeEnd).Milliseconds() == 0 {
		t.Error(ts.timeEnd, ts.timeBegin)
	}
	if ts.timeBegin.Sub(ts.timeEnd).Microseconds() == 0 {
		t.Error(ts.timeEnd, ts.timeBegin)
	}
	if ts.timeBegin.Sub(ts.timeEnd).Nanoseconds() == 0 {
		t.Error(ts.timeEnd, ts.timeBegin)
	}
}

func Test_incNumRepeat(t *testing.T) {
	ts := newReadStat()
	ts.incNumRepeat()
	if ts.numRepeat != 1 {
		t.Error(ts.numRepeat)
	}
	ts.incNumRepeat()
	if ts.numRepeat != 2 {
		t.Error(ts.numRepeat)
	}
}

func Test_incNumStat(t *testing.T) {
	ts := newReadStat()
	ts.incNumStat()
	if ts.numStat != 1 {
		t.Error(ts.numStat)
	}
	ts.incNumStat()
	if ts.numStat != 2 {
		t.Error(ts.numStat)
	}
}

func Test_incNumRead(t *testing.T) {
	ts := newReadStat()
	ts.incNumRead()
	if ts.numRead != 1 {
		t.Error(ts.numRead)
	}
	ts.incNumRead()
	if ts.numRead != 2 {
		t.Error(ts.numRead)
	}
}

func Test_addNumReadBytes(t *testing.T) {
	ts := newReadStat()
	siz := 1234
	ts.addNumReadBytes(siz)
	if ts.numReadBytes != uint64(siz) {
		t.Error(ts.numReadBytes)
	}
	ts.addNumReadBytes(siz)
	if ts.numReadBytes != uint64(siz)*2 {
		t.Error(ts.numReadBytes)
	}
	ts.addNumReadBytes(0)
	if ts.numReadBytes != uint64(siz)*2 {
		t.Error(ts.numReadBytes)
	}
}

func Test_setIncNumWrite(t *testing.T) {
	ts := newReadStat()
	ts.incNumWrite()
	if ts.numWrite != 1 {
		t.Error(ts.numWrite)
	}
	ts.incNumWrite()
	if ts.numWrite != 2 {
		t.Error(ts.numWrite)
	}
}

func Test_addNumWriteBytes(t *testing.T) {
	ts := newReadStat()
	siz := 1234
	ts.addNumWriteBytes(siz)
	if ts.numWriteBytes != uint64(siz) {
		t.Error(ts.numWriteBytes)
	}
	ts.addNumWriteBytes(siz)
	if ts.numWriteBytes != uint64(siz)*2 {
		t.Error(ts.numWriteBytes)
	}
	ts.addNumWriteBytes(0)
	if ts.numWriteBytes != uint64(siz)*2 {
		t.Error(ts.numWriteBytes)
	}
}
