package main

import (
	"testing"
)

func Test_initStat(t *testing.T) {
	siz := uint(10)
	initStat(siz, 0)

	if n := uint(len(numRepeat)); n != siz {
		t.Error(n)
	}
	if n := uint(len(numStat)); n != siz {
		t.Error(n)
	}
	if n := uint(len(numRead)); n != siz {
		t.Error(n)
	}
	if n := uint(len(numReadBytes)); n != siz {
		t.Error(n)
	}

	for i := 0; i < len(numStat); i++ {
		if numStat[i] != 0 {
			t.Error(i, numStat[i])
		}
	}
	for i := 0; i < len(numRead); i++ {
		if numStat[i] != 0 {
			t.Error(i, numRead[i])
		}
	}
	for i := 0; i < len(numReadBytes); i++ {
		if numStat[i] != 0 {
			t.Error(i, numReadBytes[i])
		}
	}
}

func Test_setTime(t *testing.T) {
	siz := uint(10)
	initStat(siz, 0)

	for i := uint(0); i < uint(len(timeBegin)); i++ {
		setTimeBegin(i)
		setTimeEnd(i)
		if timeEnd[i].Sub(timeBegin[i]).Seconds() <= 0 {
			t.Error(i, timeBegin[i], timeEnd[i])
		}
		if timeBegin[i].Sub(timeEnd[i]).Seconds() >= 0 {
			t.Error(i, timeBegin[i], timeEnd[i])
		}
	}
}

func Test_incNumRepeat(t *testing.T) {
	siz := uint(10)
	initStat(siz, 0)

	gid := uint(5)
	incNumRepeat(gid)
	for i := uint(0); i < uint(len(numRepeat)); i++ {
		if i == gid {
			if numRepeat[i] != 1 {
				t.Error(i, numRepeat[i])
			}
		} else {
			if numRepeat[i] != 0 {
				t.Error(i, numRepeat[i])
			}
		}
	}
	incNumRepeat(gid)
	for i := uint(0); i < uint(len(numRepeat)); i++ {
		if i == gid {
			if numRepeat[i] != 2 {
				t.Error(i, numRepeat[i])
			}
		} else {
			if numRepeat[i] != 0 {
				t.Error(i, numRepeat[i])
			}
		}
	}
}

func Test_incNumStat(t *testing.T) {
	siz := uint(10)
	initStat(siz, 0)

	gid := uint(5)
	incNumStat(gid)
	for i := uint(0); i < uint(len(numStat)); i++ {
		if i == gid {
			if numStat[i] != 1 {
				t.Error(i, numStat[i])
			}
		} else {
			if numStat[i] != 0 {
				t.Error(i, numStat[i])
			}
		}
	}
	incNumStat(gid)
	for i := uint(0); i < uint(len(numStat)); i++ {
		if i == gid {
			if numStat[i] != 2 {
				t.Error(i, numStat[i])
			}
		} else {
			if numStat[i] != 0 {
				t.Error(i, numStat[i])
			}
		}
	}
}

func Test_incNumRead(t *testing.T) {
	siz := uint(10)
	initStat(siz, 0)

	gid := uint(5)
	incNumRead(gid)
	for i := uint(0); i < uint(len(numRead)); i++ {
		if i == gid {
			if numRead[i] != 1 {
				t.Error(i, numRead[i])
			}
		} else {
			if numRead[i] != 0 {
				t.Error(i, numRead[i])
			}
		}
	}
	incNumRead(gid)
	for i := uint(0); i < uint(len(numRead)); i++ {
		if i == gid {
			if numRead[i] != 2 {
				t.Error(i, numRead[i])
			}
		} else {
			if numRead[i] != 0 {
				t.Error(i, numRead[i])
			}
		}
	}
}

func Test_addNumReadBytes(t *testing.T) {
	siz := uint(10)
	initStat(siz, 0)

	gid := uint(5)
	rdsiz := 1234
	addNumReadBytes(gid, rdsiz)
	for i := uint(0); i < uint(len(numReadBytes)); i++ {
		if i == gid {
			if numReadBytes[i] != uint64(rdsiz) {
				t.Error(i, numReadBytes[i])
			}
		} else {
			if numReadBytes[i] != 0 {
				t.Error(i, numReadBytes[i])
			}
		}
	}
	addNumReadBytes(gid, rdsiz)
	for i := uint(0); i < uint(len(numReadBytes)); i++ {
		if i == gid {
			if numReadBytes[i] != uint64(rdsiz*2) {
				t.Error(i, numReadBytes[i])
			}
		} else {
			if numReadBytes[i] != 0 {
				t.Error(i, numReadBytes[i])
			}
		}
	}
	addNumReadBytes(gid, 0)
	for i := uint(0); i < uint(len(numReadBytes)); i++ {
		if i == gid {
			if numReadBytes[i] != uint64(rdsiz*2) {
				t.Error(i, numReadBytes[i])
			}
		} else {
			if numReadBytes[i] != 0 {
				t.Error(i, numReadBytes[i])
			}
		}
	}
}
