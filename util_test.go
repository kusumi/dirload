package main

import (
	"fmt"
	"testing"
)

func Test_Lock(t *testing.T) {
	initLock()
	globalLock()
	globalUnlock()
	cleanupLock()
}

func Test_isWindows(t *testing.T) {
	if isWindows() {
		t.Error("Windows unsupported")
	}
}

func Test_getPathSeparator(t *testing.T) {
	if isWindows() {
		return
	}
	if s := getPathSeparator(); s != "/" {
		t.Error(s)
	}
}

var (
	dirList = []string{
		".",
		"..",
		"/",
		"/dev"}
	invalidList = []string{
		"",
		"516e7cb4-6ecf-11d6-8ff8-00022d09712b"}
)

func Test_getRawFileType(t *testing.T) {
	for _, f := range dirList {
		if ret, err := getRawFileType(f); ret != typeDir || err != nil {
			t.Error(f)
		}
	}
	for _, f := range invalidList {
		if ret, _ := getRawFileType(f); ret != typeInvalid {
			t.Error(f)
		}
	}
}

func Test_getFileType(t *testing.T) {
	for _, f := range dirList {
		if ret, err := getFileType(f); ret != typeDir || err != nil {
			t.Error(f)
		}
	}
	for _, f := range invalidList {
		if ret, _ := getFileType(f); ret != typeInvalid {
			t.Error(f)
		}
	}
}

func Test_pathExists(t *testing.T) {
	for _, f := range dirList {
		if exists, err := pathExists(f); !exists || err != nil {
			t.Error(f)
		}
	}
	for _, f := range invalidList {
		if exists, err := pathExists(f); exists || err == nil {
			t.Error(f)
		}
	}
}

func Test_isDotPath(t *testing.T) {
	DotList := []string{
		"/.",
		"/..",
		"./", // XXX
		"./.",
		"./..",
		".",
		"..",
		".git",
		"..git",
		"/path/to/.",
		"/path/to/..",
		"/path/to/.git/xxx",
		"/path/to/.git/.xxx",
		"/path/to/..git/xxx",
		"/path/to/..git/.xxx"}
	for _, f := range DotList {
		if !isDotPath(f) {
			t.Error(f)
		}
	}

	nonDotList := []string{
		"/",
		"xxx",
		"xxx.",
		"xxx..",
		"/path/to/xxx",
		"/path/to/xxx.",
		"/path/to/x.xxx.",
		"/path/to/git./xxx",
		"/path/to/git./xxx.",
		"/path/to/git./x.xxx."}
	for _, f := range nonDotList {
		if isDotPath(f) {
			t.Error(f)
		}
	}
}

func Test_isDirWritable(t *testing.T) {
	if !isLinux() {
		return
	}

	writableList := []string{
		"/tmp"}
	for _, f := range writableList {
		if writable, err := isDirWritable(f); !writable || err != nil {
			t.Error(f)
		}
	}

	unwritableList := []string{
		"/proc"}
	for _, f := range unwritableList {
		if writable, err := isDirWritable(f); writable || err != nil {
			t.Error(f)
		}
	}

	invalidList := []string{
		"/proc/vmstat", // regular file
		"516e7cb4-6ecf-11d6-8ff8-00022d09712b"}
	for _, f := range invalidList {
		if writable, err := isDirWritable(f); writable || err == nil {
			t.Error(f)
		}
	}
}

func Test_removeDupString(t *testing.T) {
	uniqListList := [][]string{
		{""},
		{"/path/to/xxx"},
		{"/path/to/xxx", "/path/to/yyy"},
		{"xxx1", "xxx2"},
		{"xxx1", "xxx2", "xxx3"},
		{"xxx1", "xxx2", "xxx3", "xxx4"},
		{"xxx1", "xxx2", "xxx3", ""},
		{"a", "b", "c", "d", "e", "f"},
	}
	for _, l := range uniqListList {
		x := removeDupString(l)
		for i := range x {
			for j := range x {
				if i != j {
					if x[i] == x[j] {
						t.Error(l, i, j, x)
					}
				}
			}
		}
		if len(l) != len(x) {
			t.Error(l, x)
		}
		for i := range x {
			if x[i] != l[i] {
				t.Error(i, x, l)
			}
		}
	}

	dupListList := [][]string{
		{"", ""},
		{"", "", ""},
		{"/path/to/xxx", "/path/to/xxx"},
		{"xxx1", "xxx2", "xxx1"},
		{"xxx1", "xxx2", "xxx1", "xxx1"},
		{"xxx1", "xxx1", "xxx2", "xxx1"},
		{"xxx1", "xxx2", "xxx1", "xxx2"},
		{"a", "b", "c", "d", "e", "f", "a", "b", "c", "d", "e", "f"},
	}
	for _, l := range dupListList {
		x := removeDupString(l)
		for i := range x {
			for j := range x {
				if i != j {
					if x[i] == x[j] {
						t.Error(l, i, j, x)
					}
				}
			}
		}
		if len(l) <= len(x) {
			t.Error(l, x)
		}
		xx := removeDupString(x)
		if len(x) != len(xx) {
			t.Error(x, xx)
		}
		for i := range x {
			if x[i] != xx[i] {
				t.Error(i, x, xx)
			}
		}
	}
}

func Test_assert(t *testing.T) {
	assert(true)
	assert(!false)
	assert(true != false)

	assert(0 == 0+0)
	assert(1 == 1+0)
	assert(0 != 1+0)

	assert("" == ""+"")
	assert("xxx" == "xxx"+"")
	assert("xxx" != "yyy")
}

func Test_kassert(t *testing.T) {
	kassert(true, nil)
	kassert(!false, nil)

	kassert(true, "")
	kassert(!false, "")

	kassert(true, fmt.Errorf(""))
	kassert(!false, fmt.Errorf(""))
}
