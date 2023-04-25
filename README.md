dirload ([v0.3.0](https://github.com/kusumi/dirload/releases/tag/v0.3.0))
========

## About

Set read workloads on a file system.

## Supported platforms

Unix-likes in general

## Requirements

go 1.18 or above

## Build

    $ make

or

    $ gmake

## Usage

    $ ./dirload
    usage: dirload: [<options>] <paths>
      -debug
            Create debug log file under home directory
      -flist_file string
            Path to flist file
      -flist_file_create
            Create flist file and exit
      -h    Print usage and exit
      -ignore_dot
            Ignore entries start with .
      -lstat
            Do not resolve symbolic links
      -num_reader int
            Number of reader Goroutines (default 1)
      -num_repeat int
            Exit Goroutines after specified iterations if > 0 (default -1)
      -num_writer int
            Number of writer Goroutines
      -path_iter string
            <paths> iteration type [walk|ordered|reverse|random] (default "walk")
      -read_buffer_size int
            Read buffer size (default 65536)
      -read_size int
            Read size per file read, use < read_buffer_size random size if 0 (default -1)
      -stat_only
            Do not read file data
      -time_minute int
            Exit Goroutines after sum of this and -time_second option if > 0
      -time_second int
            Exit Goroutines after sum of this and -time_minute option if > 0
      -v    Print version and exit
      -verbose
            Enable verbose print

## Resource

[https://github.com/kusumi/dirload/](https://github.com/kusumi/dirload/)
