dirload ([v0.2.0](https://github.com/kusumi/dirload/releases/tag/v0.2.0))
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
            Ignore entry starts with .
      -lstat
            Do not resolve symbolic link
      -num_repeat int
            Exit Goroutines after specified iterations if > 0 (default -1)
      -num_worker int
            Number of worker Goroutines (default 1)
      -path_iter string
            <paths> iteration type [walk|ordered|reverse|random] (default "walk")
      -read_buffer_size int
            Read buffer size (default 65536)
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
