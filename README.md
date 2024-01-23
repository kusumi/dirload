dirload ([v0.4.4](https://github.com/kusumi/dirload/releases/tag/v0.4.4))
========

## About

Set read / write workloads on a file system.

## Supported platforms

Unix-likes in general

## Requirements

go 1.18 or above

## Build

    $ make

## Usage

    $ ./dirload
    usage: dirload: [<options>] <paths>
      -clean_write_paths
            Unlink existing write paths and exit
      -debug
            Create debug log file under home directory
      -dirsync_write_paths
            fsync(2) parent directories of write paths
      -flist_file string
            Path to flist file
      -flist_file_create
            Create flist file and exit
      -force
            Enable force mode
      -fsync_write_paths
            fsync(2) write paths
      -h    Print usage and exit
      -ignore_dot
            Ignore entries start with .
      -keep_write_paths
            Do not unlink write paths after writer Goroutines exit
      -lstat
            Do not resolve symbolic links
      -num_reader int
            Number of reader Goroutines
      -num_repeat int
            Exit Goroutines after specified iterations if > 0 (default -1)
      -num_write_paths int
            Exit writer Goroutines after creating specified files or directories if > 0 (default 1024)
      -num_writer int
            Number of writer Goroutines
      -path_iter string
            <paths> iteration type [walk|ordered|reverse|random] (default "ordered")
      -random_write_data
            Use pseudo random write data
      -read_buffer_size int
            Read buffer size (default 65536)
      -read_size int
            Read residual size per file read, use < read_buffer_size random size if 0 (default -1)
      -stat_only
            Do not read file data
      -time_minute int
            Exit Goroutines after sum of this and -time_second option if > 0
      -time_second int
            Exit Goroutines after sum of this and -time_minute option if > 0
      -truncate_write_paths
            ftruncate(2) write paths for regular files instead of write(2)
      -v    Print version and exit
      -verbose
            Enable verbose print
      -write_buffer_size int
            Write buffer size (default 65536)
      -write_paths_base string
            Base name for write paths (default "x")
      -write_paths_type string
            File types for write paths [d|r|s|l] (default "dr")
      -write_size int
            Write residual size per file write, use < write_buffer_size random size if 0 (default -1)
