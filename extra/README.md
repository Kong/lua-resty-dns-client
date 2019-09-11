Log analyzer
============

A tool to help analyze dns client loggings, by pretty printing the logged data.

This script will do 2 things:

1. split the log-file in separate files per worker process. Since dns data is not
   shared between workers it should be analyzed per worker.
2. pretty print the JSON snippets in the logs. It will expand the JSON into
   multiple lines, whilst retaining the log prefix with date and time stamps etc.

It will handle normal logging, and the very verbose logging (when the extra log
lines have been activated in the source code)

Usage
=====

The script should be called with 1 parameter; the log file to analyze. When ran,
it will output several files, next to the original log file, each with the 
corresponding PID appended.

Installation
============

It is a Lua script, and it requires Penlight and Cjson modules (can be installed
through LuaRocks).

You can run it from the main repo as;

```
$ luajit ./extra/clientlog.lua path/to/file/exported_logs.log
```
