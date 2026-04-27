# Summary

A simple command-line utility for key-value storage based on an sqlite3 db

## Building

Supported Zig version is `0.15.2`

`zig build` in the project folder

This utility requires `sqlite3` c library to be installed on your machine

## Usage

Usage: `sqey [options] <path to the file> [options] <command> <one or more command arguments>`

Available commands: `get, get-or-else, get-or-else-set, set, keys, key-values, keys-like, delete, delete-if-exists, stdin`

Example: `sqey mydb.db set key1 value1 key2 value2 && sqey mydb.db get key1 key2`

Available Options:

-0: Output: use null terminator instead of new line when printing. Input: tokens are separated by null terminator instead of newline when using "stdin"

-b: Output: use binary format when printing. Input: use binary format when using "stdin". Binary format: instead of terminator, each token is preceded by a 32-bit unsigned little endian length

-r: Reverse output order for some commands that print keys (keys, key-values, ...)

-h\\--help: Print help
