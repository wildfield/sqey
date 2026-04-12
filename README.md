# Summary

A simple command-line utility for key-value storage based on an sqlite3 db

## Building

Supported Zig version is `0.15.2`

`zig build` in the project folder

This utility requires `sqlite3` c library to be installed on your machine

## Usage

`sqey <path to the sqlite database> <command> <one or more command arguments>`

Available commands: get, get-or-else, get-or-else-set, set, keys, key-values, keys-like, delete, delete-if-exists, stdin

Example: `sqey mydb.db set key1 value1 key2 value2 && sqey mydb.db get key1 key2`
