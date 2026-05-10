# Summary

A simple command-line utility for key-value storage based on an sqlite3 db

## Building

Supported Zig version is `0.16.0`

`zig build` in the project folder

This utility requires `sqlite3` c library to be installed on your machine

Tested on Arch Linux and MacOS

## Usage

```
sqey [options] <database> [options] <command> [args...]
```

### Commands

| Command | Description | Example |
|---|---|---|
| `set` | Insert or update key-value pairs | `sqey example.db set name Alice age 30` |
| `get` | Retrieve values by key. Fails if any key is missing | `sqey example.db get name age` |
| `get-or-else` | Retrieve value, or print a default if missing | `sqey example.db get-or-else country N/A` |
| `get-or-else-set` | Like `get-or-else`, but also stores the default | `sqey example.db get-or-else-set country UK` |
| `keys` | List all keys | `sqey example.db keys` |
| `key-values` | List all keys and values (alternating) | `sqey example.db key-values` |
| `keys-like` | List keys matching a SQL LIKE pattern | `sqey example.db keys-like 'na%'` |
| `delete` | Delete keys. Fails if any key is missing | `sqey example.db delete city` |
| `delete-if-exists` | Delete keys without error if missing | `sqey example.db delete-if-exists missing_key` |
| `rename` | Rename keys (pairs of old/new names) | `sqey example.db rename old_key new_key` |
| `stdin` | Read arguments from stdin instead of CLI | `echo -e "k1\nv1" \| sqey example.db stdin set` |

All commands that accept keys or key-value pairs can accept more than one in a single invocation

### Options

| Option | Description |
|---|---|
| `-n` | Create the database file if it does not exist. `set` and `get-or-else-set` default to true |
| `-o` | Open in readonly mode (write commands fail) |
| `-r` | Reverse output order for `keys`, `key-values`, etc. |
| `-0` | Use null (`\0`) instead of newline as separator (input and output) |
| `-b` | Use binary format (unsigned 4-byte little-endian length prefix per token) for input/output |
| `-s` | Single entry mode: treat all input as one value; output as a single value without separators |
| `-h` / `--help` | Print help |

### Examples

```bash
# Store and retrieve values
sqey example.db set name Alice age 30 city London
sqey example.db get name age city

# Read key-value pairs from stdin
echo -e "foo\nbar\nbaz\nqux" | sqey example.db stdin set
sqey example.db keys  # foo, baz

# Read-only access
sqey example.db -o get name

# Create DB on first use
sqey new.db -n get-or-else-set k1 v1
```

Options can be combined: `sqey example.db -rn keys` (reverse, create-if-missing).
