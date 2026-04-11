const std = @import("std");
const c = @cImport({
    @cInclude("sqlite3.h");
});

const DbError = error {
    FailedToOpenDatabase,
    FailedToCloseDatabase,
    FailedToCreateTable,
    FailedToExecuteQuery,
    FailedToWrite,
    FailedToGetKey,
    FailedToDeleteKey,
};

const MessageType = enum {
    Get,
    GetOrElse,
    Set,
    Keys,
    KeyValues,
    KeysLike,
    Delete,
    DeleteIfExists,
    Stdin,
};

// Accepts either regular or sentinel-terminated slice
fn KeyValuePair(comptime slice: type) type {
    return struct { 
        key: slice,
        value: slice,
    };
}

// const KeyValuePair = struct {
//     key: [:0]const u8,
//     value: [:0]const u8,
// };

fn Message(comptime slice: type) type {
    return union(MessageType) {
        Get: slice,
        GetOrElse: KeyValuePair(slice),
        Set: KeyValuePair(slice),
        Keys: void,
        KeyValues: void,
        KeysLike: slice,
        Delete: slice,
        DeleteIfExists: slice,
        Stdin: void,
    };
}

const StateMachineError = error {
    InvalidState,
    InvalidCommand,
};

const StateType = enum {
    Initial,
    DatabaseOpen,
    Processing,
    Invalid,
    Closed,
};

const State = union(StateType) {
    Initial: void,
    DatabaseOpen: void,
    Processing: MessageType,
    Invalid: void,
    Closed: void,
};

fn prepare_statement(
    db: *c.sqlite3,
    query: []const u8,
) !*c.sqlite3_stmt {
    var statement: *c.sqlite3_stmt = undefined;
    const failure = c.sqlite3_prepare_v2(
        db,
        @ptrCast(query),
        @intCast(query.len),
        @ptrCast(&statement),
        null,
    );
    if (failure != c.SQLITE_OK) {
        std.log.err("Failed to compile statement: {s}", .{ c.sqlite3_errmsg(db) });
        return DbError.FailedToExecuteQuery;
    }
    return statement;
}

fn bind_text(
    db: *c.sqlite3,
    statement: *c.sqlite3_stmt,
    column: i32,
    value: []const u8, 
) !void {
    const result = c.sqlite3_bind_text(
        statement,
        column,
        @ptrCast(value),
        @intCast(value.len),
        c.SQLITE_STATIC,
    );

    if (result != c.SQLITE_OK) {
        std.log.err("Failed to bind parameter: {s}", .{ c.sqlite3_errmsg(db) });
        return DbError.FailedToExecuteQuery;
    }
}

const StateMachine = struct {
    current_state: State = .Initial,
    stdout: *std.Io.Writer,
    db: *c.sqlite3 = undefined,
    stdout_writer: *std.fs.File.Writer = undefined,
    statement: *c.sqlite3_stmt = undefined,

    fn open(self: *StateMachine, filepath: [:0]const u8) !void {
        if (self.current_state != .Initial) {
            return StateMachineError.InvalidState;
        }
        errdefer self.current_state = .Closed;

        const db: *c.sqlite3 = db: {
            var db: *c.sqlite3 = undefined;
            const failure = c.sqlite3_open(filepath, @ptrCast(&db)); 
            if (failure != 0) {
                std.log.err("Failed to open database: {s}", .{ c.sqlite3_errmsg(db) });
                _ = c.sqlite3_close(db);
                return DbError.FailedToOpenDatabase;
            }

            var error_msg: [*:0]u8 = undefined;
            const failure2 = c.sqlite3_exec(
                db,
                "CREATE TABLE IF NOT EXISTS data(id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE, value TEXT)",
                null,
                null,
                @ptrCast(&error_msg), 
            );
            if (failure2 != 0) {
                std.log.err("Failed to create table: {s}", .{ error_msg });
                c.sqlite3_free(error_msg);
                _ = c.sqlite3_close(db);
                return DbError.FailedToCreateTable;
            }

            break :db db;
        };

        self.db = db;
        errdefer _ = c.sqlite3_close(self.db);

        self.current_state = .{ .DatabaseOpen = undefined };
    }

    fn process(self: *StateMachine, comptime slice: type, message: Message(slice)) !void {
        switch (self.current_state) {
            .Initial => return StateMachineError.InvalidState, 
            .Closed => return StateMachineError.InvalidState,
            .Invalid => return StateMachineError.InvalidState,
            .Processing => |message_type| {
                if (message_type != message) {
                    return StateMachineError.InvalidState;
                }
            },
            .DatabaseOpen => {
                errdefer {
                    self.current_state = .Invalid;
                }

                switch (message) {
                    .Get, .GetOrElse => {
                        const statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db, 
                                "SELECT value FROM data WHERE key = ?",
                            ); 
                        errdefer _ = c.sqlite3_finalize(statement);
                        self.statement = statement;
                    },
                    .Set => {
                        const statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db, 
                                "INSERT INTO data (key, value) VALUES (:key, :value) ON CONFLICT(key) DO UPDATE SET id=excluded.id, value=excluded.value",
                            ); 
                        errdefer _ = c.sqlite3_finalize(statement);
                        self.statement = statement;

                        var begin_err_msg: [:0]u8 = undefined;
                        const begin_code = c.sqlite3_exec(self.db, "BEGIN TRANSACTION", null, null, @ptrCast(&begin_err_msg));
                        if (begin_code != 0) {
                            std.log.err("Failed to begin transaction {s}", .{ begin_err_msg });
                            return DbError.FailedToExecuteQuery;
                        }
                    },
                    .Keys => {
                        const statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db, 
                                "SELECT key FROM data ORDER BY id",
                            ); 
                        errdefer _ = c.sqlite3_finalize(statement);
                        self.statement = statement;
                    },
                    .KeyValues => {
                        const statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db, 
                                "SELECT key, value FROM data ORDER BY id",
                            ); 
                        errdefer _ = c.sqlite3_finalize(statement);
                        self.statement = statement;
                    },
                    .KeysLike => {
                        const statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db, 
                                "SELECT key FROM data WHERE key LIKE ? ORDER BY id",
                            ); 
                        errdefer _ = c.sqlite3_finalize(statement);
                        self.statement = statement;
                    },
                    .Delete, .DeleteIfExists => {
                        const statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db, 
                                "DELETE FROM data WHERE key = ?",
                            ); 
                        errdefer _ = c.sqlite3_finalize(statement);
                        self.statement = statement;

                        var begin_err_msg: [:0]u8 = undefined;
                        const begin_code = c.sqlite3_exec(self.db, "BEGIN TRANSACTION", null, null, @ptrCast(&begin_err_msg));
                        if (begin_code != 0) {
                            std.log.err("Failed to begin transaction {s}", .{ begin_err_msg });
                            return DbError.FailedToExecuteQuery;
                        }
                    },
                    .Stdin => return StateMachineError.InvalidCommand,
                }

                self.current_state =  .{ .Processing = message };
            }
        }

        errdefer {
            _ = c.sqlite3_finalize(self.statement);
            self.current_state = .Invalid;
        }

        switch (message) {
            .Get => |key| {
                try bind_text(self.db, self.statement, 1, key);

                const result_code = c.sqlite3_step(self.statement);
                if (result_code == c.SQLITE_ROW) {
                    _ = try self.stdout.print("{s}\n", .{ c.sqlite3_column_text(self.statement, 0) });
                } else if (result_code == c.SQLITE_DONE) {
                    std.log.err("No value found for key \"{s}\"", .{ key });
                    return DbError.FailedToGetKey;
                } else {
                    std.log.err("Failed to read row: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }

                const failure2 = c.sqlite3_reset(self.statement);
                if (failure2 != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }
            },
            .GetOrElse => |pair| {
                try bind_text(self.db, self.statement, 1, pair.key);

                const result_code = c.sqlite3_step(self.statement);
                if (result_code == c.SQLITE_ROW) {
                    _ = try self.stdout.print("{s}\n", .{ c.sqlite3_column_text(self.statement, 0) });
                } else if (result_code == c.SQLITE_DONE) {
                    _ = try self.stdout.write(pair.value);
                    _ = try self.stdout.write("\n");
                } else {
                    std.log.err("Failed to read row: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }

                const failure2 = c.sqlite3_reset(self.statement);
                if (failure2 != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }
            },
            .Set => |pair| {
                errdefer {
                    var end_err_msg: [:0]u8 = undefined;
                    const end_code = c.sqlite3_exec(self.db, "ROLLBACK TRANSACTION", null, null, @ptrCast(&end_err_msg));
                    if (end_code != 0) {
                        std.log.err("Failed to rollback transaction {s}", .{ end_err_msg });
                    }
                }

                try bind_text(self.db, self.statement, 1, pair.key);
                try bind_text(self.db, self.statement, 2, pair.value);

                const result_code = c.sqlite3_step(self.statement);
                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to insert row: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }

                const failure3 = c.sqlite3_reset(self.statement);
                if (failure3 != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }
            },
            .Keys => {
                var result_code = c.sqlite3_step(self.statement);
                while(result_code == c.SQLITE_ROW) {
                    _ = try self.stdout.print("{s}\n", .{ c.sqlite3_column_text(self.statement, 0) });

                    result_code = c.sqlite3_step(self.statement);
                }

                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to list keys: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }

                _ = c.sqlite3_finalize(self.statement);
                self.current_state = .{ .Invalid = undefined };
            },
            .KeyValues => {
                var result_code = c.sqlite3_step(self.statement);
                while(result_code == c.SQLITE_ROW) {
                    _ = try self.stdout.print("{s}={s}\n", .{ c.sqlite3_column_text(self.statement, 0), c.sqlite3_column_text(self.statement, 1) });

                    result_code = c.sqlite3_step(self.statement);
                }

                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to list keys: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }

                _ = c.sqlite3_finalize(self.statement);
                self.current_state = .{ .Invalid = undefined };
            },
            .KeysLike => |pattern| {
                try bind_text(self.db, self.statement, 1, pattern);

                var result_code = c.sqlite3_step(self.statement);
                while(result_code == c.SQLITE_ROW) {
                    _ = try self.stdout.print("{s}\n", .{ c.sqlite3_column_text(self.statement, 0) });

                    result_code = c.sqlite3_step(self.statement);
                }

                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to list keys: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }
            },
            .Delete => |key| {
                errdefer {
                    var end_err_msg: [:0]u8 = undefined;
                    const end_code = c.sqlite3_exec(self.db, "ROLLBACK TRANSACTION", null, null, @ptrCast(&end_err_msg));
                    if (end_code != 0) {
                        std.log.err("Failed to rollback transaction {s}", .{ end_err_msg });
                    }
                }

                try bind_text(self.db, self.statement, 1, key);

                const result_code = c.sqlite3_step(self.statement);
                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to delete row: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }

                const row_count = c.sqlite3_changes64(self.db);
                if (row_count == 0) {
                    std.log.err("Failed to delete entry. Key not found: \"{s}\"", .{ key });
                    return DbError.FailedToDeleteKey;
                }

                const failure2 = c.sqlite3_reset(self.statement);
                if (failure2 != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }
            },
            .DeleteIfExists => |key| {
                errdefer {
                    var end_err_msg: [:0]u8 = undefined;
                    const end_code = c.sqlite3_exec(self.db, "ROLLBACK TRANSACTION", null, null, @ptrCast(&end_err_msg));
                    if (end_code != 0) {
                        std.log.err("Failed to rollback transaction {s}", .{ end_err_msg });
                    }
                }

                try bind_text(self.db, self.statement, 1, key);

                const result_code = c.sqlite3_step(self.statement);
                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to delete row: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }

                const failure2 = c.sqlite3_reset(self.statement);
                if (failure2 != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{ c.sqlite3_errmsg(self.db) });
                    return DbError.FailedToExecuteQuery;
                }
            },
            .Stdin => return StateMachineError.InvalidCommand,
        }
    }

    fn close(self: *StateMachine) void {
        switch (self.current_state) {
            .Initial => {
                self.current_state = .{ .Closed = undefined };
                return;
            },
            .Closed => {
                // Nothing to do
                return;
            },
            .DatabaseOpen, .Invalid => {},
            .Processing => |message| {
                switch (message) {
                    .Set, .Delete, .DeleteIfExists => {
                        var end_err_msg: [:0]u8 = undefined;
                        const end_code = c.sqlite3_exec(self.db, "END TRANSACTION", null, null, @ptrCast(&end_err_msg));
                        if (end_code != 0) {
                            std.log.err("Failed to end transaction {s}", .{ end_err_msg });
                        }

                        _ = c.sqlite3_finalize(self.statement);
                    },
                    .Get, .GetOrElse, .Keys, .KeyValues, .KeysLike => {
                        _ = c.sqlite3_finalize(self.statement);
                    },
                    .Stdin => {
                        std.log.err("Stdin in close operation", .{});
                    },
                }
            },
        }

        _ = c.sqlite3_close(self.db);
        self.stdout.flush() catch {};

        self.current_state = .{ .Closed = undefined };
    }
};

const ArgIteratorWrapper = struct {
    iterator: *std.process.ArgIterator,

    fn next(self: *const ArgIteratorWrapper) !?[:0]const u8 {
        return self.iterator.next();
    }
};

const DelimiterIterator = struct {
    reader: *std.Io.Reader,
    delimiter: u8,
    is_done: bool = false,

    fn next(self: *DelimiterIterator) !?[]const u8 {
        if (self.is_done) return null;
        const result = self.reader.takeDelimiterInclusive(self.delimiter) catch |err| {
            switch(err) {
                std.Io.Reader.DelimiterError.EndOfStream => {
                    self.is_done = true;
                    return null;
                },
                else => {
                    return err;
                }
            }
        };
        if (result.len == 0) {
            self.is_done = true;
            return null;
        } else {
            return result[0..result.len - 1];
        }
    }
};

const usage = "Usage: sqey db_filepath command command_args*";

pub fn main() !u8 {
    var args = std.process.args();
    _ = args.skip();

    const filepath = args.next() orelse {
        std.log.err(usage, .{});
        return 1;
    };

    if (std.mem.eql(u8, filepath, "help") or std.mem.eql(u8, filepath, "--help")) {
        const help =
            \\
            \\Usage: sqey <path to the sqlite database> <command> <one or more command arguments>
            \\Available commands: get, get-or-else, set, keys, key-values, keys-like, delete, delete-if-exists, stdin
            \\Example: sqey mydb.db set key1 value1 key2 value2 && sqey mydb.db get key1 key2
        ;
        std.log.info(help, .{});
        return 0;
    }

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    var state_machine: StateMachine = .{ .stdout = stdout };
    defer state_machine.close();

    const wrapper: ArgIteratorWrapper = .{
        .iterator = &args,
    };

    return try processArgs(
        wrapper, 
        filepath, 
        false, 
        &state_machine,
    );
}

pub fn processArgs(
    args: anytype, 
    filepath: [:0]const u8, 
    is_stdin: bool,
    state_machine: *StateMachine,
) !u8 {
    const command_str = try args.next() orelse {
        std.log.err(usage, .{});
        return 1;
    };

    const command = if (std.mem.eql(u8, command_str, "get")) result: {
        break :result MessageType.Get;
    } else if (std.mem.eql(u8, command_str, "get-or-else")) result: {
        break :result MessageType.GetOrElse;
    } else if (std.mem.eql(u8, command_str, "set")) result: {
        break :result MessageType.Set;
    } else if (std.mem.eql(u8, command_str, "keys")) result: {
        break :result MessageType.Keys;
    } else if (std.mem.eql(u8, command_str, "key-values")) result: {
        break :result MessageType.KeyValues;
    } else if (std.mem.eql(u8, command_str, "keys-like")) result: {
        break :result MessageType.KeysLike;
    } else if (std.mem.eql(u8, command_str, "delete")) result: {
        break :result MessageType.Delete;
    } else if (std.mem.eql(u8, command_str, "delete-if-exists")) result: {
        break :result MessageType.DeleteIfExists;
    } else if (std.mem.eql(u8, command_str, "stdin")) result: {
        if (is_stdin) {
            std.log.err("Cannot process \"stdin\" while already reading from stdin", .{});
            return 1;
        } else {
            break :result MessageType.Stdin;
        }
    } else {
        std.log.err("Unknown command. Possible commands: get, get-or-else, set, keys, key-values, keys-like, delete, delete-if-exists, stdin", .{});
        return 1;
    };

    switch (command) {
        .Get => {
            var did_receive_valid_arg = false;
            while (try args.next()) |key| {
                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_machine.open(filepath);
                }

                try state_machine.process(@TypeOf(key), .{ .Get = key });
            }
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key for get command", .{});
                return 1;
            }
        },
        .GetOrElse => {
            var did_receive_valid_arg = false;
            while (try args.next()) |key| {
                const value = try args.next() orelse {
                    std.log.err("Missing default value for key \"{s}\"", .{ key });
                    return 1;
                };

                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_machine.open(filepath);
                }

                try state_machine.process(@TypeOf(key), .{ .GetOrElse = .{ .key = key, .value = value }});
            }
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key value pair for get-or-else command", .{});
                return 1;
            }
        },
        .Set => {
            var did_receive_valid_arg = false;
            while (try args.next()) |key| {
                const value = try args.next() orelse {
                    std.log.err("Missing value for key \"{s}\"", .{ key });
                    return 1;
                };

                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_machine.open(filepath);
                }

                try state_machine.process(@TypeOf(key), .{ .Set = .{ .key = key, .value = value }});
            }
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key value pair for set command", .{});
                return 1;
            }
        },
        .Keys => {
            if (try args.next()) |_| {
                std.log.err("Keys command doesn't accept extra arguments", .{});
                return 1;
            }

            try state_machine.open(filepath);
            try state_machine.process([]const u8, .{ .Keys = undefined });
        },
        .KeyValues => {
            if (try args.next()) |_| {
                std.log.err("\"key-values\" command doesn't accept extra arguments", .{});
                return 1;
            }

            try state_machine.open(filepath);
            try state_machine.process([]const u8, .{ .KeyValues = undefined });
        },
        .KeysLike => {
            const pattern = try args.next() orelse {
                std.log.err("Missing pattern for \"keys-like\"", .{});
                return 1;
            };

            if (try args.next()) |_| {
                std.log.err("\"keys-like\" command doesn't accept any extra arguments after the pattern", .{});
                return 1;
            }

            try state_machine.open(filepath);
            try state_machine.process(@TypeOf(pattern), .{ .KeysLike = pattern });
        },
        .Delete => {
            var did_receive_valid_arg = false;
            while (try args.next()) |key| {
                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_machine.open(filepath);
                }

                try state_machine.process(@TypeOf(key), .{ .Delete = key });
            }
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key for delete command", .{});
                return 1;
            }
        },
        .DeleteIfExists => {
            var did_receive_valid_arg = false;
            while (try args.next()) |key| {
                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_machine.open(filepath);
                }

                try state_machine.process(@TypeOf(key), .{ .DeleteIfExists = key });
            }
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key for \"delete-if-exists\" command", .{});
                return 1;
            }
        },
        .Stdin => {
            if (try args.next()) |_| {
                std.log.err("\"stdin\" command doesn't accept extra arguments", .{});
                return 1;
            }

            if (!is_stdin) {
                var stdin_buffer: [4096]u8 = undefined;
                var stdin_reader = std.fs.File.stdin().reader(&stdin_buffer);
                const stdin = &stdin_reader.interface;

                var iterator: DelimiterIterator = .{
                    .reader = stdin,
                    .delimiter = '\n',
                };
                return try processArgs(&iterator, filepath, true, state_machine);
            } else {
                std.log.err("Processing stdin from stdin", .{});
                return 1;
            }
        }
    }

    return 0;
}
