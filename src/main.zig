const std = @import("std");
const c = @cImport({
    @cInclude("sqlite3.h");
});

const DbError = error{
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
    GetOrElseSet,
    Set,
    Keys,
    KeyValues,
    KeysLike,
    Delete,
    DeleteIfExists,
    Stdin,
};

// Accepts either regular or sentinel-terminated slice
const KeyValuePair = struct {
    key: []const u8,
    value: []const u8,
};

const Message = union(MessageType) {
    Get: []const u8,
    GetOrElse: KeyValuePair,
    GetOrElseSet: KeyValuePair,
    Set: KeyValuePair,
    Keys: void,
    KeyValues: void,
    KeysLike: []const u8,
    Delete: []const u8,
    DeleteIfExists: []const u8,
    Stdin: void,
};

const StateMachineError = error{
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
        std.log.err("Failed to compile statement: {s}", .{c.sqlite3_errmsg(db)});
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
    const result = c.sqlite3_bind_text64(
        statement,
        column,
        @ptrCast(value),
        @intCast(value.len),
        c.SQLITE_STATIC,
        c.SQLITE_UTF8,
    );

    if (result != c.SQLITE_OK) {
        std.log.err("Failed to bind parameter: {s}", .{c.sqlite3_errmsg(db)});
        return DbError.FailedToExecuteQuery;
    }
}

fn bind_blob(
    db: *c.sqlite3,
    statement: *c.sqlite3_stmt,
    column: i32,
    value: []const u8,
) !void {
    const result = c.sqlite3_bind_blob64(
        statement,
        column,
        @ptrCast(value),
        value.len,
        c.SQLITE_STATIC,
    );

    if (result != c.SQLITE_OK) {
        std.log.err("Failed to bind parameter: {s}", .{c.sqlite3_errmsg(db)});
        return DbError.FailedToExecuteQuery;
    }
}

const TokenWriterError = error {
    SizeTooLarge
};

fn printTokenWriter(
    writer: *std.Io.Writer,
    delimiter: u8,
    is_binary_format: bool,
    is_single_entry: bool,
    token: []const u8,
) !void {
    if (is_binary_format) {
        if (std.math.cast(u32, token.len)) |len| { 
            _ = try writer.writeInt(u32, len, .little);
        } else {
            return TokenWriterError.SizeTooLarge;
        }
        _ = try writer.writeAll(token);
    } else if (is_single_entry) {
        _ = try writer.writeAll(token);
    } else {
        _ = try writer.writeAll(token);
        _ = try writer.writeByte(delimiter);
    }
}

const StateMachine = struct {
    current_state: State = .Initial,
    stdout: *std.Io.Writer,
    delimiter: u8,
    is_binary_protocol: bool,
    is_single_entry: bool,
    is_reverse_order_output: bool,
    db: *c.sqlite3 = undefined,
    statement: *c.sqlite3_stmt = undefined,
    // GetOrElseSet uses 2 statements
    extra_statement: *c.sqlite3_stmt = undefined,

    fn open(self: *StateMachine, filepath: [:0]const u8, allow_create: bool) !void {
        if (self.current_state != .Initial) {
            return StateMachineError.InvalidState;
        }
        errdefer self.current_state = .Closed;

        const db: *c.sqlite3 = db: {
            var db: *c.sqlite3 = undefined;
            var flags: c_int = c.SQLITE_OPEN_READWRITE;
            if (allow_create) {
                flags |= c.SQLITE_OPEN_CREATE;
            }
            const failure = c.sqlite3_open_v2(filepath, @ptrCast(&db), flags, null);
            if (failure != 0) {
                std.log.err("Failed to open database: {s}", .{c.sqlite3_errmsg(db)});
                std.log.err("Hint: Database must first be created using a \"set\" command if it doesn't exist.", .{});
                _ = c.sqlite3_close(db);
                return DbError.FailedToOpenDatabase;
            }

            var error_msg: [*c]u8 = undefined;
            const failure2 = c.sqlite3_exec(
                db,
                "CREATE TABLE IF NOT EXISTS data(id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE, value BLOB)",
                null,
                null,
                &error_msg,
            );
            if (failure2 != 0) {
                std.log.err("Failed to create table: {s}", .{error_msg});
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

    fn printToken(self: StateMachine, token: []const u8) !void {
        try printTokenWriter(
            self.stdout,
            self.delimiter,
            self.is_binary_protocol,
            self.is_single_entry,
            token,
        );
    }

    fn getColumnBlob(self: StateMachine, column: c_int) []const u8 {
        return @as([*]const u8, @ptrCast(c.sqlite3_column_blob(self.statement, column)))[0..@intCast(c.sqlite3_column_bytes(self.statement, column))];
    }

    fn process(self: *StateMachine, message: Message) !void {
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

                const order = order: {
                    if (!self.is_reverse_order_output) {
                        break :order "ASC";
                    } else {
                        break :order "DESC";
                    }
                };

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
                    .GetOrElseSet => {
                        const statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db,
                                "SELECT value FROM data WHERE key = ?",
                            );
                        errdefer _ = c.sqlite3_finalize(statement);
                        self.statement = statement;

                        const extra_statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db,
                                "INSERT INTO data (key, value) VALUES (:key, :value) ON CONFLICT(key) DO UPDATE SET id=excluded.id, value=excluded.value",
                            );
                        errdefer _ = c.sqlite3_finalize(extra_statement);
                        self.extra_statement = extra_statement;

                        var begin_err_msg: [*c]u8 = undefined;
                        const begin_code = c.sqlite3_exec(self.db, "BEGIN TRANSACTION", null, null, &begin_err_msg);
                        if (begin_code != 0) {
                            std.log.err("Failed to begin transaction {s}", .{begin_err_msg});
                            c.sqlite3_free(begin_err_msg);
                            return DbError.FailedToExecuteQuery;
                        }
                    },
                    .Set => {
                        const statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db,
                                "INSERT INTO data (key, value) VALUES (:key, :value) ON CONFLICT(key) DO UPDATE SET id=excluded.id, value=excluded.value",
                            );
                        errdefer _ = c.sqlite3_finalize(statement);
                        self.statement = statement;

                        var begin_err_msg: [*c]u8 = undefined;
                        const begin_code = c.sqlite3_exec(self.db, "BEGIN TRANSACTION", null, null, &begin_err_msg);
                        if (begin_code != 0) {
                            std.log.err("Failed to begin transaction {s}", .{begin_err_msg});
                            c.sqlite3_free(begin_err_msg);
                            return DbError.FailedToExecuteQuery;
                        }
                    },
                    .Keys => {
                        const statement_str_pattern = "SELECT key FROM data ORDER BY id {s}";
                        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
                        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{ order });

                        const statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db,
                                statement_str,
                            );
                        errdefer _ = c.sqlite3_finalize(statement);
                        self.statement = statement;
                    },
                    .KeyValues => {
                        const statement_str_pattern = "SELECT key, value FROM data ORDER BY id {s}";
                        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
                        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{ order });

                        const statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db,
                                statement_str,
                            );
                        errdefer _ = c.sqlite3_finalize(statement);
                        self.statement = statement;
                    },
                    .KeysLike => {
                        const statement_str_pattern = "SELECT key FROM data WHERE key LIKE ? ORDER BY id {s}";
                        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
                        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{ order });

                        const statement: *c.sqlite3_stmt =
                            try prepare_statement(
                                self.db,
                                statement_str,
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

                        var begin_err_msg: [*c]u8 = undefined;
                        const begin_code = c.sqlite3_exec(self.db, "BEGIN TRANSACTION", null, null, &begin_err_msg);
                        if (begin_code != 0) {
                            std.log.err("Failed to begin transaction {s}", .{begin_err_msg});
                            c.sqlite3_free(begin_err_msg);
                            return DbError.FailedToExecuteQuery;
                        }
                    },
                    .Stdin => return StateMachineError.InvalidCommand,
                }

                self.current_state = .{ .Processing = message };
            },
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
                    try self.printToken(self.getColumnBlob(0));
                } else if (result_code == c.SQLITE_DONE) {
                    std.log.err("No value found for key \"{s}\"", .{key});
                    return DbError.FailedToGetKey;
                } else {
                    std.log.err("Failed to read row: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }

                const failure2 = c.sqlite3_reset(self.statement);
                if (failure2 != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }
            },
            .GetOrElse => |pair| {
                try bind_text(self.db, self.statement, 1, pair.key);

                const result_code = c.sqlite3_step(self.statement);
                if (result_code == c.SQLITE_ROW) {
                    try self.printToken(self.getColumnBlob(0));
                } else if (result_code == c.SQLITE_DONE) {
                    try self.printToken(pair.value);
                } else {
                    std.log.err("Failed to read row: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }

                const failure2 = c.sqlite3_reset(self.statement);
                if (failure2 != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }
            },
            .GetOrElseSet => |pair| {
                errdefer {
                    _ = c.sqlite3_finalize(self.extra_statement);

                    var end_err_msg: [*c]u8 = undefined;
                    const end_code = c.sqlite3_exec(self.db, "ROLLBACK TRANSACTION", null, null, &end_err_msg);
                    if (end_code != 0) {
                        std.log.err("Failed to rollback transaction {s}", .{end_err_msg});
                    }
                }

                try bind_text(self.db, self.statement, 1, pair.key);

                const result_code = c.sqlite3_step(self.statement);
                if (result_code == c.SQLITE_ROW) {
                    try self.printToken(self.getColumnBlob(0));
                } else if (result_code == c.SQLITE_DONE) {
                    try self.printToken(pair.value);

                    try bind_text(self.db, self.extra_statement, 1, pair.key);
                    try bind_blob(self.db, self.extra_statement, 2, pair.value);
                    const extra_result_code = c.sqlite3_step(self.extra_statement);

                    if (extra_result_code != c.SQLITE_DONE) {
                        std.log.err("Failed to insert row: {s}", .{c.sqlite3_errmsg(self.db)});
                        return DbError.FailedToExecuteQuery;
                    }

                    const extra_reset_code = c.sqlite3_reset(self.extra_statement);
                    if (extra_reset_code != c.SQLITE_OK) {
                        std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(self.db)});
                        return DbError.FailedToExecuteQuery;
                    }
                } else {
                    std.log.err("Failed to read row: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }

                const reset_code = c.sqlite3_reset(self.statement);
                if (reset_code != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }
            },
            .Set => |pair| {
                errdefer {
                    var end_err_msg: [*c]u8 = undefined;
                    const end_code = c.sqlite3_exec(self.db, "ROLLBACK TRANSACTION", null, null, &end_err_msg);
                    if (end_code != 0) {
                        std.log.err("Failed to rollback transaction {s}", .{end_err_msg});
                    }
                }

                try bind_text(self.db, self.statement, 1, pair.key);
                try bind_blob(self.db, self.statement, 2, pair.value);

                const result_code = c.sqlite3_step(self.statement);
                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to insert row: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }

                const failure3 = c.sqlite3_reset(self.statement);
                if (failure3 != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }
            },
            .Keys => {
                var result_code = c.sqlite3_step(self.statement);
                while (result_code == c.SQLITE_ROW) {
                    try self.printToken(std.mem.sliceTo(c.sqlite3_column_text(self.statement, 0), 0));

                    result_code = c.sqlite3_step(self.statement);
                }

                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to list keys: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }

                _ = c.sqlite3_finalize(self.statement);
                self.current_state = .{ .Invalid = undefined };
            },
            .KeyValues => {
                var result_code = c.sqlite3_step(self.statement);
                while (result_code == c.SQLITE_ROW) {
                    try self.printToken(std.mem.sliceTo(c.sqlite3_column_text(self.statement, 0), 0));
                    try self.printToken(self.getColumnBlob(1));

                    result_code = c.sqlite3_step(self.statement);
                }

                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to list keys: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }

                _ = c.sqlite3_finalize(self.statement);
                self.current_state = .{ .Invalid = undefined };
            },
            .KeysLike => |pattern| {
                try bind_text(self.db, self.statement, 1, pattern);

                var result_code = c.sqlite3_step(self.statement);
                while (result_code == c.SQLITE_ROW) {
                    try self.printToken(std.mem.sliceTo(c.sqlite3_column_text(self.statement, 0), 0));

                    result_code = c.sqlite3_step(self.statement);
                }

                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to list keys: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }

                _ = c.sqlite3_finalize(self.statement);
                self.current_state = .{ .Invalid = undefined };
            },
            .Delete => |key| {
                errdefer {
                    var end_err_msg: [*c]u8 = undefined;
                    const end_code = c.sqlite3_exec(self.db, "ROLLBACK TRANSACTION", null, null, &end_err_msg);
                    if (end_code != 0) {
                        std.log.err("Failed to rollback transaction {s}", .{end_err_msg});
                    }
                }

                try bind_text(self.db, self.statement, 1, key);

                const result_code = c.sqlite3_step(self.statement);
                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to delete row: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }

                const row_count = c.sqlite3_changes64(self.db);
                if (row_count == 0) {
                    std.log.err("Failed to delete entry. Key not found: \"{s}\"", .{key});
                    return DbError.FailedToDeleteKey;
                }

                const failure2 = c.sqlite3_reset(self.statement);
                if (failure2 != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }
            },
            .DeleteIfExists => |key| {
                errdefer {
                    var end_err_msg: [*c]u8 = undefined;
                    const end_code = c.sqlite3_exec(self.db, "ROLLBACK TRANSACTION", null, null, &end_err_msg);
                    if (end_code != 0) {
                        std.log.err("Failed to rollback transaction {s}", .{end_err_msg});
                    }
                }

                try bind_text(self.db, self.statement, 1, key);

                const result_code = c.sqlite3_step(self.statement);
                if (result_code != c.SQLITE_DONE) {
                    std.log.err("Failed to delete row: {s}", .{c.sqlite3_errmsg(self.db)});
                    return DbError.FailedToExecuteQuery;
                }

                const failure2 = c.sqlite3_reset(self.statement);
                if (failure2 != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(self.db)});
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
                        var end_err_msg: [*c]u8 = undefined;
                        const end_code = c.sqlite3_exec(self.db, "END TRANSACTION", null, null, &end_err_msg);
                        if (end_code != 0) {
                            std.log.err("Failed to end transaction {s}", .{end_err_msg});
                        }

                        _ = c.sqlite3_finalize(self.statement);
                    },
                    .GetOrElseSet => {
                        var end_err_msg: [*c]u8 = undefined;
                        const end_code = c.sqlite3_exec(self.db, "END TRANSACTION", null, null, &end_err_msg);
                        if (end_code != 0) {
                            std.log.err("Failed to end transaction {s}", .{end_err_msg});
                        }

                        _ = c.sqlite3_finalize(self.statement);
                        _ = c.sqlite3_finalize(self.extra_statement);
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

    // This converts ?[]const u8 to !?[]const u8 such that we can use try with it
    // it is needed to make interface consistent between ArgIterator and DelimiterIterator
    fn next(self: *const ArgIteratorWrapper) !?[]const u8 {
        return self.iterator.next();
    }
};

// Limit entries to 1 MB for sanity checking
const MAX_STDIN_SIZE = 1024 * 1024;

const DelimiterIteratorError = error {
    SizeTooLarge
};

const DelimiterIteratorOptions = struct {
    delimiter: u8,
    is_binary_protocol: bool,
    is_single_entry: bool,
};

const DelimiterIterator = struct {
    reader: *std.Io.Reader,
    delimiter: u8,
    is_binary_protocol: bool,
    is_single_entry: bool,
    is_done: bool = false,
    leftover_args: []const []const u8,
    leftover_args_read_count: usize = 0,
    input_writer: std.io.Writer.Allocating,

    fn init(allocator: std.mem.Allocator, reader: *std.Io.Reader, leftover_args: [][]const u8, options: DelimiterIteratorOptions) DelimiterIterator {
        const input_writer = std.io.Writer.Allocating.init(allocator);

        return .{
            .reader = reader,
            .delimiter = options.delimiter,
            .is_binary_protocol = options.is_binary_protocol,
            .is_single_entry = options.is_single_entry,
            .leftover_args = leftover_args,
            .input_writer = input_writer,
        };
    }

    fn deinit(self: *DelimiterIterator) void {
        self.input_writer.deinit();
    }

    // Calling next invalidates the result from previous call, except when the iterator is done
    fn next(self: *DelimiterIterator) !?[]const u8 {
        if (self.leftover_args_read_count < self.leftover_args.len) {
            self.leftover_args_read_count += 1;
            return self.leftover_args[self.leftover_args_read_count - 1];
        } else if (self.is_binary_protocol) {
            while (true) {
                if (self.is_done) return null;
                self.input_writer.clearRetainingCapacity();

                const number_bytes = self.reader.takeInt(u32, .little) catch |err| {
                    switch (err) {
                        std.Io.Reader.Error.EndOfStream => {
                            self.is_done = true;
                            return null;
                        },
                        else => {
                            return err;
                        },
                    }
                };

                if (number_bytes >= MAX_STDIN_SIZE) {
                    std.log.err("The length of token is too long: {d}", .{number_bytes});
                    self.is_done = true;
                    return DelimiterIteratorError.SizeTooLarge;
                } else if (number_bytes > 0) {
                    try self.reader.streamExact(&self.input_writer.writer, number_bytes);
                    return self.input_writer.written();
                } else {
                    // Ignore 0-length element
                }
            }
        } else if (self.is_single_entry) {
            if (self.is_done) return null;
            self.input_writer.clearRetainingCapacity();

            _ = try self.reader.stream(&self.input_writer.writer, .limited(MAX_STDIN_SIZE));
            self.is_done = true;

            return self.input_writer.written();
        } else {
            while (true) {
                if (self.is_done) return null;
                self.input_writer.clearRetainingCapacity();

                const read_bytes = try self.reader.streamDelimiterLimit(
                    &self.input_writer.writer,
                    self.delimiter,
                    .limited(MAX_STDIN_SIZE),
                );

                // Toss the delimiter
                _ = self.reader.takeByte() catch |err| {
                    switch (err) {
                        error.EndOfStream => {
                            self.is_done = true;
                        },
                        else => {
                            return err;
                        },
                    }
                };

                if (read_bytes == 0) {
                    // Skip to the next iteration
                } else {
                    return self.input_writer.written();
                }
            }
        }
    }
};

const help =
    \\
    \\Usage: sqey [options] <path to the file> [options] <command> <one or more command arguments>
    \\
    \\Available Commands: get, get-or-else, get-or-else-set, set, keys, key-values, keys-like, delete, delete-if-exists, stdin
    \\
    \\Example: sqey mydb.db set key1 value1 key2 value2 && sqey mydb.db get key1 key2
    \\
    \\Available Options:
    \\-0: Output: use null terminator instead of new line when printing. Input: tokens are separated by null terminator instead of newline when using "stdin"
    \\-b: Output: use binary format when printing. Input: use binary format when using "stdin". Binary format: instead of terminator, each token is preceded by a 32-bit unsigned little endian length
    \\-s: Single entry input/output
    \\-r: Reverse output order for some commands that print keys (keys, key-values, ...)
    \\-h\--help: Print help
    \\
;

const usage = help;

const OptionsParsingError = error {
    MissingArgument,
    ConflictingOptions,
};

const OptionParsingResultEnum = enum {
    OptionsAndArg,
    Help
};

const Options = struct {
    delimiter: u8 = '\n',
    is_binary_protocol: bool = false,
    is_reverse_order_output: bool = false,
    is_single_entry: bool = false,
};

const OptionsResult = struct {
    options: Options,
    arg: [:0]const u8,
};

const OptionParsingResult = union(OptionParsingResultEnum) {
    OptionsAndArg: OptionsResult,
    Help: void,
};

fn parseOptionsOrArg(
    args: *std.process.ArgIterator,
    initial_options: Options,
) OptionsParsingError!OptionParsingResult {
    var options = initial_options;

    var arg = args.next() orelse {
        std.log.err(usage, .{});
        return error.MissingArgument;
    };

    var is_options = true; 

    while (is_options) {
        is_options = arg.len > 0 and arg[0] == '-';  

        if (is_options) {
            const options_arg = arg;

            const is_help = std.mem.eql(u8, options_arg, "--help") or std.mem.containsAtLeastScalar(u8, options_arg, 1, 'h');
            if (is_help) {
                return .{ .Help = undefined };
            } else if (std.mem.containsAtLeastScalar(u8, options_arg, 1, '0')) {
                if (!options.is_binary_protocol and !options.is_single_entry) {
                    options.delimiter = 0;
                } else {
                    std.log.err("Binary protocol, null terminator and single entry are mutually exclusive", .{});
                    return error.ConflictingOptions;
                }
            } else if (std.mem.containsAtLeastScalar(u8, options_arg, 1, 'b')) {
                if (options.delimiter != 0 and !options.is_single_entry) {
                    options.is_binary_protocol = true;
                } else {
                    std.log.err("Binary protocol, null terminator and single entry are mutually exclusive", .{});
                    return error.ConflictingOptions;
                }
            } else if (std.mem.containsAtLeastScalar(u8, options_arg, 1, 's')) {
                if (options.delimiter != 0 and !options.is_binary_protocol) {
                    options.is_single_entry = true;
                } else {
                    std.log.err("Binary protocol, null terminator and single entry are mutually exclusive", .{});
                    return error.ConflictingOptions;
                }
            }

            if (std.mem.containsAtLeastScalar(u8, options_arg, 1, 'r')) {
                options.is_reverse_order_output = true;
            }

            arg = args.next() orelse {
                std.log.err(usage, .{});
                return error.MissingArgument;
            };
        }
    }

    return .{ .OptionsAndArg = .{ .options = options, .arg = arg }};
}

pub fn main() !u8 {
    var args = std.process.args();
    _ = args.skip();

    var options: Options = .{};
    const filepath_result = try parseOptionsOrArg(&args, options);
    switch (filepath_result) {
        .Help => {
            std.log.info(help, .{});
            return 0;
        },
        .OptionsAndArg => |result| {
            const filepath = result.arg;
            options = result.options;

            const command_str_result = try parseOptionsOrArg(&args, options);
            switch (command_str_result) {
                .Help => {
                    std.log.info(help, .{});
                    return 0;
                },
                .OptionsAndArg => |command_result| {
                    const command_str = command_result.arg;
                    options = command_result.options;

                    var stdout_buffer: [4096]u8 = undefined;
                    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
                    const stdout = &stdout_writer.interface;

                    var state_machine: StateMachine = .{
                        .stdout = stdout,
                        .delimiter = options.delimiter,
                        .is_binary_protocol = options.is_binary_protocol,
                        .is_single_entry = options.is_single_entry,
                        .is_reverse_order_output = options.is_reverse_order_output,
                    };
                    defer state_machine.close();

                    const wrapper: ArgIteratorWrapper = .{
                        .iterator = &args,
                    };

                    return try processArgs(
                        false,
                        std.heap.smp_allocator,
                        wrapper,
                        command_str,
                        filepath,
                        &state_machine,
                        options,
                    );
                }
            }
        }
    }
}

const CommandError = error{
    InvalidCommand,
};

pub fn parseCommand(
    str: []const u8,
    is_stdin: bool,
) !MessageType {
    if (std.mem.eql(u8, str, "get")) {
        return MessageType.Get;
    } else if (std.mem.eql(u8, str, "get-or-else")) {
        return MessageType.GetOrElse;
    } else if (std.mem.eql(u8, str, "get-or-else-set")) {
        return MessageType.GetOrElseSet;
    } else if (std.mem.eql(u8, str, "set")) {
        return MessageType.Set;
    } else if (std.mem.eql(u8, str, "keys")) {
        return MessageType.Keys;
    } else if (std.mem.eql(u8, str, "key-values")) {
        return MessageType.KeyValues;
    } else if (std.mem.eql(u8, str, "keys-like")) {
        return MessageType.KeysLike;
    } else if (std.mem.eql(u8, str, "delete")) {
        return MessageType.Delete;
    } else if (std.mem.eql(u8, str, "delete-if-exists")) {
        return MessageType.DeleteIfExists;
    } else if (std.mem.eql(u8, str, "stdin")) {
        if (is_stdin) {
            std.log.err("Cannot process \"stdin\" while already reading from stdin", .{});
            return CommandError.InvalidCommand;
        } else {
            return MessageType.Stdin;
        }
    } else {
        std.log.err("Unknown command. Possible commands: get, get-or-else, get-or-else-set, set, keys, key-values, keys-like, delete, delete-if-exists, stdin", .{});
        return CommandError.InvalidCommand;
    }
}

// Temporarily stores the slice in the writer buffer
// Clears the writer before using
fn tempBuffered(
    comptime is_stdin: bool,
    writer: *std.Io.Writer.Allocating,
    slice: []const u8,
) ![]const u8 {
    // Stdin iterator returns slice that is valid only for one iteration
    // Non-stdin (arg) iterator returns slice pointing to the internal buffer, therefore no need to copy again
    if (is_stdin) {
        writer.clearRetainingCapacity();
        _ = try writer.writer.write(slice);
        return writer.written();
    } else {
        return slice;
    }
}

pub fn processArgs(
    comptime is_stdin: bool,
    allocator: std.mem.Allocator,
    args: anytype,
    command_str: [:0]const u8,
    filepath: [:0]const u8,
    state_machine: *StateMachine,
    options: Options,
) !u8 {
    const command = if (is_stdin) command: {
        if (try args.next()) |arg| {
            const command = try parseCommand(arg, is_stdin); 
            break :command command;
        } else {
            std.log.err("Missing a command in the std input", .{});
            return 1;
        }
    } else try parseCommand(command_str, is_stdin);

    var key_buffer = std.io.Writer.Allocating.init(allocator);
    defer {
        key_buffer.deinit();
    }

    switch (command) {
        .Get => {
            var did_receive_valid_arg = false;
            while (try args.next()) |key| {
                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_machine.open(filepath, false);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    return 1;
                }

                try state_machine.process(.{ .Get = key });
            }
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key for get command", .{});
                return 1;
            }
        },
        .GetOrElse => {
            var did_receive_valid_arg = false;
            while (try args.next()) |raw_key| {
                const key = try tempBuffered(is_stdin, &key_buffer, raw_key);

                const value = try args.next() orelse {
                    std.log.err("Missing default value for key \"{s}\"", .{key});
                    return 1;
                };

                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_machine.open(filepath, true);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    return 1;
                }

                try state_machine.process(.{ .GetOrElse = .{ .key = key, .value = value } });
            }
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key value pair for get-or-else command", .{});
                return 1;
            }
        },
        .GetOrElseSet => {
            var did_receive_valid_arg = false;
            while (try args.next()) |raw_key| {
                const key = try tempBuffered(is_stdin, &key_buffer, raw_key);

                const value = try args.next() orelse {
                    std.log.err("Missing default value for key \"{s}\"", .{key});
                    return 1;
                };

                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_machine.open(filepath, true);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    return 1;
                }

                try state_machine.process(.{ .GetOrElseSet = .{ .key = key, .value = value } });
            }
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key value pair for get-or-else-set command", .{});
                return 1;
            }
        },
        .Set => {
            var did_receive_valid_arg = false;
            while (try args.next()) |raw_key| {
                const key = try tempBuffered(is_stdin, &key_buffer, raw_key);

                const value = try args.next() orelse {
                    std.log.err("Missing value for key \"{s}\"", .{key});
                    return 1;
                };

                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_machine.open(filepath, true);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    return 1;
                }

                try state_machine.process(.{ .Set = .{ .key = key, .value = value } });
            }
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key value pair for set command", .{});
                return 1;
            }
        },
        .Keys => {
            if (options.is_single_entry) {
                std.log.err("Key operations are not allowed with single entry flag", .{});
                return 1;
            }

            if (try args.next()) |_| {
                std.log.err("Keys command doesn't accept extra arguments", .{});
                return 1;
            }

            try state_machine.open(filepath, true);
            try state_machine.process(.{ .Keys = undefined });
        },
        .KeyValues => {
            if (options.is_single_entry) {
                std.log.err("Key operations are not allowed with single entry flag", .{});
                return 1;
            }

            if (try args.next()) |_| {
                std.log.err("\"key-values\" command doesn't accept extra arguments", .{});
                return 1;
            }

            try state_machine.open(filepath, true);
            try state_machine.process(.{ .KeyValues = undefined });
        },
        .KeysLike => {
            const raw_pattern = try args.next() orelse {
                std.log.err("Missing pattern for \"keys-like\"", .{});
                return 1;
            };

            const pattern = try tempBuffered(is_stdin, &key_buffer, raw_pattern);

            if (try args.next()) |_| {
                std.log.err("\"keys-like\" command doesn't accept any extra arguments after the pattern", .{});
                return 1;
            }

            try state_machine.open(filepath, true);
            try state_machine.process(.{ .KeysLike = pattern });
        },
        .Delete => {
            var did_receive_valid_arg = false;
            while (try args.next()) |key| {
                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_machine.open(filepath, false);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    return 1;
                }

                try state_machine.process(.{ .Delete = key });
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
                    try state_machine.open(filepath, true);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    return 1;
                }

                try state_machine.process(.{ .DeleteIfExists = key });
            }
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key for \"delete-if-exists\" command", .{});
                return 1;
            }
        },
        .Stdin => {
            if (!is_stdin) {
                // Prepare stdin reader
                var stdin_buffer: [4096]u8 = undefined;
                var stdin_reader = std.fs.File.stdin().reader(&stdin_buffer);
                const stdin = &stdin_reader.interface;
                
                // Copy any leftover command-line args to the next iteration
                // This allows more convenient commands like 'stdin set' or 'stdin set <key>'
                var trailing_args_buffer = try std.ArrayList([]const u8).initCapacity(allocator, 8);
                defer {
                    for (trailing_args_buffer.items) |item| {
                        allocator.free(item);
                    }
                    trailing_args_buffer.deinit(allocator);
                }

                while (try args.next()) |arg| {
                    try trailing_args_buffer.append(allocator, try allocator.dupe(u8, arg));
                }

                var iterator = DelimiterIterator.init(
                    allocator,
                    stdin,
                    trailing_args_buffer.items,
                    .{
                        .delimiter = options.delimiter,
                        .is_binary_protocol = options.is_binary_protocol,
                        .is_single_entry = options.is_single_entry,
                    },
                );
                defer {
                    iterator.deinit();
                }

                return try processArgs(
                    true,
                    allocator,
                    &iterator,
                    command_str,
                    filepath,
                    state_machine,
                    options,
                );
            } else {
                std.log.err("Processing stdin from stdin", .{});
                return 1;
            }
        },
    }

    return 0;
}
