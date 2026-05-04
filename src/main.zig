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
    UnexpectedNullEntry,
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

const DatabaseStateError = error{
    InvalidState,
};

const DatabaseState = enum {
    Initial,
    DatabaseOpen,
    Closed,
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

const TokenWriterError = error{SizeTooLarge};

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

// =============================================================================
// DatabaseStateManager — manages DB open/close lifecycle only
// =============================================================================

fn getColumnBlob(statement: *c.sqlite3_stmt, column: c_int) ![]const u8 {
    const blob_ptr: ?[*]const u8 = @ptrCast(c.sqlite3_column_blob(statement, column));
    if (blob_ptr) |ptr| {
        return ptr[0..@intCast(c.sqlite3_column_bytes(statement, column))];
    } else {
        return DbError.UnexpectedNullEntry;
    }
}

const DatabaseStateManager = struct {
    current_state: DatabaseState = .Initial,
    stdout: *std.Io.Writer,
    delimiter: u8,
    is_binary_protocol: bool,
    is_single_entry: bool,
    is_reverse_order_output: bool,
    db: *c.sqlite3 = undefined,

    fn open(self: *DatabaseStateManager, filepath: [:0]const u8, allow_create: bool) !void {
        if (self.current_state != .Initial) {
            return DatabaseStateError.InvalidState;
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
                "CREATE TABLE IF NOT EXISTS data(id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE NOT NULL, value BLOB NOT NULL)",
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

        self.current_state = .DatabaseOpen;
    }

    fn printToken(self: DatabaseStateManager, token: []const u8) !void {
        try printTokenWriter(
            self.stdout,
            self.delimiter,
            self.is_binary_protocol,
            self.is_single_entry,
            token,
        );
    }

    fn close(self: *DatabaseStateManager) void {
        switch (self.current_state) {
            .Initial => {
                self.current_state = .Closed;
            },
            .Closed => {},
            .DatabaseOpen => {
                _ = c.sqlite3_close(self.db);
                self.stdout.flush() catch {};

                self.current_state = .Closed;
            },
        }
    }
};

// =============================================================================
// Handlers — one per message type, each owns its statement lifecycle
// =============================================================================

// --- GetHandler ---
const GetHandler = struct {
    statement: ?*c.sqlite3_stmt = null,

    fn process(self: *GetHandler, sm: *DatabaseStateManager, key: []const u8) !void {
        if (self.statement == null) {
            self.statement = try prepare_statement(sm.db, "SELECT value FROM data WHERE key = ?");
        }
        defer {
            const reset_code = c.sqlite3_reset(self.statement.?);
            if (reset_code != c.SQLITE_OK) {
                std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
            }
        }

        try bind_text(sm.db, self.statement.?, 1, key);

        const result_code = c.sqlite3_step(self.statement.?);
        if (result_code == c.SQLITE_ROW) {
            try sm.printToken(try getColumnBlob(self.statement.?, 0));
        } else if (result_code == c.SQLITE_DONE) {
            std.log.err("No value found for key \"{s}\"", .{key});
            return DbError.FailedToGetKey;
        } else {
            std.log.err("Failed to read row: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }

    fn close(self: *GetHandler) void {
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }
};

// --- GetOrElseHandler ---
const GetOrElseHandler = struct {
    statement: ?*c.sqlite3_stmt = null,

    fn process(self: *GetOrElseHandler, sm: *DatabaseStateManager, pair: KeyValuePair) !void {
        if (self.statement == null) {
            self.statement = try prepare_statement(sm.db, "SELECT value FROM data WHERE key = ?");
        }
        defer {
            const reset_code = c.sqlite3_reset(self.statement.?);
            if (reset_code != c.SQLITE_OK) {
                std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
            }
        }

        try bind_text(sm.db, self.statement.?, 1, pair.key);

        const result_code = c.sqlite3_step(self.statement.?);
        if (result_code == c.SQLITE_ROW) {
            try sm.printToken(try getColumnBlob(self.statement.?, 0));
        } else if (result_code == c.SQLITE_DONE) {
            try sm.printToken(pair.value);
        } else {
            std.log.err("Failed to read row: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }

    fn close(self: *GetOrElseHandler) void {
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }
};

// --- GetOrElseSetHandler ---
const GetOrElseSetHandler = struct {
    statement: ?*c.sqlite3_stmt = null,
    extra_statement: ?*c.sqlite3_stmt = null,

    fn process(self: *GetOrElseSetHandler, sm: *DatabaseStateManager, pair: KeyValuePair) !void {
        if (self.statement == null) {
            self.statement = try prepare_statement(sm.db, "SELECT value FROM data WHERE key = ?");
            self.extra_statement = try prepare_statement(
                sm.db,
                "INSERT INTO data (key, value) VALUES (:key, :value) ON CONFLICT(key) DO UPDATE SET id=excluded.id, value=excluded.value",
            );

            var begin_err_msg: [*c]u8 = undefined;
            const begin_code = c.sqlite3_exec(sm.db, "BEGIN TRANSACTION", null, null, &begin_err_msg);
            if (begin_code != 0) {
                std.log.err("Failed to begin transaction {s}", .{begin_err_msg});
                c.sqlite3_free(begin_err_msg);
                return DbError.FailedToExecuteQuery;
            }
        }

        defer {
            if (self.statement) |stmt| {
                const reset_code = c.sqlite3_reset(stmt);
                if (reset_code != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
                    // cannot return error from defer, transaction will be rolled back on close
                }
            }
            if (self.extra_statement) |stmt| {
                const reset_code = c.sqlite3_reset(stmt);
                if (reset_code != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
                }
            }
        }

        try bind_text(sm.db, self.statement.?, 1, pair.key);

        const result_code = c.sqlite3_step(self.statement.?);
        if (result_code == c.SQLITE_ROW) {
            try sm.printToken(try getColumnBlob(self.statement.?, 0));
        } else if (result_code == c.SQLITE_DONE) {
            try sm.printToken(pair.value);

            try bind_text(sm.db, self.extra_statement.?, 1, pair.key);
            try bind_blob(sm.db, self.extra_statement.?, 2, pair.value);
            const extra_result_code = c.sqlite3_step(self.extra_statement.?);

            if (extra_result_code != c.SQLITE_DONE) {
                std.log.err("Failed to insert row: {s}", .{c.sqlite3_errmsg(sm.db)});
                return DbError.FailedToExecuteQuery;
            }
        } else {
            std.log.err("Failed to read row: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }

    fn close(self: *GetOrElseSetHandler) void {
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
        if (self.extra_statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }
};

// --- SetHandler ---
const SetHandler = struct {
    statement: ?*c.sqlite3_stmt = null,

    fn process(self: *SetHandler, sm: *DatabaseStateManager, pair: KeyValuePair) !void {
        if (self.statement == null) {
            self.statement = try prepare_statement(
                sm.db,
                "INSERT INTO data (key, value) VALUES (:key, :value) ON CONFLICT(key) DO UPDATE SET id=excluded.id, value=excluded.value",
            );

            var begin_err_msg: [*c]u8 = undefined;
            const begin_code = c.sqlite3_exec(sm.db, "BEGIN TRANSACTION", null, null, &begin_err_msg);
            if (begin_code != 0) {
                std.log.err("Failed to begin transaction {s}", .{begin_err_msg});
                c.sqlite3_free(begin_err_msg);
                return DbError.FailedToExecuteQuery;
            }
        }

        defer {
            const reset_code = c.sqlite3_reset(self.statement.?);
            if (reset_code != c.SQLITE_OK) {
                std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
            }
        }

        try bind_text(sm.db, self.statement.?, 1, pair.key);
        try bind_blob(sm.db, self.statement.?, 2, pair.value);

        const result_code = c.sqlite3_step(self.statement.?);
        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to insert row: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }

    fn close(self: *SetHandler) void {
        // finalize() implicitly commits the active transaction
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }
};

// --- KeysHandler ---
const KeysHandler = struct {
    fn process(sm: *DatabaseStateManager) !void {
        const order = if (!sm.is_reverse_order_output) "ASC" else "DESC";

        const statement_str_pattern = "SELECT key FROM data ORDER BY id {s}";
        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{order});

        const statement: *c.sqlite3_stmt = try prepare_statement(sm.db, statement_str);
        errdefer _ = c.sqlite3_finalize(statement);

        var result_code = c.sqlite3_step(statement);
        while (result_code == c.SQLITE_ROW) {
            try sm.printToken(std.mem.sliceTo(c.sqlite3_column_text(statement, 0), 0));
            result_code = c.sqlite3_step(statement);
        }

        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to list keys: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }

        _ = c.sqlite3_finalize(statement);
    }
};

// --- KeyValuesHandler ---
const KeyValuesHandler = struct {
    fn process(sm: *DatabaseStateManager) !void {
        const is_reverse = sm.is_reverse_order_output;
        const order: []const u8 = if (!is_reverse) "ASC" else "DESC";

        const statement_str_pattern = "SELECT key, value FROM data ORDER BY id {s}";
        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{order});

        const statement: *c.sqlite3_stmt = try prepare_statement(sm.db, statement_str);
        errdefer _ = c.sqlite3_finalize(statement);

        var result_code = c.sqlite3_step(statement);
        while (result_code == c.SQLITE_ROW) {
            const text = std.mem.sliceTo(c.sqlite3_column_text(statement, 0), 0);
            try sm.printToken(text);
            const blob = try getColumnBlob(statement, 1);
            try sm.printToken(blob);
            result_code = c.sqlite3_step(statement);
        }

        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to list keys: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }

        _ = c.sqlite3_finalize(statement);
    }
};

// --- KeysLikeHandler ---
const KeysLikeHandler = struct {
    fn process(sm: *DatabaseStateManager, pattern: []const u8) !void {
        const is_reverse = sm.is_reverse_order_output;
        const order: []const u8 = if (!is_reverse) "ASC" else "DESC";

        const statement_str_pattern = "SELECT key FROM data WHERE key LIKE ? ORDER BY id {s}";
        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{order});

        const statement: *c.sqlite3_stmt = try prepare_statement(sm.db, statement_str);
        errdefer _ = c.sqlite3_finalize(statement);

        try bind_text(sm.db, statement, 1, pattern);

        var result_code = c.sqlite3_step(statement);
        while (result_code == c.SQLITE_ROW) {
            try sm.printToken(std.mem.sliceTo(c.sqlite3_column_text(statement, 0), 0));
            result_code = c.sqlite3_step(statement);
        }

        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to list keys: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }

        _ = c.sqlite3_finalize(statement);
    }
};

// --- DeleteHandler ---
const DeleteHandler = struct {
    statement: ?*c.sqlite3_stmt = null,

    fn process(self: *DeleteHandler, sm: *DatabaseStateManager, key: []const u8) !void {
        if (self.statement == null) {
            self.statement = try prepare_statement(sm.db, "DELETE FROM data WHERE key = ?");

            var begin_err_msg: [*c]u8 = undefined;
            const begin_code = c.sqlite3_exec(sm.db, "BEGIN TRANSACTION", null, null, &begin_err_msg);
            if (begin_code != 0) {
                std.log.err("Failed to begin transaction {s}", .{begin_err_msg});
                c.sqlite3_free(begin_err_msg);
                return DbError.FailedToExecuteQuery;
            }
        }

        defer {
            const reset_code = c.sqlite3_reset(self.statement.?);
            if (reset_code != c.SQLITE_OK) {
                std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
            }
        }

        try bind_text(sm.db, self.statement.?, 1, key);

        const result_code = c.sqlite3_step(self.statement.?);
        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to delete row: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }

        const row_count = c.sqlite3_changes64(sm.db);
        if (row_count == 0) {
            std.log.err("Failed to delete entry. Key not found: \"{s}\"", .{key});
            return DbError.FailedToDeleteKey;
        }
    }

    fn close(self: *DeleteHandler) void {
        // finalize() implicitly commits the active transaction
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }
};

// --- DeleteIfExistsHandler ---
const DeleteIfExistsHandler = struct {
    statement: ?*c.sqlite3_stmt = null,

    fn process(self: *DeleteIfExistsHandler, sm: *DatabaseStateManager, key: []const u8) !void {
        if (self.statement == null) {
            self.statement = try prepare_statement(sm.db, "DELETE FROM data WHERE key = ?");

            var begin_err_msg: [*c]u8 = undefined;
            const begin_code = c.sqlite3_exec(sm.db, "BEGIN TRANSACTION", null, null, &begin_err_msg);
            if (begin_code != 0) {
                std.log.err("Failed to begin transaction {s}", .{begin_err_msg});
                c.sqlite3_free(begin_err_msg);
                return DbError.FailedToExecuteQuery;
            }
        }

        defer {
            const reset_code = c.sqlite3_reset(self.statement.?);
            if (reset_code != c.SQLITE_OK) {
                std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
                // cannot return error from defer
            }
        }

        try bind_text(sm.db, self.statement.?, 1, key);

        const result_code = c.sqlite3_step(self.statement.?);
        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to delete row: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }

    fn close(self: *DeleteIfExistsHandler) void {
        // finalize() implicitly commits the active transaction
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
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

const MAX_STDIN_SIZE = 1024 * 1024;

const DelimiterIteratorError = error{SizeTooLarge};

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
                } else {}
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

                if (read_bytes == 0) {} else {
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

const OptionsParsingError = error{
    MissingArgument,
    ConflictingOptions,
};

const OptionParsingResultEnum = enum { OptionsAndArg, Help };

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

    return .{ .OptionsAndArg = .{ .options = options, .arg = arg } };
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

                    var state_manager: DatabaseStateManager = .{
                        .stdout = stdout,
                        .delimiter = options.delimiter,
                        .is_binary_protocol = options.is_binary_protocol,
                        .is_single_entry = options.is_single_entry,
                        .is_reverse_order_output = options.is_reverse_order_output,
                    };
                    defer state_manager.close();

                    const wrapper: ArgIteratorWrapper = .{
                        .iterator = &args,
                    };

                    return try processArgs(
                        false,
                        std.heap.smp_allocator,
                        wrapper,
                        command_str,
                        filepath,
                        &state_manager,
                        options,
                    );
                },
            }
        },
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
    state_manager: *DatabaseStateManager,
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
            var handler: GetHandler = .{};
            var did_receive_valid_arg = false;
            while (try args.next()) |key| {
                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_manager.open(filepath, false);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    handler.close();
                    return 1;
                }

                try handler.process(state_manager, key);
            }
            handler.close();
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key for get command", .{});
                return 1;
            }
        },
        .GetOrElse => {
            var handler: GetOrElseHandler = .{};
            var did_receive_valid_arg = false;
            while (try args.next()) |raw_key| {
                const key = try tempBuffered(is_stdin, &key_buffer, raw_key);

                const value = try args.next() orelse {
                    std.log.err("Missing default value for key \"{s}\"", .{key});
                    handler.close();
                    return 1;
                };

                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_manager.open(filepath, true);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    handler.close();
                    return 1;
                }

                try handler.process(state_manager, .{ .key = key, .value = value });
            }
            handler.close();
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key value pair for get-or-else command", .{});
                return 1;
            }
        },
        .GetOrElseSet => {
            var handler: GetOrElseSetHandler = .{};
            var did_receive_valid_arg = false;
            while (try args.next()) |raw_key| {
                const key = try tempBuffered(is_stdin, &key_buffer, raw_key);

                const value = try args.next() orelse {
                    std.log.err("Missing default value for key \"{s}\"", .{key});
                    handler.close();
                    return 1;
                };

                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_manager.open(filepath, true);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    handler.close();
                    return 1;
                }

                try handler.process(state_manager, .{ .key = key, .value = value });
            }
            handler.close();
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key value pair for get-or-else-set command", .{});
                return 1;
            }
        },
        .Set => {
            var handler: SetHandler = .{};
            var did_receive_valid_arg = false;
            while (try args.next()) |raw_key| {
                const key = try tempBuffered(is_stdin, &key_buffer, raw_key);

                const value = try args.next() orelse {
                    std.log.err("Missing value for key \"{s}\"", .{key});
                    handler.close();
                    return 1;
                };

                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_manager.open(filepath, true);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    handler.close();
                    return 1;
                }

                try handler.process(state_manager, .{ .key = key, .value = value });
            }
            handler.close();
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

            try state_manager.open(filepath, true);
            try KeysHandler.process(state_manager);
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

            try state_manager.open(filepath, true);
            try KeyValuesHandler.process(state_manager);
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

            try state_manager.open(filepath, true);
            try KeysLikeHandler.process(state_manager, pattern);
        },
        .Delete => {
            var handler: DeleteHandler = .{};
            var did_receive_valid_arg = false;
            while (try args.next()) |key| {
                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_manager.open(filepath, false);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    handler.close();
                    return 1;
                }

                try handler.process(state_manager, key);
            }
            handler.close();
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key for delete command", .{});
                return 1;
            }
        },
        .DeleteIfExists => {
            var handler: DeleteIfExistsHandler = .{};
            var did_receive_valid_arg = false;
            while (try args.next()) |key| {
                if (!did_receive_valid_arg) {
                    did_receive_valid_arg = true;
                    try state_manager.open(filepath, true);
                } else if (options.is_single_entry) {
                    std.log.err("Only single input/output key is allowed with single entry flag", .{});
                    handler.close();
                    return 1;
                }

                try handler.process(state_manager, key);
            }
            handler.close();
            if (!did_receive_valid_arg) {
                std.log.err("Missing at least one key for \"delete-if-exists\" command", .{});
                return 1;
            }
        },
        .Stdin => {
            if (!is_stdin) {
                var stdin_buffer: [4096]u8 = undefined;
                var stdin_reader = std.fs.File.stdin().reader(&stdin_buffer);
                const stdin = &stdin_reader.interface;

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
                    state_manager,
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
