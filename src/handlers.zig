const std = @import("std");
const c = @import("c");

const database = @import("database.zig");
const token_writer = @import("token_writer.zig");
const utils = @import("utils.zig");

const DbError = database.DbError;
const DatabaseStateManager = database.DatabaseStateManager;
const TokenWriter = token_writer.TokenWriter;

const KeyValuePair = utils.KeyValuePair;
const Options = utils.Options;
const ProcessArgsError = utils.ProcessArgsError;
const singleEntryFail = utils.singleEntryFail;
const tempBuffered = utils.tempBuffered;

fn prepareStatement(
    db: *c.sqlite3,
    query: []const u8,
) !*c.sqlite3_stmt {
    var statement: ?*c.sqlite3_stmt = null;
    const failure = c.sqlite3_prepare_v2(
        db,
        @ptrCast(query),
        @intCast(query.len),
        &statement,
        null,
    );
    if (failure != c.SQLITE_OK) {
        std.log.err("Failed to compile statement: {s}", .{c.sqlite3_errmsg(db)});
        return DbError.FailedToPrepareStatement;
    }
    if (statement) |ptr| {
        return ptr;
    } else {
        return DbError.FailedToPrepareStatement;
    }
}

fn bindText(
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

fn bindBlob(
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

fn getColumnBlob(statement: *c.sqlite3_stmt, column: c_int) ![]const u8 {
    const blob_ptr: ?[*]const u8 = @ptrCast(c.sqlite3_column_blob(statement, column));
    if (blob_ptr) |ptr| {
        return ptr[0..@intCast(c.sqlite3_column_bytes(statement, column))];
    } else {
        return &.{};
    }
}

// Executes simple statements that do not return values or take parameters.
// `error_message` must take a single `{s}` as a format argument.
fn sqlite3SimpleExec(
    db: *c.sqlite3,
    sql: [*c]const u8,
    comptime error_message: []const u8,
) !void {
    var err_msg: ?[*c]u8 = null;
    const rollback_code = c.sqlite3_exec(db, sql, null, null, @ptrCast(&err_msg));
    if (rollback_code != 0) {
        if (err_msg) |msg| {
            std.log.err(error_message, .{msg});
            c.sqlite3_free(msg);
        } else {
            std.log.err("Unknown sqlite error", .{});
        }
        return DbError.FailedToExecuteQuery;
    }
}

pub const Transaction = struct {
    active: bool = false,

    // Propagates errors
    fn begin(self: *Transaction, sm: *DatabaseStateManager) !void {
        if (!self.active) {
            try sqlite3SimpleExec(sm.db, "BEGIN TRANSACTION", "Failed to begin transaction {s}");
            self.active = true;
        }
    }

    // Swallows errors. Normally used in defer statements
    fn rollback(self: *Transaction, sm: *DatabaseStateManager) void {
        if (self.active) {
            self.active = false;
            sqlite3SimpleExec(sm.db, "ROLLBACK TRANSACTION", "Failed to rollback transaction {s}") catch {};
        }
    }

    // Swallows errors. Normally used in defer statements
    fn commit(self: *Transaction, sm: *DatabaseStateManager) void {
        if (self.active) {
            self.active = false;
            sqlite3SimpleExec(sm.db, "COMMIT TRANSACTION", "Failed to commit transaction {s}") catch {};
        }
    }
};

/// Retrieve values by key. Fails if any key is missing.
pub const GetHandler = struct {
    statement: ?*c.sqlite3_stmt = null,

    fn processStep(self: *GetHandler, sm: *DatabaseStateManager, writer: *TokenWriter, key: []const u8) !void {
        if (self.statement == null) {
            self.statement = try prepareStatement(sm.db, "SELECT value FROM data WHERE key = ?");
        }
        defer {
            const reset_code = c.sqlite3_reset(self.statement.?);
            if (reset_code != c.SQLITE_OK) {
                std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
            }
        }

        try bindText(sm.db, self.statement.?, 1, key);

        const result_code = c.sqlite3_step(self.statement.?);
        if (result_code == c.SQLITE_ROW) {
            try writer.printToken(try getColumnBlob(self.statement.?, 0));
        } else if (result_code == c.SQLITE_DONE) {
            std.log.err("No value found for key \"{s}\"", .{key});
            return DbError.FailedToGetKey;
        } else {
            std.log.err("Failed to read row: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }

    pub fn close(self: *GetHandler) void {
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }

    // Runs the full workflow: read args, open the database if needed, process each step. Close is handled externally.
    pub fn run(
        self: *GetHandler,
        allocator: std.mem.Allocator,
        args: anytype,
        filepath: [:0]const u8,
        sm: *DatabaseStateManager,
        writer: *TokenWriter,
        options: Options,
    ) !void {
        var key_buffer = std.Io.Writer.Allocating.init(allocator);
        defer key_buffer.deinit();

        defer self.close();
        var did_receive_valid_arg = false;
        while (try args.next()) |raw_key| {
            const key = try tempBuffered(options.is_input_stdin, &key_buffer, raw_key);

            if (!did_receive_valid_arg) {
                did_receive_valid_arg = true;
                try sm.open(filepath, options.allow_create);
            } else if (options.is_single_entry) {
                return singleEntryFail();
            }

            try self.processStep(sm, writer, key);
        }
        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key for get command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};

/// Retrieve value, or print a default if missing.
pub const GetOrElseHandler = struct {
    statement: ?*c.sqlite3_stmt = null,

    fn processStep(self: *GetOrElseHandler, sm: *DatabaseStateManager, writer: *TokenWriter, pair: KeyValuePair) !void {
        if (self.statement == null) {
            self.statement = try prepareStatement(sm.db, "SELECT value FROM data WHERE key = ?");
        }
        defer {
            const reset_code = c.sqlite3_reset(self.statement.?);
            if (reset_code != c.SQLITE_OK) {
                std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
            }
        }

        try bindText(sm.db, self.statement.?, 1, pair.key);

        const result_code = c.sqlite3_step(self.statement.?);
        if (result_code == c.SQLITE_ROW) {
            try writer.printToken(try getColumnBlob(self.statement.?, 0));
        } else if (result_code == c.SQLITE_DONE) {
            try writer.printToken(pair.value);
        } else {
            std.log.err("Failed to read row: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }

    pub fn close(self: *GetOrElseHandler) void {
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }

    // Runs the full workflow: read args, open the database if needed, process each step. Close is handled externally.
    pub fn run(
        self: *GetOrElseHandler,
        allocator: std.mem.Allocator,
        args: anytype,
        filepath: [:0]const u8,
        sm: *DatabaseStateManager,
        writer: *TokenWriter,
        options: Options,
    ) !void {
        var key_buffer = std.Io.Writer.Allocating.init(allocator);
        defer key_buffer.deinit();

        defer self.close();
        var did_receive_valid_arg = false;
        while (try args.next()) |raw_key| {
            const key = try tempBuffered(options.is_input_stdin, &key_buffer, raw_key);

            const value = try args.next() orelse {
                std.log.err("Missing default value for key \"{s}\"", .{key});
                return ProcessArgsError.GeneralError;
            };

            if (!did_receive_valid_arg) {
                did_receive_valid_arg = true;
                try sm.open(filepath, options.allow_create);
            } else if (options.is_single_entry) {
                return singleEntryFail();
            }

            try self.processStep(sm, writer, .{ .key = key, .value = value });
        }
        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key value pair for get-or-else command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};

/// Like get-or-else, but also stores the default value in the database.
pub const GetOrElseSetHandler = struct {
    get_statement: ?*c.sqlite3_stmt = null,
    insert_statement: ?*c.sqlite3_stmt = null,
    tx: Transaction = .{},

    fn processStep(self: *GetOrElseSetHandler, sm: *DatabaseStateManager, writer: *TokenWriter, pair: KeyValuePair) !void {
        if (self.get_statement == null) {
            self.get_statement = try prepareStatement(sm.db, "SELECT value FROM data WHERE key = ?");
            // We update the id on conflict to reorder the elements when using key-based operations
            // The latest updated element will be on the bottom. This allows to use for history-like applications
            self.insert_statement = try prepareStatement(
                sm.db,
                "INSERT INTO data (key, value) VALUES (:key, :value) ON CONFLICT(key) DO UPDATE SET id=excluded.id, value=excluded.value",
            );
        }

        defer {
            if (self.get_statement) |stmt| {
                const reset_code = c.sqlite3_reset(stmt);
                if (reset_code != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
                }
            }
            if (self.insert_statement) |stmt| {
                const reset_code = c.sqlite3_reset(stmt);
                if (reset_code != c.SQLITE_OK) {
                    std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
                }
            }
        }

        try bindText(sm.db, self.get_statement.?, 1, pair.key);

        const result_code = c.sqlite3_step(self.get_statement.?);
        if (result_code == c.SQLITE_ROW) {
            try writer.printToken(try getColumnBlob(self.get_statement.?, 0));
        } else if (result_code == c.SQLITE_DONE) {
            try writer.printToken(pair.value);

            try bindText(sm.db, self.insert_statement.?, 1, pair.key);
            try bindBlob(sm.db, self.insert_statement.?, 2, pair.value);
            const extra_result_code = c.sqlite3_step(self.insert_statement.?);

            if (extra_result_code != c.SQLITE_DONE) {
                std.log.err("Failed to insert row: {s}", .{c.sqlite3_errmsg(sm.db)});
                return DbError.FailedToExecuteQuery;
            }
        } else {
            std.log.err("Failed to read row: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }

    pub fn close(self: *GetOrElseSetHandler) void {
        if (self.get_statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
        if (self.insert_statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }

    // Runs the full workflow: read args, open the database if needed, process each step. Close is handled externally.
    pub fn run(
        self: *GetOrElseSetHandler,
        allocator: std.mem.Allocator,
        args: anytype,
        filepath: [:0]const u8,
        sm: *DatabaseStateManager,
        writer: *TokenWriter,
        options: Options,
    ) !void {
        var key_buffer = std.Io.Writer.Allocating.init(allocator);
        defer key_buffer.deinit();

        if (options.is_readonly) {
            std.log.err("Write operation is not allowed in readonly mode", .{});
            return ProcessArgsError.GeneralError;
        }

        defer self.close();
        defer self.tx.commit(sm);
        errdefer self.tx.rollback(sm);
        var did_receive_valid_arg = false;
        while (try args.next()) |raw_key| {
            const key = try tempBuffered(options.is_input_stdin, &key_buffer, raw_key);

            const value = try args.next() orelse {
                std.log.err("Missing default value for key \"{s}\"", .{key});
                return ProcessArgsError.GeneralError;
            };

            if (!did_receive_valid_arg) {
                did_receive_valid_arg = true;
                try sm.open(filepath, true);
            } else if (options.is_single_entry) {
                return singleEntryFail();
            }

            try self.tx.begin(sm);
            try self.processStep(sm, writer, .{ .key = key, .value = value });
        }

        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key value pair for get-or-else-set command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};

/// Insert or update key-value pairs.
pub const SetHandler = struct {
    statement: ?*c.sqlite3_stmt = null,
    tx: Transaction = .{},

    fn processStep(self: *SetHandler, sm: *DatabaseStateManager, pair: KeyValuePair) !void {
        if (self.statement == null) {
            self.statement = try prepareStatement(
                sm.db,
                "INSERT INTO data (key, value) VALUES (:key, :value) ON CONFLICT(key) DO UPDATE SET id=excluded.id, value=excluded.value",
            );
        }

        defer {
            const reset_code = c.sqlite3_reset(self.statement.?);
            if (reset_code != c.SQLITE_OK) {
                std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
            }
        }

        try bindText(sm.db, self.statement.?, 1, pair.key);
        try bindBlob(sm.db, self.statement.?, 2, pair.value);

        const result_code = c.sqlite3_step(self.statement.?);
        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to insert row: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }

    pub fn close(self: *SetHandler) void {
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }

    // Runs the full workflow: read args, open the database if needed, process each step. Close is handled externally.
    pub fn run(
        self: *SetHandler,
        allocator: std.mem.Allocator,
        args: anytype,
        filepath: [:0]const u8,
        sm: *DatabaseStateManager,
        options: Options,
    ) !void {
        var key_buffer = std.Io.Writer.Allocating.init(allocator);
        defer key_buffer.deinit();

        if (options.is_readonly) {
            std.log.err("Write operation is not allowed in readonly mode", .{});
            return ProcessArgsError.GeneralError;
        }

        defer self.close();
        defer self.tx.commit(sm);
        errdefer self.tx.rollback(sm);
        var did_receive_valid_arg = false;
        while (try args.next()) |raw_key| {
            const key = try tempBuffered(options.is_input_stdin, &key_buffer, raw_key);

            const value = try args.next() orelse {
                std.log.err("Missing value for key \"{s}\"", .{key});
                return ProcessArgsError.GeneralError;
            };

            if (!did_receive_valid_arg) {
                did_receive_valid_arg = true;
                try sm.open(filepath, true);
            } else if (options.is_single_entry) {
                return singleEntryFail();
            }

            try self.tx.begin(sm);
            try self.processStep(sm, .{ .key = key, .value = value });
        }

        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key value pair for set command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};

/// List all keys in the database.
pub const KeysHandler = struct {
    // Runs the full workflow. Keys workflows are performed in a single step.
    pub fn run(sm: *DatabaseStateManager, writer: *TokenWriter) !void {
        const order = if (!writer.options.is_reverse_order_output) "ASC" else "DESC";

        const statement_str_pattern = "SELECT key FROM data ORDER BY id {s}";
        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{order});

        const statement: *c.sqlite3_stmt = try prepareStatement(sm.db, statement_str);
        defer _ = c.sqlite3_finalize(statement);

        var result_code = c.sqlite3_step(statement);
        while (result_code == c.SQLITE_ROW) {
            try writer.printToken(std.mem.sliceTo(c.sqlite3_column_text(statement, 0), 0));
            result_code = c.sqlite3_step(statement);
        }

        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to list keys: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }
};

/// List all keys and values in alternating order.
pub const KeyValuesHandler = struct {
    // Runs the full workflow. Keys workflows are performed in a single step.
    pub fn run(sm: *DatabaseStateManager, writer: *TokenWriter) !void {
        const is_reverse = writer.options.is_reverse_order_output;
        const order: []const u8 = if (!is_reverse) "ASC" else "DESC";

        const statement_str_pattern = "SELECT key, value FROM data ORDER BY id {s}";
        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{order});

        const statement: *c.sqlite3_stmt = try prepareStatement(sm.db, statement_str);
        defer _ = c.sqlite3_finalize(statement);

        var result_code = c.sqlite3_step(statement);
        while (result_code == c.SQLITE_ROW) {
            const text = std.mem.sliceTo(c.sqlite3_column_text(statement, 0), 0);
            try writer.printToken(text);
            const blob = try getColumnBlob(statement, 1);
            try writer.printToken(blob);
            result_code = c.sqlite3_step(statement);
        }

        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to list keys: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }
};

/// List keys matching a SQL LIKE pattern.
pub const KeysLikeHandler = struct {
    // Runs the full workflow. Keys workflows are performed in a single step.
    pub fn run(sm: *DatabaseStateManager, writer: *TokenWriter, pattern: []const u8) !void {
        const is_reverse = writer.options.is_reverse_order_output;
        const order: []const u8 = if (!is_reverse) "ASC" else "DESC";

        const statement_str_pattern = "SELECT key FROM data WHERE key LIKE ? ORDER BY id {s}";
        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{order});

        const statement: *c.sqlite3_stmt = try prepareStatement(sm.db, statement_str);
        defer _ = c.sqlite3_finalize(statement);

        try bindText(sm.db, statement, 1, pattern);

        var result_code = c.sqlite3_step(statement);
        while (result_code == c.SQLITE_ROW) {
            try writer.printToken(std.mem.sliceTo(c.sqlite3_column_text(statement, 0), 0));
            result_code = c.sqlite3_step(statement);
        }

        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to list keys: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }
};

/// Delete keys. Fails if any key is missing.
pub const DeleteHandler = struct {
    statement: ?*c.sqlite3_stmt = null,
    tx: Transaction = .{},

    fn processStep(self: *DeleteHandler, sm: *DatabaseStateManager, key: []const u8) !void {
        if (self.statement == null) {
            self.statement = try prepareStatement(sm.db, "DELETE FROM data WHERE key = ?");
        }

        defer {
            const reset_code = c.sqlite3_reset(self.statement.?);
            if (reset_code != c.SQLITE_OK) {
                std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
            }
        }

        try bindText(sm.db, self.statement.?, 1, key);

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

    pub fn close(self: *DeleteHandler) void {
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }

    // Runs the full workflow: read args, open the database if needed, process each step. Close is handled externally.
    pub fn run(
        self: *DeleteHandler,
        args: anytype,
        filepath: [:0]const u8,
        sm: *DatabaseStateManager,
        options: Options,
    ) !void {
        if (options.is_readonly) {
            std.log.err("Write operation is not allowed in readonly mode", .{});
            return ProcessArgsError.GeneralError;
        }

        defer self.close();
        defer self.tx.commit(sm);
        errdefer self.tx.rollback(sm);
        var did_receive_valid_arg = false;
        while (try args.next()) |key| {
            if (!did_receive_valid_arg) {
                did_receive_valid_arg = true;
                try sm.open(filepath, false);
            } else if (options.is_single_entry) {
                return singleEntryFail();
            }

            try self.tx.begin(sm);
            try self.processStep(sm, key);
        }

        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key for delete command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};

/// Delete keys without error if they are missing.
pub const DeleteIfExistsHandler = struct {
    statement: ?*c.sqlite3_stmt = null,
    tx: Transaction = .{},

    fn processStep(self: *DeleteIfExistsHandler, sm: *DatabaseStateManager, key: []const u8) !void {
        if (self.statement == null) {
            self.statement = try prepareStatement(sm.db, "DELETE FROM data WHERE key = ?");
        }

        defer {
            const reset_code = c.sqlite3_reset(self.statement.?);
            if (reset_code != c.SQLITE_OK) {
                std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
            }
        }

        try bindText(sm.db, self.statement.?, 1, key);

        const result_code = c.sqlite3_step(self.statement.?);
        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to delete row: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }

    pub fn close(self: *DeleteIfExistsHandler) void {
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }

    // Runs the full workflow: read args, open the database if needed, process each step. Close is handled externally.
    pub fn run(
        self: *DeleteIfExistsHandler,
        args: anytype,
        filepath: [:0]const u8,
        sm: *DatabaseStateManager,
        options: Options,
    ) !void {
        if (options.is_readonly) {
            std.log.err("Write operation is not allowed in readonly mode", .{});
            return ProcessArgsError.GeneralError;
        }

        defer self.close();
        defer self.tx.commit(sm);
        errdefer self.tx.rollback(sm);
        var did_receive_valid_arg = false;
        while (try args.next()) |key| {
            if (!did_receive_valid_arg) {
                did_receive_valid_arg = true;
                try sm.open(filepath, options.allow_create);
            } else if (options.is_single_entry) {
                return singleEntryFail();
            }

            try self.tx.begin(sm);
            try self.processStep(sm, key);
        }
        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key for \"delete-if-exists\" command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};

/// Rename keys (pairs of old/new names).
pub const RenameHandler = struct {
    statement: ?*c.sqlite3_stmt = null,
    tx: Transaction = .{},

    fn processStep(self: *RenameHandler, sm: *DatabaseStateManager, src: []const u8, dest: []const u8) !void {
        if (self.statement == null) {
            self.statement = try prepareStatement(sm.db, "UPDATE data SET key = ? WHERE key = ?");
        }

        defer {
            const reset_code = c.sqlite3_reset(self.statement.?);
            if (reset_code != c.SQLITE_OK) {
                std.log.err("Failed to reset: {s}", .{c.sqlite3_errmsg(sm.db)});
            }
        }

        try bindText(sm.db, self.statement.?, 1, dest);
        try bindText(sm.db, self.statement.?, 2, src);

        const result_code = c.sqlite3_step(self.statement.?);
        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to rename key: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }

        const row_count = c.sqlite3_changes64(sm.db);
        if (row_count == 0) {
            std.log.err("Failed to rename entry. Source key not found: \"{s}\"", .{src});
            return DbError.FailedToRenameKey;
        }
    }

    pub fn close(self: *RenameHandler) void {
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }

    // Runs the full workflow: read args, open the database if needed, process each step. Close is handled externally.
    pub fn run(
        self: *RenameHandler,
        allocator: std.mem.Allocator,
        args: anytype,
        filepath: [:0]const u8,
        sm: *DatabaseStateManager,
        options: Options,
    ) !void {
        var key_buffer = std.Io.Writer.Allocating.init(allocator);
        defer key_buffer.deinit();

        if (options.is_readonly) {
            std.log.err("Write operation is not allowed in readonly mode", .{});
            return ProcessArgsError.GeneralError;
        }

        defer self.close();
        defer self.tx.commit(sm);
        errdefer self.tx.rollback(sm);
        var did_receive_valid_arg = false;
        while (try args.next()) |raw_src| {
            const src = try tempBuffered(options.is_input_stdin, &key_buffer, raw_src);

            const dest = try args.next() orelse {
                std.log.err("Missing destination key for source key \"{s}\"", .{src});
                return ProcessArgsError.GeneralError;
            };

            if (!did_receive_valid_arg) {
                did_receive_valid_arg = true;
                try sm.open(filepath, options.allow_create);
            } else if (options.is_single_entry) {
                return singleEntryFail();
            }

            try self.tx.begin(sm);
            try self.processStep(sm, src, dest);
        }

        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key pair for rename command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};
