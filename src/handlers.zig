const std = @import("std");

const database = @import("database.zig");
const c = database.c;
const utils = @import("utils.zig");

const DbError = database.DbError;
pub const DatabaseStateManager = database.DatabaseStateManager;

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
// `format` must take a single `{s}` as a format argument.
fn sqlite3SimpleExec(
    db: *c.sqlite3,
    sql: [*c]const u8,
    comptime format: []const u8,
) !void {
    var err_msg: ?[*c]u8 = null;
    const rollback_code = c.sqlite3_exec(db, sql, null, null, @ptrCast(&err_msg));
    if (rollback_code != 0) {
        if (err_msg) |msg| {
            std.log.err(format, .{msg});
            c.sqlite3_free(msg);
        } else {
            std.log.err("Unknown sqlite error", .{});
        }
        return DbError.FailedToExecuteQuery;
    }
}

fn beginTransaction(
    db: *c.sqlite3,
) !void {
    try sqlite3SimpleExec(db, "BEGIN TRANSACTION", "Failed to begin transaction {s}");
}

// Swallows errors, primarily used in defer blocks
fn commitTransaction(
    db: *c.sqlite3,
) void {
    sqlite3SimpleExec(db, "COMMIT TRANSACTION", "Failed to commit transaction {s}") catch {};
}

// Swallows errors, primarily used in defer blocks
fn rollbackTransaction(
    db: *c.sqlite3,
) void {
    sqlite3SimpleExec(db, "ROLLBACK TRANSACTION", "Failed to rollback transaction {s}") catch {};
}

pub const GetHandler = struct {
    statement: ?*c.sqlite3_stmt = null,

    fn processStep(self: *GetHandler, sm: *DatabaseStateManager, key: []const u8) !void {
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
            try sm.printToken(try getColumnBlob(self.statement.?, 0));
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
        comptime is_stdin: bool,
        allocator: std.mem.Allocator,
        args: anytype,
        filepath: [:0]const u8,
        sm: *DatabaseStateManager,
        options: Options,
    ) !void {
        var key_buffer = std.Io.Writer.Allocating.init(allocator);
        defer key_buffer.deinit();

        defer self.close();
        var did_receive_valid_arg = false;
        while (try args.next()) |raw_key| {
            const key = try tempBuffered(is_stdin, &key_buffer, raw_key);

            if (!did_receive_valid_arg) {
                did_receive_valid_arg = true;
                try sm.open(filepath, false);
            } else if (options.is_single_entry) {
                return singleEntryFail();
            }

            try self.processStep(sm, key);
        }
        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key for get command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};

pub const GetOrElseHandler = struct {
    statement: ?*c.sqlite3_stmt = null,

    fn processStep(self: *GetOrElseHandler, sm: *DatabaseStateManager, pair: KeyValuePair) !void {
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
            try sm.printToken(try getColumnBlob(self.statement.?, 0));
        } else if (result_code == c.SQLITE_DONE) {
            try sm.printToken(pair.value);
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
        comptime is_stdin: bool,
        allocator: std.mem.Allocator,
        args: anytype,
        filepath: [:0]const u8,
        sm: *DatabaseStateManager,
        options: Options,
    ) !void {
        var key_buffer = std.Io.Writer.Allocating.init(allocator);
        defer key_buffer.deinit();

        defer self.close();
        var did_receive_valid_arg = false;
        while (try args.next()) |raw_key| {
            const key = try tempBuffered(is_stdin, &key_buffer, raw_key);

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

            try self.processStep(sm, .{ .key = key, .value = value });
        }
        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key value pair for get-or-else command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};

pub const GetOrElseSetHandler = struct {
    get_statement: ?*c.sqlite3_stmt = null,
    insert_statement: ?*c.sqlite3_stmt = null,
    is_transaction_active: bool = false,

    fn begin(self: *GetOrElseSetHandler, sm: *DatabaseStateManager) !void {
        if (!self.is_transaction_active) {
            try beginTransaction(sm.db);
            self.is_transaction_active = true;
        }
    }

    fn processStep(self: *GetOrElseSetHandler, sm: *DatabaseStateManager, pair: KeyValuePair) !void {
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
            try sm.printToken(try getColumnBlob(self.get_statement.?, 0));
        } else if (result_code == c.SQLITE_DONE) {
            try sm.printToken(pair.value);

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

    pub fn rollback(self: *GetOrElseSetHandler, sm: *DatabaseStateManager) void {
        if (self.is_transaction_active) {
            self.is_transaction_active = false;
            rollbackTransaction(sm.db);
        }
    }

    pub fn close(self: *GetOrElseSetHandler, sm: *DatabaseStateManager) void {
        if (self.is_transaction_active) {
            self.is_transaction_active = false;
            commitTransaction(sm.db);
        }
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
        comptime is_stdin: bool,
        allocator: std.mem.Allocator,
        args: anytype,
        filepath: [:0]const u8,
        sm: *DatabaseStateManager,
        options: Options,
    ) !void {
        var key_buffer = std.Io.Writer.Allocating.init(allocator);
        defer key_buffer.deinit();

        defer self.close(sm);
        errdefer self.rollback(sm);
        var did_receive_valid_arg = false;
        while (try args.next()) |raw_key| {
            const key = try tempBuffered(is_stdin, &key_buffer, raw_key);

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

            try self.begin(sm);
            try self.processStep(sm, .{ .key = key, .value = value });
        }

        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key value pair for get-or-else-set command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};

pub const SetHandler = struct {
    statement: ?*c.sqlite3_stmt = null,
    is_transaction_active: bool = false,

    fn begin(self: *SetHandler, sm: *DatabaseStateManager) !void {
        if (!self.is_transaction_active) {
            try beginTransaction(sm.db);
            self.is_transaction_active = true;
        }
    }

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

    pub fn rollback(self: *SetHandler, sm: *DatabaseStateManager) void {

        if (self.is_transaction_active) {
            self.is_transaction_active = false;
            rollbackTransaction(sm.db);
        }
    }

    pub fn close(self: *SetHandler, sm: *DatabaseStateManager) void {
        if (self.is_transaction_active) {
            self.is_transaction_active = false;
            commitTransaction(sm.db);
        }
        if (self.statement) |stmt| {
            _ = c.sqlite3_finalize(stmt);
        }
    }

    // Runs the full workflow: read args, open the database if needed, process each step. Close is handled externally.
    pub fn run(
        self: *SetHandler,
        comptime is_stdin: bool,
        allocator: std.mem.Allocator,
        args: anytype,
        filepath: [:0]const u8,
        sm: *DatabaseStateManager,
        options: Options,
    ) !void {

        var key_buffer = std.Io.Writer.Allocating.init(allocator);
        defer key_buffer.deinit();

        defer self.close(sm);
        errdefer self.rollback(sm);
        var did_receive_valid_arg = false;
        while (try args.next()) |raw_key| {
            const key = try tempBuffered(is_stdin, &key_buffer, raw_key);

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

            try self.begin(sm);
            try self.processStep(sm, .{ .key = key, .value = value });
        }

        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key value pair for set command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};

pub const KeysHandler = struct {
    // Runs the full workflow. Keys workflows are performed in a single step.
    pub fn run(sm: *DatabaseStateManager) !void {
        const order = if (!sm.is_reverse_order_output) "ASC" else "DESC";

        const statement_str_pattern = "SELECT key FROM data ORDER BY id {s}";
        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{order});

        const statement: *c.sqlite3_stmt = try prepareStatement(sm.db, statement_str);
        defer _ = c.sqlite3_finalize(statement);

        var result_code = c.sqlite3_step(statement);
        while (result_code == c.SQLITE_ROW) {
            try sm.printToken(std.mem.sliceTo(c.sqlite3_column_text(statement, 0), 0));
            result_code = c.sqlite3_step(statement);
        }

        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to list keys: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }
};

pub const KeyValuesHandler = struct {
    // Runs the full workflow. Keys workflows are performed in a single step.
    pub fn run(sm: *DatabaseStateManager) !void {
        const is_reverse = sm.is_reverse_order_output;
        const order: []const u8 = if (!is_reverse) "ASC" else "DESC";

        const statement_str_pattern = "SELECT key, value FROM data ORDER BY id {s}";
        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{order});

        const statement: *c.sqlite3_stmt = try prepareStatement(sm.db, statement_str);
        defer _ = c.sqlite3_finalize(statement);

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
    }
};

pub const KeysLikeHandler = struct {
    // Runs the full workflow. Keys workflows are performed in a single step.
    pub fn run(sm: *DatabaseStateManager, pattern: []const u8) !void {
        const is_reverse = sm.is_reverse_order_output;
        const order: []const u8 = if (!is_reverse) "ASC" else "DESC";

        const statement_str_pattern = "SELECT key FROM data WHERE key LIKE ? ORDER BY id {s}";
        var statement_str_buf: [statement_str_pattern.len + 1]u8 = undefined;
        const statement_str = try std.fmt.bufPrint(&statement_str_buf, statement_str_pattern, .{order});

        const statement: *c.sqlite3_stmt = try prepareStatement(sm.db, statement_str);
        defer _ = c.sqlite3_finalize(statement);

        try bindText(sm.db, statement, 1, pattern);

        var result_code = c.sqlite3_step(statement);
        while (result_code == c.SQLITE_ROW) {
            try sm.printToken(std.mem.sliceTo(c.sqlite3_column_text(statement, 0), 0));
            result_code = c.sqlite3_step(statement);
        }

        if (result_code != c.SQLITE_DONE) {
            std.log.err("Failed to list keys: {s}", .{c.sqlite3_errmsg(sm.db)});
            return DbError.FailedToExecuteQuery;
        }
    }
};

pub const DeleteHandler = struct {
    statement: ?*c.sqlite3_stmt = null,
    is_transaction_active: bool = false,

    fn begin(self: *DeleteHandler, sm: *DatabaseStateManager) !void {
        if (!self.is_transaction_active) {
            try beginTransaction(sm.db);
            self.is_transaction_active = true;
        }
    }

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

    pub fn rollback(self: *DeleteHandler, sm: *DatabaseStateManager) void {
        if (self.is_transaction_active) {
            self.is_transaction_active = false;
            rollbackTransaction(sm.db);
        }
    }

    pub fn close(self: *DeleteHandler, sm: *DatabaseStateManager) void {
        if (self.is_transaction_active) {
            self.is_transaction_active = false;
            commitTransaction(sm.db);
        }
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
        defer self.close(sm);
        errdefer self.rollback(sm);
        var did_receive_valid_arg = false;
        while (try args.next()) |key| {
            if (!did_receive_valid_arg) {
                did_receive_valid_arg = true;
                try sm.open(filepath, false);
            } else if (options.is_single_entry) {
                return singleEntryFail();
            }

            try self.begin(sm);
            try self.processStep(sm, key);
        }

        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key for delete command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};

pub const DeleteIfExistsHandler = struct {
    statement: ?*c.sqlite3_stmt = null,
    is_transaction_active: bool = false,

    fn begin(self: *DeleteIfExistsHandler, sm: *DatabaseStateManager) !void {
        if (!self.is_transaction_active) {
            try beginTransaction(sm.db);
            self.is_transaction_active = true;
        }
    }

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

    pub fn rollback(self: *DeleteIfExistsHandler, sm: *DatabaseStateManager) void {
        if (self.is_transaction_active) {
            self.is_transaction_active = false;
            rollbackTransaction(sm.db);
        }
    }

    pub fn close(self: *DeleteIfExistsHandler, sm: *DatabaseStateManager) void {
        if (self.is_transaction_active) {
            self.is_transaction_active = false;
            commitTransaction(sm.db);
        }
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
        defer self.close(sm);
        errdefer self.rollback(sm);
        var did_receive_valid_arg = false;
        while (try args.next()) |key| {
            if (!did_receive_valid_arg) {
                did_receive_valid_arg = true;
                try sm.open(filepath, true);
            } else if (options.is_single_entry) {
                return singleEntryFail();
            }

            try self.begin(sm);
            try self.processStep(sm, key);
        }
        if (!did_receive_valid_arg) {
            std.log.err("Missing at least one key for \"delete-if-exists\" command", .{});
            return ProcessArgsError.GeneralError;
        }
    }
};
