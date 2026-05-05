const std = @import("std");
pub const c = @cImport({
    @cInclude("sqlite3.h");
});

pub const DbError = error{
    FailedToOpenDatabase,
    FailedToCloseDatabase,
    FailedToCreateTable,
    FailedToExecuteQuery,
    FailedToWrite,
    FailedToGetKey,
    FailedToDeleteKey,
    UnexpectedNullEntry,
};

pub const DatabaseStateError = error{
    InvalidState,
};

pub const DatabaseState = enum {
    Initial,
    DatabaseOpen,
    Closed,
};

pub const TokenWriterError = error{SizeTooLarge};

pub fn printTokenWriter(
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

pub const DatabaseStateManager = struct {
    current_state: DatabaseState = .Initial,
    stdout: *std.Io.Writer,
    delimiter: u8,
    is_binary_protocol: bool,
    is_single_entry: bool,
    is_reverse_order_output: bool,
    db: *c.sqlite3 = undefined,

    pub fn open(self: *DatabaseStateManager, filepath: [:0]const u8, allow_create: bool) !void {
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

    pub fn printToken(self: DatabaseStateManager, token: []const u8) !void {
        try printTokenWriter(
            self.stdout,
            self.delimiter,
            self.is_binary_protocol,
            self.is_single_entry,
            token,
        );
    }

    pub fn close(self: *DatabaseStateManager) void {
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
