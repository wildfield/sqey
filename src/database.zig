const std = @import("std");
const c = @import("c");

pub const DbError = error{
    FailedToOpenDatabase,
    FailedToCloseDatabase,
    FailedToCreateTable,
    FailedToExecuteQuery,
    FailedToWrite,
    FailedToGetKey,
    FailedToDeleteKey,
    FailedToRenameKey,
    UnexpectedNullEntry,
    EphemeralDatabaseNotAllowed,
    FailedToPrepareStatement,
};

pub const DatabaseStateError = error{
    InvalidState,
};

pub const DatabaseState = enum {
    Initial,
    DatabaseOpen,
    Closed,
};


pub const DatabaseStateManager = struct {
    current_state: DatabaseState = .Initial,
    is_readonly: bool,
    db: *c.sqlite3 = undefined,

    pub fn open(self: *DatabaseStateManager, filepath: [:0]const u8, allow_create: bool) !void {
        if (self.current_state != .Initial) {
            return DatabaseStateError.InvalidState;
        }
        
        if (filepath.len == 0) {
            return DbError.EphemeralDatabaseNotAllowed;
        }

        errdefer self.current_state = .Closed;

        const db: *c.sqlite3 = db: {
            var db: *c.sqlite3 = undefined;
            var flags: c_int = if (self.is_readonly) c.SQLITE_OPEN_READONLY else c.SQLITE_OPEN_READWRITE;
            if (!self.is_readonly and allow_create) {
                flags |= c.SQLITE_OPEN_CREATE;
            }
            const failure = c.sqlite3_open_v2(filepath, @ptrCast(&db), flags, null);
            errdefer _ = c.sqlite3_close(db);

            if (failure != 0) {
                std.log.err("Failed to open database: {s}", .{c.sqlite3_errmsg(db)});
                std.log.err("Hint: Database must first be created using a \"set\" command if it doesn't exist.", .{});
                return DbError.FailedToOpenDatabase;
            }

            if (!self.is_readonly and allow_create) {
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
                    return DbError.FailedToCreateTable;
                }
            }

            break :db db;
        };

        self.db = db;
        errdefer _ = c.sqlite3_close(self.db);

        self.current_state = .DatabaseOpen;
    }

    pub fn close(self: *DatabaseStateManager) void {
        switch (self.current_state) {
            .Initial => {
                self.current_state = .Closed;
            },
            .Closed => {},
            .DatabaseOpen => {
                _ = c.sqlite3_close(self.db);
                self.current_state = .Closed;
            },
        }
    }
};
