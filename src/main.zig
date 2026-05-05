const std = @import("std");

const database = @import("database.zig");
const handlers = @import("handlers.zig");
const utils = @import("utils.zig");

const DatabaseStateManager = database.DatabaseStateManager;

const Options = utils.Options;
const ProcessArgsError = utils.ProcessArgsError;
const tempBuffered = utils.tempBuffered;

const GetHandler = handlers.GetHandler;
const GetOrElseHandler = handlers.GetOrElseHandler;
const GetOrElseSetHandler = handlers.GetOrElseSetHandler;
const SetHandler = handlers.SetHandler;
const KeysHandler = handlers.KeysHandler;
const KeyValuesHandler = handlers.KeyValuesHandler;
const KeysLikeHandler = handlers.KeysLikeHandler;
const DeleteHandler = handlers.DeleteHandler;
const DeleteIfExistsHandler = handlers.DeleteIfExistsHandler;

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

const Message = union(MessageType) {
    Get: []const u8,
    GetOrElse: utils.KeyValuePair,
    GetOrElseSet: utils.KeyValuePair,
    Set: utils.KeyValuePair,
    Keys: void,
    KeyValues: void,
    KeysLike: []const u8,
    Delete: []const u8,
    DeleteIfExists: []const u8,
    Stdin: void,
};

const ArgIteratorWrapper = struct {
    iterator: *std.process.ArgIterator,

    // This converts ?[]const u8 to !?[]const u8 such that we can use try with it
    // it is needed to make interface consistent between ArgIterator and DelimiterIterator
    pub fn next(self: *const ArgIteratorWrapper) !?[]const u8 {
        return self.iterator.next();
    }
};

const MAX_STDIN_SIZE = 1024 * 1024;

const DelimiterIteratorError = error{
    SizeTooLarge,
    UnexpectedZeroRead,
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
    pub fn next(self: *DelimiterIterator) !?[]const u8 {
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

                // Consume the delimiter if it exists
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
                    // Zero bytes entries are indicative of an issue
                    return DelimiterIteratorError.UnexpectedZeroRead;
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

const OptionsParsingError = error{
    MissingArgument,
    ConflictingOptions,
};

const OptionParsingResultEnum = enum { OptionsAndArg, Help };

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

pub fn main() !void {
    var args = std.process.args();
    _ = args.skip();

    var options: Options = .{};
    const filepath_result = try parseOptionsOrArg(&args, options);
    switch (filepath_result) {
        .Help => {
            std.log.info(help, .{});
            return;
        },
        .OptionsAndArg => |result| {
            const filepath = result.arg;
            options = result.options;

            const command_str_result = try parseOptionsOrArg(&args, options);
            switch (command_str_result) {
                .Help => {
                    std.log.info(help, .{});
                    return;
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

                    try processArgs(
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

pub fn processArgs(
    comptime is_stdin: bool,
    allocator: std.mem.Allocator,
    args: anytype,
    command_str: [:0]const u8,
    filepath: [:0]const u8,
    database_manager: *DatabaseStateManager,
    options: Options,
) !void {
    const command = if (is_stdin) command: {
        if (try args.next()) |arg| {
            const command = try parseCommand(arg, is_stdin);
            break :command command;
        } else {
            std.log.err("Missing a command in the std input", .{});
            return ProcessArgsError.GeneralError;
        }
    } else try parseCommand(command_str, is_stdin);

    switch (command) {
        .Get => {
            var handler: GetHandler = .{};
            defer handler.close();
            try handler.run(is_stdin, allocator, args, filepath, database_manager, options);
        },
        .GetOrElse => {
            var handler: GetOrElseHandler = .{};
            defer handler.close();
            try handler.run(is_stdin, allocator, args, filepath, database_manager, options);
        },
        .GetOrElseSet => {
            var handler: GetOrElseSetHandler = .{};
            defer handler.close(database_manager);
            try handler.run(is_stdin, allocator, args, filepath, database_manager, options);
        },
        .Set => {
            var handler: SetHandler = .{};
            defer handler.close(database_manager);
            try handler.run(is_stdin, allocator, args, filepath, database_manager, options);
        },
        .Keys => {
            if (options.is_single_entry) {
                std.log.err("Key operations are not allowed with single entry flag", .{});
                return ProcessArgsError.GeneralError;
            }

            if (try args.next()) |_| {
                std.log.err("Keys command doesn't accept extra arguments", .{});
                return ProcessArgsError.GeneralError;
            }

            try database_manager.open(filepath, true);
            try KeysHandler.run(database_manager);
        },
        .KeyValues => {
            if (options.is_single_entry) {
                std.log.err("Key operations are not allowed with single entry flag", .{});
                return ProcessArgsError.GeneralError;
            }

            if (try args.next()) |_| {
                std.log.err("\"key-values\" command doesn't accept extra arguments", .{});
                return ProcessArgsError.GeneralError;
            }

            try database_manager.open(filepath, true);
            try KeyValuesHandler.run(database_manager);
        },
        .KeysLike => {
            var key_buffer = std.io.Writer.Allocating.init(allocator);
            defer key_buffer.deinit();

            const raw_pattern = try args.next() orelse {
                std.log.err("Missing pattern for \"keys-like\"", .{});
                return ProcessArgsError.GeneralError;
            };

            const pattern = try tempBuffered(is_stdin, &key_buffer, raw_pattern);

            if (try args.next()) |_| {
                std.log.err("\"keys-like\" command doesn't accept any extra arguments after the pattern", .{});
                return ProcessArgsError.GeneralError;
            }

            try database_manager.open(filepath, true);
            try KeysLikeHandler.run(database_manager, pattern);
        },
        .Delete => {
            var handler: DeleteHandler = .{};
            defer handler.close(database_manager);
            try handler.run(args, filepath, database_manager, options);
        },
        .DeleteIfExists => {
            var handler: DeleteIfExistsHandler = .{};
            defer handler.close(database_manager);
            try handler.run(args, filepath, database_manager, options);
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
                    database_manager,
                    options,
                );
            } else {
                std.log.err("Processing stdin from stdin", .{});
                return ProcessArgsError.GeneralError;
            }
        },
    }
}
