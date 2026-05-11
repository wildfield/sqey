const std = @import("std");

const database = @import("database.zig");
const handlers = @import("handlers.zig");
const utils = @import("utils.zig");

const DatabaseStateManager = database.DatabaseStateManager;

const Options = utils.Options;
const ProcessArgsError = utils.ProcessArgsError;
const tempBuffered = utils.tempBuffered;
const KeyValuePair = utils.KeyValuePair;

const GetHandler = handlers.GetHandler;
const GetOrElseHandler = handlers.GetOrElseHandler;
const GetOrElseSetHandler = handlers.GetOrElseSetHandler;
const SetHandler = handlers.SetHandler;
const KeysHandler = handlers.KeysHandler;
const KeyValuesHandler = handlers.KeyValuesHandler;
const KeysLikeHandler = handlers.KeysLikeHandler;
const DeleteHandler = handlers.DeleteHandler;
const DeleteIfExistsHandler = handlers.DeleteIfExistsHandler;
const RenameHandler = handlers.RenameHandler;

const Command = enum {
    // Retrieve values by key. Fails if any key is missing.
    Get,
    // Retrieve value, or print a default if missing.
    GetOrElse,
    // Like get-or-else, but also stores the default value in the database.
    GetOrElseSet,
    // Insert or update key-value pairs.
    Set,
    // List all keys in the database.
    Keys,
    // List all keys and values in alternating order.
    KeyValues,
    // List keys matching a SQL LIKE pattern.
    KeysLike,
    // Delete keys. Fails if any key is missing.
    Delete,
    // Delete keys without error if they are missing.
    DeleteIfExists,
    // Rename keys (pairs of old/new names).
    Rename,
};

const ArgIteratorWrapper = struct {
    iterator: *std.process.Args.Iterator,

    // This converts "?[:0]const u8" from iterator.next() to !?[]const u8 because we need to use try with it
    // it is needed to make interface consistent between ArgIterator and StdinIterator
    pub fn next(self: ArgIteratorWrapper) !?[]const u8 {
        return self.iterator.next();
    }
};

const MAX_STDIN_SIZE = 1024 * 1024;

const StdinIteratorError = error{
    SizeTooLarge,
};

const StdinIteratorOptions = struct {
    delimiter: u8,
    is_binary_protocol: bool,
    is_single_entry: bool,
};

const StdinIterator = struct {
    reader: *std.Io.Reader,
    delimiter: u8,
    is_binary_protocol: bool,
    is_single_entry: bool,
    is_done: bool = false,
    leftover_args: []const []const u8,
    leftover_args_read_count: usize = 0,
    input_writer: std.Io.Writer.Allocating,

    fn init(allocator: std.mem.Allocator, reader: *std.Io.Reader, leftover_args: []const []const u8, options: StdinIteratorOptions) StdinIterator {
        const input_writer = std.Io.Writer.Allocating.init(allocator);

        return .{
            .reader = reader,
            .delimiter = options.delimiter,
            .is_binary_protocol = options.is_binary_protocol,
            .is_single_entry = options.is_single_entry,
            .leftover_args = leftover_args,
            .input_writer = input_writer,
        };
    }

    fn deinit(self: *StdinIterator) void {
        self.input_writer.deinit();
    }

    // Calling next invalidates the result from previous call, except when the iterator is done
    pub fn next(self: *StdinIterator) !?[]const u8 {
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

                if (number_bytes > MAX_STDIN_SIZE) {
                    std.log.err("The length of token is too long: {d}", .{number_bytes});
                    self.is_done = true;
                    return StdinIteratorError.SizeTooLarge;
                } else if (number_bytes > 0) {
                    try self.reader.streamExact(&self.input_writer.writer, number_bytes);
                    return self.input_writer.written();
                } else {
                    if (self.is_done) {
                        return null;
                    } else {
                        return &.{};
                    }
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
                    if (self.is_done) {
                        return null;
                    } else {
                        return &.{};
                    }
                } else {
                    return self.input_writer.written();
                }
            }
        }
    }
};

const help =
    \\sqey - a simple key-value store backed by sqlite3
    \\
    \\Usage: sqey [options] <database> [options] <command> [args...]
    \\
    \\Commands:
    \\  set               Insert or update key-value pairs
    \\  get               Retrieve values by key. Fails if any key is missing
    \\  get-or-else       Retrieve value, or print a default if missing
    \\  get-or-else-set   Like get-or-else, but also stores the default
    \\  keys              List all keys
    \\  key-values        List all keys and values (alternating)
    \\  keys-like         List keys matching a SQL LIKE pattern
    \\  delete            Delete keys. Fails if any key is missing
    \\  delete-if-exists  Delete keys without error if missing
    \\  rename            Rename keys (pairs of old/new names)
    \\
    \\Options:
    \\  -n                Create the database file if it does not exist
    \\  -o                Open in readonly mode (write commands fail)
    \\  -r                Reverse output order for keys, key-values, etc.
    \\  -0                Use null (\\0) instead of newline as separator
    \\  -b                Use binary format (4-byte unsigned little-endian length prefix per token)
    \\  -s                Single entry mode: treat all input as one value
    \\  -i                Read commands and arguments from stdin. You can pass leading arguments after -i
    \\  -h/--help         Print help
    \\
;

const OptionsParsingError = error{
    MissingArgument,
    ConflictingOptions,
    UnknownFlag,
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

fn printHelp(io: std.Io) void {
    _ = std.Io.File.stderr().writeStreamingAll(io, help) catch {};
}

fn parseOptionsOrArg(
    args: *std.process.Args.Iterator,
    initial_options: Options,
    io: std.Io,
) OptionsParsingError!OptionParsingResult {
    var options = initial_options;

    var arg = args.next() orelse {
        printHelp(io);
        return OptionsParsingError.MissingArgument;
    };

    var is_options = true;

    while (is_options) {
        is_options = arg.len > 0 and arg[0] == '-';

        if (is_options) {
            const options_arg = arg;

            const is_help = std.mem.eql(u8, options_arg, "--help") or std.mem.eql(u8, options_arg, "-h");
            if (is_help) {
                return .{ .Help = undefined };
            }

            if (std.mem.containsAtLeastScalar(u8, options_arg, 1, '0')) {
                if (!options.is_binary_protocol and !options.is_single_entry) {
                    options.delimiter = 0;
                } else {
                    std.log.err("Binary protocol, null terminator and single entry are mutually exclusive", .{});
                    return OptionsParsingError.ConflictingOptions;
                }
            }

            if (std.mem.containsAtLeastScalar(u8, options_arg, 1, 'b')) {
                if (options.delimiter != 0 and !options.is_single_entry) {
                    options.is_binary_protocol = true;
                } else {
                    std.log.err("Binary protocol, null terminator and single entry are mutually exclusive", .{});
                    return OptionsParsingError.ConflictingOptions;
                }
            }

            if (std.mem.containsAtLeastScalar(u8, options_arg, 1, 's')) {
                if (options.delimiter != 0 and !options.is_binary_protocol) {
                    options.is_single_entry = true;
                } else {
                    std.log.err("Binary protocol, null terminator and single entry are mutually exclusive", .{});
                    return OptionsParsingError.ConflictingOptions;
                }
            }

            if (std.mem.containsAtLeastScalar(u8, options_arg, 1, 'r')) {
                options.is_reverse_order_output = true;
            }

            if (std.mem.containsAtLeastScalar(u8, options_arg, 1, 'o')) {
                if (!options.allow_create) {
                    options.is_readonly = true;
                } else {
                    std.log.err("-n and -o are mutually exclusive", .{});
                    return OptionsParsingError.ConflictingOptions;
                }
            }

            if (std.mem.containsAtLeastScalar(u8, options_arg, 1, 'n')) {
                if (!options.is_readonly) {
                    options.allow_create = true;
                } else {
                    std.log.err("-n and -o are mutually exclusive", .{});
                    return OptionsParsingError.ConflictingOptions;
                }
            }

            if (std.mem.containsAtLeastScalar(u8, options_arg, 1, 'i')) {
                options.is_input_stdin = true;
            }

            for (options_arg) |byte| {
                const valid_flags = "-0bsrnoi";
                const is_valid_flag = std.mem.containsAtLeastScalar(u8, valid_flags, 1, byte);
                if (!is_valid_flag) {
                    printHelp(io);
                    return OptionsParsingError.UnknownFlag;
                }
            }

            arg = args.next() orelse {
                printHelp(io);
                return OptionsParsingError.MissingArgument;
            };
        }
    }

    return .{ .OptionsAndArg = .{ .options = options, .arg = arg } };
}

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.iterate(init.minimal.args);
    _ = args.skip();

    var options: Options = .{};
    const filepath_result = try parseOptionsOrArg(&args, options, init.io);
    switch (filepath_result) {
        .Help => {
            printHelp(init.io);
            return;
        },
        .OptionsAndArg => |result| {
            const filepath = result.arg;
            options = result.options;

            const command_str_result = try parseOptionsOrArg(&args, options, init.io);
            switch (command_str_result) {
                .Help => {
                    printHelp(init.io);
                    return;
                },
                .OptionsAndArg => |command_result| {
                    const command_str = command_result.arg;
                    options = command_result.options;

                    var allocator = std.heap.smp_allocator;

                    const stdout_buffer = try allocator.alloc(u8, 64 * 1024);
                    defer allocator.free(stdout_buffer);

                    var stdout_writer = std.Io.File.stdout().writer(init.io, stdout_buffer);
                    const stdout = &stdout_writer.interface;

                    var state_manager: DatabaseStateManager = .{
                        .stdout = stdout,
                        .delimiter = options.delimiter,
                        .is_binary_protocol = options.is_binary_protocol,
                        .is_single_entry = options.is_single_entry,
                        .is_reverse_order_output = options.is_reverse_order_output,
                        .is_readonly = options.is_readonly,
                    };
                    defer state_manager.close();

                    if (options.is_input_stdin) {
                        const stdin_buffer = try allocator.alloc(u8, 64 * 1024);
                        defer allocator.free(stdin_buffer);

                        var stdin_reader = std.Io.File.stdin().reader(init.io, stdin_buffer);
                        const stdin = &stdin_reader.interface;

                        var trailing_args_buffer = try std.ArrayList([]const u8).initCapacity(allocator, 8);
                        defer {
                            for (trailing_args_buffer.items) |item| {
                                allocator.free(item);
                            }
                            trailing_args_buffer.deinit(allocator);
                        }

                        while (args.next()) |arg| {
                            try trailing_args_buffer.append(allocator, try allocator.dupe(u8, arg));
                        }

                        var iterator = StdinIterator.init(
                            allocator,
                            stdin,
                            trailing_args_buffer.items,
                            .{
                                .delimiter = options.delimiter,
                                .is_binary_protocol = options.is_binary_protocol,
                                .is_single_entry = options.is_single_entry,
                            },
                        );
                        defer iterator.deinit();

                        try processArgs(
                            allocator,
                            &iterator,
                            command_str,
                            filepath,
                            &state_manager,
                            options,
                        );
                    } else {
                        const wrapper: ArgIteratorWrapper = .{
                            .iterator = &args,
                        };

                        try processArgs(
                            std.heap.smp_allocator,
                            wrapper,
                            command_str,
                            filepath,
                            &state_manager,
                            options,
                        );
                    }
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
) !Command {
    if (std.mem.eql(u8, str, "get")) {
        return Command.Get;
    } else if (std.mem.eql(u8, str, "get-or-else")) {
        return Command.GetOrElse;
    } else if (std.mem.eql(u8, str, "get-or-else-set")) {
        return Command.GetOrElseSet;
    } else if (std.mem.eql(u8, str, "set")) {
        return Command.Set;
    } else if (std.mem.eql(u8, str, "keys")) {
        return Command.Keys;
    } else if (std.mem.eql(u8, str, "key-values")) {
        return Command.KeyValues;
    } else if (std.mem.eql(u8, str, "keys-like")) {
        return Command.KeysLike;
    } else if (std.mem.eql(u8, str, "delete")) {
        return Command.Delete;
    } else if (std.mem.eql(u8, str, "delete-if-exists")) {
        return Command.DeleteIfExists;
    } else if (std.mem.eql(u8, str, "rename")) {
        return Command.Rename;
    } else {
        std.log.err("Unknown command. Possible commands: get, get-or-else, get-or-else-set, set, keys, key-values, keys-like, delete, delete-if-exists, rename", .{});
        return CommandError.InvalidCommand;
    }
}

pub fn processArgs(
    allocator: std.mem.Allocator,
    args: anytype,
    command_str: [:0]const u8,
    filepath: [:0]const u8,
    database_manager: *DatabaseStateManager,
    options: Options,
) !void {
    const command = try parseCommand(command_str);

    switch (command) {
        .Get => {
            var handler: GetHandler = .{};
            try handler.run(allocator, args, filepath, database_manager, options);
        },
        .GetOrElse => {
            var handler: GetOrElseHandler = .{};
            try handler.run(allocator, args, filepath, database_manager, options);
        },
        .GetOrElseSet => {
            var handler: GetOrElseSetHandler = .{};
            try handler.run(allocator, args, filepath, database_manager, options);
        },
        .Set => {
            var handler: SetHandler = .{};
            try handler.run(allocator, args, filepath, database_manager, options);
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

            try database_manager.open(filepath, options.allow_create);
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

            try database_manager.open(filepath, options.allow_create);
            try KeyValuesHandler.run(database_manager);
        },
        .KeysLike => {
            if (options.is_single_entry) {
                std.log.err("Key operations are not allowed with single entry flag", .{});
                return ProcessArgsError.GeneralError;
            }

            const pattern = try args.next() orelse {
                std.log.err("Missing pattern for \"keys-like\"", .{});
                return ProcessArgsError.GeneralError;
            };

            if (try args.next()) |_| {
                std.log.err("\"keys-like\" command doesn't accept any extra arguments after the pattern", .{});
                return ProcessArgsError.GeneralError;
            }

            try database_manager.open(filepath, options.allow_create);
            try KeysLikeHandler.run(database_manager, pattern);
        },
        .Delete => {
            var handler: DeleteHandler = .{};
            try handler.run(args, filepath, database_manager, options);
        },
        .DeleteIfExists => {
            var handler: DeleteIfExistsHandler = .{};
            try handler.run(args, filepath, database_manager, options);
        },
        .Rename => {
            var handler: RenameHandler = .{};
            try handler.run(allocator, args, filepath, database_manager, options);
        },
    }
}
