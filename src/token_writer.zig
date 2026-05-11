const utils = @import("utils.zig");
const std = @import("std");

pub const Error = error{SizeTooLarge};

pub fn writeTokenToWriter(
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
            return Error.SizeTooLarge;
        }
        _ = try writer.writeAll(token);
    } else if (is_single_entry) {
        _ = try writer.writeAll(token);
    } else {
        _ = try writer.writeAll(token);
        _ = try writer.writeByte(delimiter);
    }
}

/// Configuration for how tokens are written to stdout.
pub const TokenWriterOptions = struct {
    delimiter: u8,
    is_binary_protocol: bool,
    is_single_entry: bool,
    is_reverse_order_output: bool,

    pub fn fromArgOptions(options: utils.Options) TokenWriterOptions {
        return .{
            .delimiter = options.delimiter,
            .is_binary_protocol = options.is_binary_protocol,
            .is_single_entry = options.is_single_entry,
            .is_reverse_order_output = options.is_reverse_order_output,
        };
    }
};

/// Buffered writer that outputs tokens to stdout.
pub const TokenWriter = struct {
    stdout_buffer: []u8,
    stdout_writer: std.Io.File.Writer,
    options: TokenWriterOptions,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: TokenWriterOptions,
    ) !TokenWriter {
        const stdout_buffer = try allocator.alloc(u8, 64 * 1024);
        const stdout_writer = std.Io.File.stdout().writer(io, stdout_buffer);

        return .{
            .stdout_buffer = stdout_buffer,
            .stdout_writer = stdout_writer,
            .options = options,
        };
    }

    pub fn deinit(self: *TokenWriter, allocator: std.mem.Allocator) void {
        self.stdout_writer.interface.flush() catch {};
        allocator.free(self.stdout_buffer);
    }

    pub fn printToken(self: *TokenWriter, token: []const u8) !void {
        try writeTokenToWriter(
            &self.stdout_writer.interface,
            self.options.delimiter,
            self.options.is_binary_protocol,
            self.options.is_single_entry,
            token,
        );
    }
};
