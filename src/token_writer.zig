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

pub const TokenWriter = struct {
    stdout_buffer: []u8,
    stdout_writer: std.Io.File.Writer,
    delimiter: u8,
    is_binary_protocol: bool,
    is_single_entry: bool,
    is_reverse_order_output: bool,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        delimiter: u8,
        is_binary_protocol: bool,
        is_single_entry: bool,
        is_reverse_order_output: bool,
    ) !TokenWriter {
        const stdout_buffer = try allocator.alloc(u8, 64 * 1024);
        const stdout_writer = std.Io.File.stdout().writer(io, stdout_buffer);

        return .{
            .stdout_buffer = stdout_buffer,
            .stdout_writer = stdout_writer,
            .delimiter = delimiter,
            .is_binary_protocol = is_binary_protocol,
            .is_single_entry = is_single_entry,
            .is_reverse_order_output = is_reverse_order_output,
        };
    }

    pub fn deinit(self: *TokenWriter, allocator: std.mem.Allocator) void {
        self.stdout_writer.interface.flush() catch {};
        allocator.free(self.stdout_buffer);
    }

    pub fn printToken(self: *TokenWriter, token: []const u8) !void {
        try writeTokenToWriter(
            &self.stdout_writer.interface,
            self.delimiter,
            self.is_binary_protocol,
            self.is_single_entry,
            token,
        );
    }
};
