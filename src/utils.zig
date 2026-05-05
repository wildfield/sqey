const std = @import("std");

pub const KeyValuePair = struct {
    key: []const u8,
    value: []const u8,
};

pub const Options = struct {
    delimiter: u8 = '\n',
    is_binary_protocol: bool = false,
    is_reverse_order_output: bool = false,
    is_single_entry: bool = false,
};

pub const ProcessArgsError = error{
    GeneralError,
};

pub fn singleEntryFail() ProcessArgsError {
    std.log.err("Only single input/output key is allowed with single entry flag", .{});
    return ProcessArgsError.GeneralError;
}

pub fn tempBuffered(
    comptime is_stdin: bool,
    writer: *std.Io.Writer.Allocating,
    slice: []const u8,
) ![]const u8 {
    // Stdin iterator returns slice that is valid only for one iteration.
    // Non-stdin (arg) iterator returns slice pointing to the internal buffer,
    // therefore no need to copy again.
    if (is_stdin) {
        writer.clearRetainingCapacity();
        _ = try writer.writer.write(slice);
        return writer.written();
    } else {
        return slice;
    }
}
