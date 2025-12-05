const std = @import("std");
// An error type for Zstd operations
const ZstdError = error{
    CompressionFailed,
    DecompressionFailed,
    ContentSizeUnknown,
};

// Import the Zstd C functions.
// This assumes the linker found the header files through the build.zig setup.
const c = @cImport({
    @cInclude("zstd.h");
});

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const stdout = std.io.getStdOut().writer();

    const data_to_compress = "The quick brown fox jumps over the lazy dog." ** 100;
    const source: []const u8 = data_to_compress;

    try stdout.print("Original Data Size: {d} bytes\n", .{source.len});

    // --- Compression ---
    const compressed_data = try compress(allocator, source, 1);
    defer allocator.free(compressed_data);

    try stdout.print("Compressed Data Size: {d} bytes\n", .{compressed_data.len});

    const ratio = @as(f64, @floatFromInt(source.len)) / @as(f64, @floatFromInt(compressed_data.len));
    try stdout.print("Compression Ratio: {d:.2}:1\n\n", .{ratio});

    // --- Decompression ---
    const decompressed_data = try decompress(allocator, compressed_data);
    defer allocator.free(decompressed_data);

    try stdout.print("Decompressed Data Size: {d} bytes\n", .{decompressed_data.len});

    // --- Verification ---
    if (!std.mem.eql(u8, source, decompressed_data)) {
        return error.VerificationFailed;
    }

    try stdout.print("Verification successful: Decompressed data matches original!\n", .{});
}

/// Compresses a slice of bytes using Zstandard's simple API.
/// The caller is responsible for freeing the returned slice's memory.
pub fn compress(
    allocator: std.mem.Allocator,
    source: []const u8,
    level: c_int, // Zstd compression level (e.g., 1 for fast, 3 for default, 22 for max)
) ![]u8 {
    // 1. Calculate the maximum required size for the compressed output buffer.
    // ZSTD_compressBound() provides a safe upper limit.
    const max_c_size = c.ZSTD_compressBound(source.len);

    if (max_c_size == 0) {
        // ZSTD_compressBound returns 0 if source.len is too large.
        return ZstdError.CompressionFailed;
    }

    // 2. Allocate the destination buffer.
    const compressed_buffer = try allocator.alloc(u8, max_c_size);

    // 3. Perform the compression.
    // ZSTD_compress returns the actual compressed size or an error code.
    const actual_c_size = c.ZSTD_compress(
        compressed_buffer.ptr, // dst: pointer to the output buffer
        compressed_buffer.len, // dstCapacity: max size of the output buffer
        source.ptr, // src: pointer to the source data
        source.len, // srcSize: size of the source data
        level, // compressionLevel: 1-22, or negative for fast levels
    );

    // 4. Check for errors and slice the result.
    if (c.ZSTD_isError(actual_c_size) != 0) {
        // Free the buffer on failure.
        allocator.free(compressed_buffer);
        std.debug.print("Zstd compression error: {s}\n", .{c.ZSTD_getErrorName(actual_c_size)});
        return ZstdError.CompressionFailed;
    }

    // Reallocate to the exact size to avoid memory management issues
    const result = try allocator.alloc(u8, @intCast(actual_c_size));
    @memcpy(result, compressed_buffer[0..@intCast(actual_c_size)]);
    allocator.free(compressed_buffer);
    return result;
}

/// Decompresses a slice of Zstandard-compressed bytes.
/// The caller is responsible for freeing the returned slice's memory.
pub fn decompress(
    allocator: std.mem.Allocator,
    compressed: []const u8,
) ![]u8 {
    // 1. Determine the original (decompressed) content size.
    // The ZSTD_compress() function embeds the content size in the frame header.
    const d_size = c.ZSTD_getFrameContentSize(
        compressed.ptr,
        compressed.len,
    );

    // Check for special return values.
    if (d_size == c.ZSTD_CONTENTSIZE_ERROR) {
        return ZstdError.DecompressionFailed;
    }
    if (d_size == c.ZSTD_CONTENTSIZE_UNKNOWN) {
        // You would use the streaming API for content size unknown, but for simple
        // compression (ZSTD_compress), this should not happen.
        return ZstdError.ContentSizeUnknown;
    }

    // Convert the u64 size to a usize (safe on 64-bit platforms, check on 32-bit).
    const decompressed_capacity: usize = @intCast(d_size);

    // 2. Allocate the destination buffer.
    const decompressed_buffer = try allocator.alloc(u8, decompressed_capacity);

    // 3. Perform the decompression.
    const actual_d_size = c.ZSTD_decompress(
        decompressed_buffer.ptr, // dst: pointer to the output buffer
        decompressed_buffer.len, // dstCapacity: exact size of the output data
        compressed.ptr, // src: pointer to the compressed data
        compressed.len, // compressedSize: size of the compressed data
    );

    // 4. Check for errors.
    if (c.ZSTD_isError(actual_d_size) != 0) {
        allocator.free(decompressed_buffer);
        std.debug.print("Zstd decompression error: {s}\n", .{c.ZSTD_getErrorName(actual_d_size)});
        return ZstdError.DecompressionFailed;
    }

    // This should match the capacity, but it's safer to use the return value.
    return decompressed_buffer[0..@intCast(actual_d_size)];
}
