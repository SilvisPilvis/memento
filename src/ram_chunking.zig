const std = @import("std");

/// Configuration for RAM chunking algorithm
pub const RamConfig = struct {
    avg_block_size: u64 = 4096,
    max_block_size: u64 = 8192,
    window_size: u64 = 0,

    pub fn init(avg_block_size: u64, max_block_size: u64) RamConfig {
        return .{
            .avg_block_size = avg_block_size,
            .max_block_size = max_block_size,
            .window_size = avg_block_size - 256,
        };
    }
};

/// VRAM-256: RAM Chunking with AVX-256 acceleration
pub const RamChunking = struct {
    config: RamConfig,
    avx256_array: []@Vector(32, u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, config: RamConfig) !RamChunking {
        const num_vectors = config.window_size / 32; // AVX-256 = 32 bytes
        const avx256_array = try allocator.alloc(@Vector(32, u8), num_vectors);

        return .{
            .config = config,
            .avx256_array = avx256_array,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RamChunking) void {
        self.allocator.free(self.avx256_array);
    }

    /// Find the next chunk boundary in the buffer
    pub fn findCutpoint(self: *RamChunking, buff: []const u8) u64 {
        var size = buff.len;

        if (size > self.config.max_block_size) {
            size = self.config.max_block_size;
        } else if (size < self.config.window_size) {
            return size;
        }

        // Find maximum value in the initial window using AVX-256
        const max_value = self.findMaximumAvx256(buff[0..self.config.window_size]);

        // Scan forward from window_size looking for first byte >= max_value
        return self.rangeScanGeqAvx256(buff, self.config.window_size, size, max_value);
    }

    /// Find maximum byte value in a region using AVX-256 instructions
    fn findMaximumAvx256(self: *RamChunking, buff: []const u8) u8 {
        const num_vectors = buff.len / 32;

        // Load contents into SIMD vectors
        for (0..num_vectors) |i| {
            const offset = i * 32;
            var vec: @Vector(32, u8) = undefined;
            inline for (0..32) |j| {
                vec[j] = buff[offset + j];
            }
            self.avx256_array[i] = vec;
        }

        // Perform pairwise maximum reduction
        var step: usize = 2;
        var half_step: usize = 1;

        while (step <= num_vectors) {
            var i: usize = 0;
            while (i < num_vectors) : (i += step) {
                self.avx256_array[i] = @max(self.avx256_array[i], self.avx256_array[i + half_step]);
            }
            half_step = step;
            step = step << 1;
        }

        // Extract final maximum from the remaining vector
        const final_vec = self.avx256_array[0];
        var max_val: u8 = 0;
        inline for (0..32) |i| {
            if (final_vec[i] > max_val) {
                max_val = final_vec[i];
            }
        }

        return max_val;
    }

    /// Scan for first position where byte >= target_value using AVX-256
    fn rangeScanGeqAvx256(self: *RamChunking, buff: []const u8, start_pos: u64, end_pos: u64, target_value: u8) u64 {
        _ = self;
        const num_vectors = (end_pos - start_pos) / 32;
        const target_vec: @Vector(32, u8) = @splat(target_value);

        for (0..num_vectors) |i| {
            const curr_pos = start_pos + (i * 32);

            // Load 32 bytes into SIMD vector
            var vec: @Vector(32, u8) = undefined;
            inline for (0..32) |j| {
                vec[j] = buff[curr_pos + j];
            }

            // Compare: create mask where vec[i] >= target_value
            const cmp_result: @Vector(32, bool) = vec >= target_vec;

            // Find first set bit in comparison mask
            inline for (0..32) |j| {
                if (cmp_result[j]) {
                    return curr_pos + j;
                }
            }
        }

        return end_pos;
    }
};

/// Scalar implementation (non-SIMD) for comparison
pub const RamChunkingScalar = struct {
    config: RamConfig,

    pub fn init(config: RamConfig) RamChunkingScalar {
        return .{ .config = config };
    }

    pub fn findCutpoint(self: *const RamChunkingScalar, buff: []const u8) u64 {
        var size = buff.len;

        if (size > self.config.max_block_size) {
            size = self.config.max_block_size;
        } else if (size < self.config.window_size) {
            return size;
        }

        // Find maximum in window (scalar)
        var max_value: u8 = buff[0];
        for (buff[0..self.config.window_size]) |byte| {
            if (byte >= max_value) {
                max_value = byte;
            }
        }

        // Scan for first byte >= max_value
        for (self.config.window_size..size) |i| {
            if (buff[i] >= max_value) {
                return i;
            }
        }

        return size;
    }
};

// Example usage and testing
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create configuration
    const config = RamConfig.init(8192, 16384);

    // Initialize VRAM-256 chunker
    var chunker = try RamChunking.init(allocator, config);
    defer chunker.deinit();

    // Create test data
    var test_data: [16384]u8 = undefined;
    var prng = std.rand.DefaultPrng.init(42);
    const random = prng.random();
    for (&test_data) |*byte| {
        byte.* = random.int(u8);
    }

    // Find chunk boundary
    const cutpoint = chunker.findCutpoint(&test_data);

    std.debug.print("VRAM-256 RAM Chunking Test\n", .{});
    std.debug.print("Config: avg_size={}, max_size={}, window={}\n", .{
        config.avg_block_size,
        config.max_block_size,
        config.window_size,
    });
    std.debug.print("Chunk boundary found at: {}\n", .{cutpoint});

    // Compare with scalar implementation
    const scalar_chunker = RamChunkingScalar.init(config);
    const scalar_cutpoint = scalar_chunker.findCutpoint(&test_data);
    std.debug.print("Scalar result: {}\n", .{scalar_cutpoint});
    std.debug.print("Results match: {}\n", .{cutpoint == scalar_cutpoint});
}

test "RAM chunking basic functionality" {
    const allocator = std.testing.allocator;
    const config = RamConfig.init(4096, 8192);

    var chunker = try RamChunking.init(allocator, config);
    defer chunker.deinit();

    // Test with small buffer (should return size)
    var small_buf = [_]u8{1} ** 100;
    try std.testing.expectEqual(@as(u64, 100), chunker.findCutpoint(&small_buf));

    // Test with buffer that has clear maximum
    var test_buf = [_]u8{50} ** 8192;
    test_buf[3840] = 255; // Maximum in window
    test_buf[4000] = 255; // First occurrence after window

    const cutpoint = chunker.findCutpoint(&test_buf);
    try std.testing.expect(cutpoint >= config.window_size);
}

test "compare SIMD vs scalar implementation" {
    const allocator = std.testing.allocator;
    const config = RamConfig.init(4096, 8192);

    var simd_chunker = try RamChunking.init(allocator, config);
    defer simd_chunker.deinit();

    const scalar_chunker = RamChunkingScalar.init(config);

    // Test with random data
    var prng = std.rand.DefaultPrng.init(12345);
    const random = prng.random();

    var test_data: [8192]u8 = undefined;
    for (&test_data) |*byte| {
        byte.* = random.int(u8);
    }

    const simd_result = simd_chunker.findCutpoint(&test_data);
    const scalar_result = scalar_chunker.findCutpoint(&test_data);

    try std.testing.expectEqual(scalar_result, simd_result);
}
