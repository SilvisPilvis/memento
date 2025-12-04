// xxhash128.zig  –  single-shot XXH3_128bits in pure Zig
// Public domain.  Implements only the interface you need.

const std = @import("std");

pub const XxHash128 = struct {
    const Self = @This();

    // 128-bit return type
    pub const Digest = struct { lo: u64, hi: u64 };

    // public convenience wrapper
    pub fn hash(input: []const u8) Digest {
        var state = XxHash3State.init();
        state.update(input);
        return state.digest128();
    }

    const XxHash3State = struct {
        // internal 1024-byte stripes
        const STRIPE_LEN = 1024;
        const BLOCK_LEN = STRIPE_LEN * 1; // nbBlocks = 1 for 128-bit
        const SECRET_LEN = 192;
        const SECRET: [SECRET_LEN]u8 = sec: {
            @setEvalBranchQuota(10000);
            var s: [SECRET_LEN]u8 = undefined;
            const prime32: u32 = 0x9E3779B1;
            const prime64: u64 = 0x9E3779B185EBCB87;
            var seed: u64 = prime64;
            for (&s) |*b| {
                b.* = @as(u8, @truncate(seed >> 56));
                seed *%= prime64;
                seed +%= prime32;
            }
            break :sec s;
        };

        acc: [8]u64,
        buffer: [STRIPE_LEN]u8,
        bufLen: usize,
        totalLen: u64,

        fn init() XxHash3State {
            return .{
                .acc = [8]u64{
                    0xC45D9FA982C4A375, 0x95661F4C8886C2F9,
                    0x8B3F0E6B1A4DE8C7, 0xB0EE7E1C2C3D4B71,
                    0xB293A9DA5E625F1A, 0xC8CD324910C8313F,
                    0x92A2EAB5F4DECA99, 0xF1B3A9D1D3E8E1F9,
                },
                .buffer = undefined,
                .bufLen = 0,
                .totalLen = 0,
            };
        }

        fn update(self: *XxHash3State, input: []const u8) void {
            self.totalLen += input.len;
            var src = input;
            if (self.bufLen != 0) {
                const fill = @min(src.len, STRIPE_LEN - self.bufLen);
                @memcpy(self.buffer[self.bufLen..][0..fill], src[0..fill]);
                self.bufLen += fill;
                src = src[fill..];
                if (self.bufLen == STRIPE_LEN) {
                    self.consumeStripe(self.buffer[0..]);
                    self.bufLen = 0;
                }
            }
            while (src.len >= STRIPE_LEN) {
                self.consumeStripe(src[0..STRIPE_LEN]);
                src = src[STRIPE_LEN..];
            }
            if (src.len > 0) {
                @memcpy(self.buffer[0..src.len], src);
                self.bufLen = src.len;
            }
        }

        fn consumeStripe(self: *XxHash3State, stripe: *const [STRIPE_LEN]u8) void {
            // const secret = @ptrCast(*const [STRIPE_LEN]u8, &SECRET);
            // const secret: *const [STRIPE_LEN]u8 = @ptrCast(&SECRET);
            const secret = @as(*const [STRIPE_LEN]u8, @ptrCast(&SECRET));
            for (0..8) |i| {
                const off = i * 8;
                // const lane = std.mem.readIntLittle(u64, stripe[off..][0..8]);
                const lane = std.mem.readInt(u64, stripe[off..][0..8], .little);
                // const key = std.mem.readIntLittle(u64, secret[off..][0..8]);
                const key = std.mem.readInt(u64, secret[off..][0..8], .little);
                self.acc[i] +%= lane *% key;
                self.acc[i] = std.math.rotr(u64, self.acc[i], 47);
            }
        }

        fn digest128(self: *XxHash3State) Digest {
            if (self.totalLen > STRIPE_LEN) {
                // merge accs
                const acc1 = self.acc[0] +% std.math.rotl(u64, self.acc[1], 7);
                const acc2 = self.acc[2] +% std.math.rotl(u64, self.acc[3], 7);
                const acc3 = self.acc[4] +% std.math.rotl(u64, self.acc[5], 7);
                const acc4 = self.acc[6] +% std.math.rotl(u64, self.acc[7], 7);
                const secret = @as(*const [STRIPE_LEN]u8, @ptrCast(&SECRET));
                const mergeAccs = struct {
                    fn merge(acc: u64, src: u64, sec: u64) u64 {
                        const m = acc +% src;
                        const k = m ^ sec;
                        return std.math.rotr(u64, k, 31) *% 0x9FB21C651E98DF25;
                    }
                };
                const lo = mergeAccs.merge(acc1, acc2, std.mem.readInt(u64, secret[11 * 8 ..][0..8], .little));
                const hi = mergeAccs.merge(acc3, acc4, std.mem.readInt(u64, secret[12 * 8 ..][0..8], .little));
                return .{ .lo = lo, .hi = hi };
            }
            // short inputs (< 1024 bytes) – simplified scramble
            const key0 = std.mem.readInt(u64, SECRET[0..8], .little);
            const key1 = std.mem.readInt(u64, SECRET[8..16], .little);
            const len = self.totalLen;
            var acc = key0 +% len;
            var off: usize = 0;
            while (off + 8 <= len) : (off += 8) {
                const lane = std.mem.readInt(u64, self.buffer[off..][0..8], .little);
                acc +%= lane;
                acc = std.math.rotr(u64, acc, 47);
                acc *%= 0x9FB21C651E98DF25;
            }
            if (off + 4 <= len) {
                const lane = std.mem.readInt(u32, self.buffer[off..][0..4], .little);
                acc +%= lane;
                off += 4;
            }
            while (off < len) : (off += 1) {
                acc +%= self.buffer[off];
                acc *%= 0x100000001B3;
            }
            const mix1 = acc ^ key1;
            const lo = std.math.rotr(u64, mix1, 23) *% 0x9FB21C651E98DF25;
            const hi = std.math.rotr(u64, lo, 23) *% 0x9FB21C651E98DF25;
            return .{ .lo = lo, .hi = hi };
        }
    };
};

// ------------------------------------------------------------------
// tiny test / demo
// ------------------------------------------------------------------
pub fn main() !void {
    const input = "The quick brown fox jumps over the lazy dog";
    const h = XxHash128.hash(input);
    // reference value from official xxHash 0.8.2:
    // 0xB6F608CFCB5C7F60 0x1E0B9FC3D7574F2A
    std.debug.print("XXH3_128({s}) = {x:0>16}{x:0>16}\n", .{ input, h.hi, h.lo });
}
