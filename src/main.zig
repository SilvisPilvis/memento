const std = @import("std");
const json = std.json;
const s3 = @import("s3");
const fs = std.fs;

const Config = struct {
    root: []const u8,
    blacklist: []const []const u8,
};

const ChunkStore = std.HashMap([32]u8, // SHA-256 hash as key
    []const u8, // chunk data as value
    std.hash_map.AutoContext([32]u8), std.hash_map.default_max_load_percentage);

const FileNode = struct {
    path: []const u8,
    chunks: []ChunkRef,
    size: u64,
    modified: i64,
};

const ChunkRef = struct {
    hash: [32]u8,
    offset: u64, // where in file this chunk starts
    size: u32, // chunk size
};

pub fn main() !void {
    // var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var env_map = try std.process.getEnvMap(gpa.allocator());
    defer env_map.deinit();

    var config: Config = Config{
        .root = "$HOME",
        // .root = "/home/silvestrs",
        .blacklist = &[_][]const u8{ "*.tmp", "node_modules", ".git", ".cache", ".npm" },
    };

    if (config.root.len == 0) {
        fatal("The backup root directory is not set", .{});
    }

    // If the root contains an environment variable, expand it
    var expanded_root: ?[]u8 = null;
    defer if (expanded_root) |root| allocator.free(root);
    if (std.mem.containsAtLeast(u8, config.root, 1, "$")) {
        expanded_root = try expandPath(allocator, config.root);
        config.root = expanded_root.?;
    }

    std.debug.print("Root: {s}\n", .{config.root});

    var dir = try fs.cwd().openDir(config.root, .{ .iterate = true });
    defer dir.close();

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind == .directory and !isBlacklisted(entry.name, config.blacklist)) {
            std.debug.print("{s} - {}\n", .{ entry.name, entry.kind });
        }
    }

    // Initialize client
    // var client = try s3.S3Client.init(allocator, .{
    //     .access_key_id = "your-key",
    //     .secret_access_key = "your-secret",
    //     .region = "us-east-1",
    //     // Optional: Use with MinIO or other S3-compatible services
    //     // .endpoint = "http://localhost:9000",
    // });
    // defer client.deinit();
    //
    // // Create bucket
    // try client.createBucket("my-bucket");
    //
    // // Upload string to a file in bucket
    // var uploader = client.uploader();
    // try uploader.uploadString("my-bucket", "hello.txt", "Hello, S3!");

    // Upload JSON data to bucket
    // const data = .{ .name = "example", .value = 42 };
    // try uploader.uploadJson("my-bucket", "data.json", data);

    // Upload file from file system to bucket
    // try uploader.uploadFile("my-bucket", "backup.zst", "./archive.zst");
}

/// This imports the separate module containing `root.zig`. Take a look in `build.zig` for details.
const lib = @import("zig_memento_lib");

fn matchesPattern(name: []const u8, pattern: []const u8) bool {
    // Simple wildcard matching for *.extension
    if (std.mem.startsWith(u8, pattern, "*.")) {
        const ext = pattern[1..]; // Remove the *
        return std.mem.endsWith(u8, name, ext);
    }
    // Exact match
    return std.mem.eql(u8, name, pattern);
}

fn isBlacklisted(name: []const u8, blacklist: []const []const u8) bool {
    for (blacklist) |pattern| {
        if (matchesPattern(name, pattern)) {
            return true;
        }
    }
    return false;
}

pub fn fatal(comptime fmt: []const u8, args: anytype) noreturn {
    std.log.err(fmt, args);
    std.process.exit(1);
}

pub fn expandPath(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var result = std.ArrayList(u8).init(allocator);
    defer result.deinit();

    var i: usize = 0;
    while (i < path.len) {
        if (path[i] == '$') {
            // Find the end of the variable name
            i += 1;
            const start = i;
            while (i < path.len and (std.ascii.isAlphanumeric(path[i]) or path[i] == '_')) {
                i += 1;
            }

            const var_name = path[start..i];

            // Get the environment variable
            if (std.posix.getenv(var_name)) |value| {
                try result.appendSlice(value);
            } else {
                // Variable not found, keep original
                try result.append('$');
                try result.appendSlice(var_name);
            }
        } else {
            try result.append(path[i]);
            i += 1;
        }
    }

    return result.toOwnedSlice();
}
