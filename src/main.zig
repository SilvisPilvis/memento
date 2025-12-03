const std = @import("std");
const json = std.json;
const s3 = @import("s3");
const fs = std.fs;

const Config = struct {
    root: []const u8,
    blacklist: []const []const u8,
    output_dir: []const u8,
    chunk_extension: []const u8,
    chunk_size: u32,
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

// const ChunkSize = enum(i32) {
//     KB = 1024,
//     MB = 1024 * 1024,
//     GB = 1024 * 1024 * 1024,
// };

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // const chunk_size = ChunkSize.KB * 4;
    const chunk_size = 1024 * 4;

    var config: Config = Config{
        // .root = "$HOME",
        .root = "/home/silvestrs/Desktop/projects/zig-memento/backup",
        .blacklist = &[_][]const u8{ "*.tmp", "node_modules", ".git", ".cache", ".npm" },
        .output_dir = "backup_chunks",
        .chunk_extension = ".chunk",
        .chunk_size = chunk_size,
    };

    // Chunk store that stores chunks of files
    var chunk_store = ChunkStore.init(allocator);
    defer {
        // Clean up allocated chunk data
        var iterator = chunk_store.iterator();
        while (iterator.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
        chunk_store.deinit();
    }

    // Array to store file nodes
    var file_nodes = std.ArrayList(FileNode).init(allocator);
    defer {
        for (file_nodes.items) |file_node| {
            allocator.free(file_node.path);
            allocator.free(file_node.chunks);
        }
        file_nodes.deinit();
    }

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

    // Walk through directories until there are no more directories
    // 1. List dir
    // 2. Backup files in current directory
    // 3. Go deeper into the directory
    // 4. Repeat

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        // Skip directories that are blacklisted
        if (entry.kind == .directory and !isBlacklisted(entry.name, config.blacklist)) {
            std.debug.print("{s} - {}\n", .{ entry.name, entry.kind });
            // TODO: Recursively process subdirectories
        } else if (entry.kind == .file and !isBlacklisted(entry.name, config.blacklist)) {
            std.debug.print("Processing file: {s}\n", .{entry.name});

            // Build full file path
            const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ config.root, entry.name });
            defer allocator.free(file_path);

            // Process the file into chunks
            const file_node = try processFileIntoChunks(allocator, &chunk_store, file_path, entry.name, config.chunk_size);
            try file_nodes.append(file_node);
        }
    }

    std.debug.print("\nBackup Summary:\n", .{});
    std.debug.print("Files processed: {}\n", .{file_nodes.items.len});
    std.debug.print("Unique chunks stored: {}\n", .{chunk_store.count()});

    // Upload chunks to S3 (mock implementation for now)
    std.debug.print("\nSaving chunks to disk...\n", .{});

    // Create the output directory if it doesn't exist
    std.fs.cwd().makeDir(config.output_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {}, // Directory already exists, that's fine
        else => return err,
    };

    //Make chunk_store into iterator
    var chunk_iterator = chunk_store.iterator();
    // Store number of uploaded chunks
    var uploaded_count: u32 = 0;
    // While there are more chunks to upload
    while (chunk_iterator.next()) |entry| {
        // Skip chunks that are already uploaded
        if (entry.value_ptr.*.len == 0) continue;

        // Hash the chunk key
        const hash_hex = try std.fmt.allocPrint(allocator, "{}", .{std.fmt.fmtSliceHexLower(&entry.key_ptr.*)});
        defer allocator.free(hash_hex);

        // const tmp_chunk = hash_hex[0..8];
        // std.debug.print("Writing chunk {s} (size: {} bytes)\n", .{ tmp_chunk, entry.value_ptr.*.len });

        // We use {s} to format the chunk hash as a string
        std.debug.print("Writing chunk {s} (size: {} bytes)\n", .{ hash_hex[0..8], entry.value_ptr.*.len });

        // Create full filepath with directory and filename
        const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}{s}", .{ config.output_dir, hash_hex, config.chunk_extension });
        defer allocator.free(file_path);

        // Write chunk to file
        const file = try std.fs.cwd().createFile(file_path, .{});
        defer file.close();
        try file.writeAll(entry.value_ptr.*);

        uploaded_count += 1;
    }

    std.debug.print("Write complete! {} chunks written to {s}/\n", .{ uploaded_count, config.output_dir });
}

// TODO: Change from fixed size chunks to fastCDC
// Process a file into chunks and store them in ChunkStore
fn processFileIntoChunks(allocator: std.mem.Allocator, chunk_store: *ChunkStore, file_path: []const u8, file_name: []const u8, chunk_size: u32) !FileNode {
    // Open and read the file
    const file = try std.fs.openFileAbsolute(file_path, .{});
    defer file.close();

    const file_size = try file.getEndPos();
    const file_content = try allocator.alloc(u8, file_size);
    defer allocator.free(file_content);
    _ = try file.readAll(file_content);

    // Split file into chunks (4KB chunks - good balance for most use cases)
    var chunks = std.ArrayList(ChunkRef).init(allocator);
    defer chunks.deinit();

    var offset: u64 = 0;
    while (offset < file_content.len) {
        const chunk_end = @min(offset + chunk_size, file_content.len);
        const chunk_data = file_content[offset..chunk_end];

        // Create SHA-256 hash for the chunk
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(chunk_data, &hash, .{});

        // Store chunk only if it's new (deduplication)
        const chunk_existed = chunk_store.contains(hash);
        if (!chunk_existed) {
            // TODO: Compress the chunk data before storing it

            const owned_chunk = try allocator.dupe(u8, chunk_data);
            try chunk_store.put(hash, owned_chunk);
            std.debug.print("  New chunk stored (offset {}, size {})\n", .{ offset, chunk_data.len });
        } else {
            std.debug.print("  Duplicate chunk found (offset {}, size {}) - skipped!\n", .{ offset, chunk_data.len });
        }

        // Create chunk reference for the file
        try chunks.append(ChunkRef{
            .hash = hash,
            .offset = offset,
            .size = @intCast(chunk_data.len),
        });

        offset = chunk_end;
    }

    // Return FileNode with all chunk references
    return FileNode{
        .path = try allocator.dupe(u8, file_name),
        .chunks = try chunks.toOwnedSlice(),
        .size = file_size,
        .modified = std.time.timestamp(),
    };
}

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

// pub fn fastCDC() {

// }

// TODO: Implement S3 upload functionality
// fn uploadChunkToS3(bucket: []const u8, hash: [32]u8, chunk_data: []const u8) !void {
//     // Initialize S3 client
//     // Upload chunk with hash as key
//     // Handle errors appropriately
// }
