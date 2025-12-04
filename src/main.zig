const std = @import("std");
const json = std.json;
const s3 = @import("s3");
const fs = std.fs;
const ram = @import("ram_chunking.zig");
const xxh = @import("xxhash128.zig");
const progress = @import("progress.zig");

const Config = struct {
    root: []const u8,
    blacklist: []const []const u8,
    output_dir: []const u8,
    chunk_extension: []const u8,
    repo_dir: []const u8,
    index_file: []const u8,
};

const ChunkStore = std.HashMap([16]u8, // SHA-256 hash as key
    []const u8, // chunk data as value
    std.hash_map.AutoContext([16]u8), std.hash_map.default_max_load_percentage);

const FileNode = struct {
    path: []const u8,
    chunks: []ChunkRef,
    size: u64,
    modified: i64,
};

const ChunkRef = struct {
    hash: [16]u8,
    offset: u64, // where in file this chunk starts
    size: u32, // chunk size
};

// Repository index to track which chunks exist
const ChunkIndex = std.HashMap([16]u8, bool, std.hash_map.AutoContext([16]u8), std.hash_map.default_max_load_percentage);

// Snapshot represents a backup at a point in time
const Snapshot = struct {
    timestamp: i64,
    files: []FileNode,
    total_size: u64,
    unique_chunks: u32,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var config: Config = Config{
        // .root = "$HOME",
        .root = "/home/silvestrs/Desktop/projects/zig-memento/backup",
        .blacklist = &[_][]const u8{ "*.tmp", "node_modules", ".git", ".cache", ".npm" },
        .output_dir = "backup_chunks",
        .chunk_extension = ".chunk",
        .repo_dir = "backup_repo",
        .index_file = "chunk_index.json",
    };

    // Initialize repository directories
    try initializeRepository(config);

    // Load existing chunk index for deduplication
    var chunk_index = try loadChunkIndex(allocator, config);
    defer chunk_index.deinit();

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
            const file_node = try processFileIntoChunks(allocator, &chunk_store, &chunk_index, file_path, entry.name);
            try file_nodes.append(file_node);
        }
    }

    std.debug.print("\nBackup Summary:\n", .{});
    std.debug.print("Files processed: {}\n", .{file_nodes.items.len});
    std.debug.print("Unique chunks stored: {}\n", .{chunk_store.count()});

    // Upload chunks to S3 (mock implementation for now)
    std.debug.print("\nSaving chunks to disk...\n", .{});

    // Save chunks to disk and update index
    try saveNewChunks(allocator, &chunk_store, &chunk_index, config);

    //Make chunk_store into iterator
    var chunk_iterator = chunk_store.iterator();
    // Store number of uploaded chunks
    var uploaded_count: u32 = 0;

    var bar = try progress.ProgressBar.init(allocator, chunk_store.count());
    bar.schema = "Backing up: [:bar] :percent% | :elapsed elapsed | ETA: :eta";
    bar.width = 30;

    // While there are more chunks to upload
    while (chunk_iterator.next()) |entry| {
        if (entry.value_ptr.*.len == 0) continue;

        // 1. hash the chunk *data* with xxHash128
        const digest = xxh.XxHash128.hash(entry.value_ptr.*); // key is the chunk bytes
        // 2. turn the 128-bit value into a 32-char hex string
        var hash_buf: [32]u8 = undefined;
        const hash_hex = try std.fmt.bufPrint(&hash_buf, "{x:0>16}{x:0>16}", .{ digest.hi, digest.lo });
        // 3. use first 16 hex chars for the file name (same as your old [0..8])
        const file_name = hash_hex[0..16];

        // std.debug.print("Writing chunk {s} (size: {} bytes)\n", .{ file_name, entry.value_ptr.*.len });

        // build full path
        const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}{s}", .{ config.output_dir, file_name, config.chunk_extension });
        defer allocator.free(file_path);

        // write chunk
        const file = try std.fs.cwd().createFile(file_path, .{});
        defer file.close();
        try file.writeAll(entry.value_ptr.*);

        uploaded_count += 1;
        try bar.tick(1);
    }

    std.debug.print("Write complete! {} chunks written to {s}/\n", .{ uploaded_count, config.output_dir });

    // Save updated chunk index
    try saveChunkIndex(allocator, &chunk_index, config);

    // Create and save snapshot
    const snapshot = Snapshot{
        .timestamp = std.time.timestamp(),
        .files = file_nodes.items,
        .total_size = calculateTotalSize(file_nodes.items),
        .unique_chunks = @intCast(chunk_index.count()),
    };
    try saveSnapshot(allocator, &snapshot, config);
}

// TODO: Change from fixed size chunks to fastCDC
// Process a file into chunks and store them in ChunkStore
/// Process a file into RAM-chunked segments and store them in ChunkStore
fn processFileIntoChunks(
    allocator: std.mem.Allocator,
    chunk_store: *ChunkStore,
    chunk_index: *ChunkIndex,
    file_path: []const u8,
    file_name: []const u8,
    // _chunk_size: u32, // ignored – RamChunking drives the size
) !FileNode {
    // Open and read the whole file
    const file = try std.fs.openFileAbsolute(file_path, .{});
    defer file.close();

    const file_size = try file.getEndPos();
    const file_content = try allocator.alloc(u8, file_size);
    defer allocator.free(file_content);
    _ = try file.readAll(file_content);

    // Initialise RAM chunker (sizes in bytes)
    const ram_config = ram.RamConfig.init(4096, 16384); // avg 4 KB, max 16 KB
    var chunker = try ram.RamChunking.init(allocator, ram_config);
    defer chunker.deinit();

    // Build chunk list
    var chunks = std.ArrayList(ChunkRef).init(allocator);
    defer chunks.deinit();

    var offset: u64 = 0;
    while (offset < file_size) {
        const remaining = file_content[offset..];
        const cut = chunker.findCutpoint(remaining); // bytes to take this round
        const chunk_data = remaining[0..cut];

        // xxhash128 hash of the *logical* chunk
        const digest = xxh.XxHash128.hash(chunk_data);
        var hash: [16]u8 = undefined;
        std.mem.writeInt(u64, hash[0..8], digest.lo, .little);
        std.mem.writeInt(u64, hash[8..16], digest.hi, .little);
        // Zero out the remaining 16 bytes to make it a full 32-byte array
        // @memset(hash[16..32], 0);
        // std.crypto.hash.sha2.Sha256.hash(chunk_data, &hash, .{});

        // Check if chunk already exists globally (from previous backups)
        const existed_globally = chunk_index.contains(hash);
        const existed_in_current = chunk_store.contains(hash);

        if (!existed_globally and !existed_in_current) {
            // Completely new chunk - store it and mark in index
            const owned = try allocator.dupe(u8, chunk_data);
            try chunk_store.put(hash, owned);
            try chunk_index.put(hash, true);
            std.debug.print("  New chunk stored (offset {}, size {})\n", .{ offset, chunk_data.len });
        } else if (existed_globally and !existed_in_current) {
            // Chunk exists from previous backup but not in current run
            std.debug.print("  Existing chunk found (offset {}, size {}) – already backed up\n", .{ offset, chunk_data.len });
        } else {
            // Duplicate within current backup
            std.debug.print("  Duplicate chunk found (offset {}, size {}) – skipped!\n", .{ offset, chunk_data.len });
        }

        try chunks.append(.{
            .hash = hash,
            .offset = offset,
            .size = @intCast(chunk_data.len),
        });

        offset += cut;
    }

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

// Initialize repository structure
fn initializeRepository(config: Config) !void {
    // Create repository directory
    std.fs.cwd().makeDir(config.repo_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    // Create chunks directory
    std.fs.cwd().makeDir(config.output_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
}

// Load chunk index from disk
fn loadChunkIndex(allocator: std.mem.Allocator, config: Config) !ChunkIndex {
    var chunk_index = ChunkIndex.init(allocator);

    const index_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ config.repo_dir, config.index_file });
    defer allocator.free(index_path);

    const file = std.fs.cwd().openFile(index_path, .{}) catch |err| switch (err) {
        error.FileNotFound => {
            std.debug.print("No existing chunk index found, starting fresh\n", .{});
            return chunk_index;
        },
        else => return err,
    };
    defer file.close();

    const file_size = try file.getEndPos();
    const content = try allocator.alloc(u8, file_size);
    defer allocator.free(content);
    _ = try file.readAll(content);

    // Parse JSON and populate chunk_index
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, content, .{});
    defer parsed.deinit();

    if (parsed.value.object.get("chunks")) |chunks_value| {
        var chunks_iter = chunks_value.object.iterator();
        while (chunks_iter.next()) |entry| {
            var hash: [16]u8 = undefined;
            _ = try std.fmt.hexToBytes(&hash, entry.key_ptr.*);
            try chunk_index.put(hash, true);
        }
    }

    std.debug.print("Loaded {} existing chunks from index\n", .{chunk_index.count()});
    return chunk_index;
}

// Save chunk index to disk
fn saveChunkIndex(allocator: std.mem.Allocator, chunk_index: *ChunkIndex, config: Config) !void {
    const index_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ config.repo_dir, config.index_file });
    defer allocator.free(index_path);

    const file = try std.fs.cwd().createFile(index_path, .{});
    defer file.close();

    try file.writeAll("{\n  \"chunks\": {\n");

    var iter = chunk_index.iterator();
    var first = true;
    while (iter.next()) |entry| {
        if (!first) try file.writeAll(",\n");
        first = false;

        const hash_hex = try std.fmt.allocPrint(allocator, "{}", .{std.fmt.fmtSliceHexLower(&entry.key_ptr.*)});
        defer allocator.free(hash_hex);

        const line = try std.fmt.allocPrint(allocator, "    \"{s}\": true", .{hash_hex});
        defer allocator.free(line);
        try file.writeAll(line);
    }

    try file.writeAll("\n  }\n}\n");
}

// Save new chunks to disk
fn saveNewChunks(allocator: std.mem.Allocator, chunk_store: *ChunkStore, chunk_index: *ChunkIndex, config: Config) !void {
    var chunk_iterator = chunk_store.iterator();
    var saved_count: u32 = 0;

    while (chunk_iterator.next()) |entry| {
        const hash_hex = try std.fmt.allocPrint(allocator, "{}", .{std.fmt.fmtSliceHexLower(&entry.key_ptr.*)});
        defer allocator.free(hash_hex);

        const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}{s}", .{ config.output_dir, hash_hex, config.chunk_extension });
        defer allocator.free(file_path);

        const file = try std.fs.cwd().createFile(file_path, .{});
        defer file.close();
        try file.writeAll(entry.value_ptr.*);

        // Update chunk index to track this chunk
        try chunk_index.put(entry.key_ptr.*, true);

        saved_count += 1;
        std.debug.print("Saved chunk {} (size: {} bytes)\n", .{ hash_hex[0..8], entry.value_ptr.*.len });
    }

    std.debug.print("Saved {} new chunks to disk\n", .{saved_count});
}

// Save snapshot metadata
fn saveSnapshot(allocator: std.mem.Allocator, snapshot: *const Snapshot, config: Config) !void {
    const timestamp_str = try std.fmt.allocPrint(allocator, "{}", .{snapshot.timestamp});
    defer allocator.free(timestamp_str);

    const snapshot_path = try std.fmt.allocPrint(allocator, "{s}/snapshot_{s}.json", .{ config.repo_dir, timestamp_str });
    defer allocator.free(snapshot_path);

    const file = try std.fs.cwd().createFile(snapshot_path, .{});
    defer file.close();

    // Write snapshot as JSON (simplified version)
    const snapshot_json = try std.fmt.allocPrint(allocator,
        \\{{
        \\  "timestamp": {},
        \\  "total_size": {},
        \\  "unique_chunks": {},
        \\  "file_count": {}
        \\}}
    , .{ snapshot.timestamp, snapshot.total_size, snapshot.unique_chunks, snapshot.files.len });
    defer allocator.free(snapshot_json);

    try file.writeAll(snapshot_json);
    std.debug.print("Snapshot saved: {s}\n", .{snapshot_path});
}

// Calculate total size of all files
fn calculateTotalSize(files: []const FileNode) u64 {
    var total: u64 = 0;
    for (files) |file| {
        total += file.size;
    }
    return total;
}

// TODO: Implement S3 upload functionality
// fn uploadChunkToS3(bucket: []const u8, hash: [32]u8, chunk_data: []const u8) !void {
//     // Initialize S3 client
//     // Upload chunk with hash as key
//     // Handle errors appropriately
// }
