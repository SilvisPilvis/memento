const std = @import("std");
const json = std.json;
const s3 = @import("s3");
const fs = std.fs;
const ram = @import("ram_chunking.zig");
const xxh = @import("xxhash128.zig");
const progress = @import("progress.zig");
const zstd = @import("zstd.zig");

const Config = struct {
    root: []const u8,
    config_path: []const u8,
    blacklist: []const []const u8,
    output_dir: []const u8,
    chunk_extension: []const u8,
    repo_dir: []const u8,
    index_file: []const u8,
    compression_level: i32,
    no_compression: bool,
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

    // Parse command line arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var compression_level: i32 = 1; // Default compression level
    var no_compression: bool = false;

    // Parse command line arguments
    var i: usize = 1;
    while (i < args.len) {
        if (std.mem.eql(u8, args[i], "--no-compression") or std.mem.eql(u8, args[i], "--no-comp")) {
            no_compression = true;
            std.debug.print("Compression disabled\n", .{});
        } else if (std.mem.eql(u8, args[i], "--help") or std.mem.eql(u8, args[i], "-h")) {
            std.debug.print("Usage: zig_memento [compression_level] [--no-compression]\n", .{});
            std.debug.print("  compression_level: Integer from -131072 to 22 (default: 1)\n", .{});
            std.debug.print("  --no-compression, --no-comp: Disable compression entirely\n", .{});
            std.process.exit(0);
        } else {
            // Try to parse as compression level
            compression_level = std.fmt.parseInt(i32, args[i], 10) catch |err| switch (err) {
                error.InvalidCharacter => blk: {
                    std.debug.print("Error: Invalid argument '{s}'. Use --help for usage.\n", .{args[i]});
                    break :blk 1;
                },
                error.Overflow => blk: {
                    std.debug.print("Error: Compression level '{s}' is too large. Using default level 1.\n", .{args[i]});
                    break :blk 1;
                },
            };

            // Clamp compression level to valid zstd range (-131072 to 22)
            if (compression_level < -131072) {
                std.debug.print("Warning: Compression level {} is too low, using -131072.\n", .{compression_level});
                compression_level = -131072;
            } else if (compression_level > 22) {
                std.debug.print("Warning: Compression level {} is too high, using 22.\n", .{compression_level});
                compression_level = 22;
            }
        }
        i += 1;
    }

    if (no_compression) {
        std.debug.print("Using no compression\n", .{});
    } else {
        std.debug.print("Using compression level: {}\n", .{compression_level});
    }

    var config: Config = undefined;

    if (try configExists(allocator, "/home/silvestrs/Desktop/projects/zig-memento/config.json")) {
        // Read the config to struct
        config = try loadConfig(allocator, "/home/silvestrs/Desktop/projects/zig-memento/config.json");
        std.debug.print("Config exists\n Repo dir: {s}\n", .{config.repo_dir});
    } else {
        // Create the config file with default values
        std.debug.print("Config does not exist\n", .{});
        config = Config{
            // .root = "$HOME",
            .root = "/home/silvestrs/Desktop/projects/zig-memento/backup",
            .config_path = "/home/silvestrs/Desktop/projects/zig-memento",
            // .config_path = "$HOME/.config/zig-memento/config.json",
            .blacklist = &[_][]const u8{ "*.tmp", "node_modules", ".git", ".cache", ".npm" },
            .output_dir = "backup_chunks",
            .chunk_extension = ".chunk",
            .repo_dir = "backup_repo",
            .index_file = "chunk_index.json",
            .compression_level = compression_level,
            .no_compression = no_compression,
        };
        try saveConfig(allocator, config);
    }

    // std.process.exit(0);

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
    var file_nodes = try std.ArrayList(FileNode).initCapacity(allocator, 0);
    defer {
        for (file_nodes.items) |file_node| {
            allocator.free(file_node.path);
            allocator.free(file_node.chunks);
        }
        file_nodes.deinit(allocator);
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
        // If the entry is a directory and not blacklisted
        if (entry.kind == .directory and !isBlacklisted(entry.name, config.blacklist)) {
            // std.debug.print("{s} - {any}\n", .{ entry.name, entry.kind });
            // TODO: Recursively process subdirectories
        } else if (entry.kind == .file and !isBlacklisted(entry.name, config.blacklist)) {
            std.debug.print("Processing file: {s}\n", .{entry.name});

            // Build full file path
            const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ config.root, entry.name });
            defer allocator.free(file_path);

            // Process the file into chunks
            const file_node = try processFileIntoChunks(allocator, &chunk_store, &chunk_index, file_path, entry.name, config);
            try file_nodes.append(allocator, file_node);
        }
    }

    // std.debug.print("\nBackup Summary:\n", .{});
    // std.debug.print("Files processed: {}\n", .{file_nodes.items.len});
    // std.debug.print("Unique chunks stored: {}\n", .{chunk_store.count()});

    // Upload chunks to S3 (mock implementation for now)
    std.debug.print("\nSaving chunks to disk...\n", .{});

    // Save chunks to disk and update index
    try saveNewChunks(allocator, &chunk_store, &chunk_index, config);

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
    config: Config,
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
    var chunks = try std.ArrayList(ChunkRef).initCapacity(allocator, 0);
    defer chunks.deinit(allocator);

    var offset: u64 = 0;
    while (offset < file_size) {
        const remaining = file_content[offset..];
        const cut = chunker.findCutpoint(remaining); // bytes to take this round
        const chunk_data = remaining[0..cut];

        const final_chunk = if (config.no_compression) chunk_data else blk: {
            const compressed_chunk = try zstd.compress(allocator, chunk_data, config.compression_level);
            break :blk compressed_chunk;
        };
        defer if (!config.no_compression) allocator.free(final_chunk);

        // xxhash128 hash of the *logical* chunk
        const digest = xxh.XxHash128.hash(chunk_data);
        var hash: [16]u8 = undefined;
        std.mem.writeInt(u64, hash[0..8], digest.lo, .little);
        std.mem.writeInt(u64, hash[8..16], digest.hi, .little);

        // Check if chunk already exists globally (from previous backups)
        const existed_globally = chunk_index.contains(hash);
        const existed_in_current = chunk_store.contains(hash);

        if (!existed_globally and !existed_in_current) {
            // Completely new chunk - store it and mark in index
            const owned = try allocator.dupe(u8, final_chunk);
            try chunk_store.put(hash, owned);
            try chunk_index.put(hash, true);
            // if (config.no_compression) {
            //     std.debug.print("  New chunk stored (offset {}, size {}) - uncompressed\n", .{ offset, chunk_data.len });
            // } else {
            //     std.debug.print("  New chunk stored (offset {}, size {}) - compressed from {} to {} bytes\n", .{ offset, chunk_data.len, chunk_data.len, final_chunk.len });
            // }
        } else if (existed_globally and !existed_in_current) {
            // Chunk exists from previous backup but not in current run
            // std.debug.print("  Existing chunk found (offset {}, size {}) – already backed up\n", .{ offset, chunk_data.len });
        } else {
            // Duplicate within current backup
            // std.debug.print("  Duplicate chunk found (offset {}, size {}) – skipped!\n", .{ offset, chunk_data.len });
        }

        try chunks.append(allocator, .{
            .hash = hash,
            .offset = offset,
            .size = @intCast(chunk_data.len),
        });

        offset += cut;
    }

    return FileNode{
        .path = try allocator.dupe(u8, file_name),
        .chunks = try chunks.toOwnedSlice(allocator),
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
    var result = try std.ArrayList(u8).initCapacity(allocator, 0);
    defer result.deinit(allocator);

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
                try result.appendSlice(allocator, value);
            } else {
                // Variable not found, keep original
                try result.append(allocator, '$');
                try result.appendSlice(allocator, var_name);
            }
        } else {
            try result.append(allocator, path[i]);
            i += 1;
        }
    }

    return result.toOwnedSlice(allocator);
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

        const hash_hex = try std.fmt.allocPrint(allocator, "{x}", .{entry.key_ptr.*});
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

    // Initialize progress bar
    var bar = try progress.ProgressBar.init(allocator, chunk_store.count());
    bar.schema = "Saving chunks: [:bar] :percent% | :elapsed elapsed | ETA: :eta";
    // bar.width = 20;

    while (chunk_iterator.next()) |entry| {
        const hash_hex = try std.fmt.allocPrint(allocator, "{x}", .{entry.key_ptr.*});
        defer allocator.free(hash_hex);

        const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}{s}", .{ config.output_dir, hash_hex, config.chunk_extension });
        defer allocator.free(file_path);

        const file = try std.fs.cwd().createFile(file_path, .{});
        defer file.close();
        try file.writeAll(entry.value_ptr.*);

        // Update chunk index to track this chunk
        try chunk_index.put(entry.key_ptr.*, true);

        saved_count += 1;

        // Update the progress bar when a tenth of the chunks have been saved
        const tenth = chunk_store.count() / 10;

        if (saved_count % tenth == 0 or saved_count == chunk_store.count()) {
            bar.current = saved_count;
            try bar.render();
        }

        // std.debug.print("Saved chunk {s} (size: {} bytes)\n", .{ hash_hex[0..8], entry.value_ptr.*.len });
    }

    std.debug.print("Write complete! {} chunks written to {s}/\n", .{ saved_count, config.output_dir });
}

// Save snapshot metadata
fn saveSnapshot(allocator: std.mem.Allocator, snapshot: *const Snapshot, config: Config) !void {
    const timestamp_str = try std.fmt.allocPrint(allocator, "{d}", .{snapshot.timestamp});
    defer allocator.free(timestamp_str);

    const snapshot_path = try std.fmt.allocPrint(allocator, "{s}/snapshot_{s}.json", .{ config.repo_dir, timestamp_str });
    defer allocator.free(snapshot_path);

    const file = try std.fs.cwd().createFile(snapshot_path, .{});
    defer file.close();

    // 1. Define a buffer for the file writer (e.g., 4096 bytes)
    var buffer: [4096]u8 = undefined;

    // 2. Pass the buffer to file.writer() and store the result (the writer struct)
    var file_writer = file.writer(&buffer);

    // 3. Use the `.interface` field to get the std.io.Writer
    // Note: This also uses the modern `std.json.stringify` (your snippet was still using the deprecated `Stringify.value`).
    try json.Stringify.value(snapshot, .{}, &file_writer.interface);

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

pub fn loadConfig(allocator: std.mem.Allocator, config_path: []const u8) !Config {
    const file = try std.fs.cwd().openFile(config_path, .{});
    defer file.close();

    const file_size = try file.getEndPos();
    const contents = try allocator.alloc(u8, file_size);
    // keep the buffer alive for the whole lifetime of the Config
    defer allocator.free(contents);

    _ = try file.readAll(contents);
    if (contents.len == 0) return error.EmptyConfigFile;

    // tell the parser to allocate every string/array into the same allocator
    const parsed = try std.json.parseFromSlice(
        Config,
        allocator,
        contents,
        .{ .allocate = .alloc_always },
    );
    // parsed.value is the Config; parsed.arena owns the memory
    errdefer parsed.deinit();
    return parsed.value;
}

// pub fn loadConfig(allocator: std.mem.Allocator, config_path: []const u8) !Config {
//     const file = try std.fs.cwd().openFile(config_path, .{});
//     defer file.close();

//     const file_size = try file.getEndPos();
//     const contents = try allocator.alloc(u8, file_size);
//     defer allocator.free(contents);

//     _ = try file.readAll(contents);

//     if (contents.len == 0) {
//         return error.EmptyConfigFile;
//     }

//     const result = try json.parseFromSlice(Config, allocator, contents, .{});
//     defer result.deinit();

//     return result.value;
// }

pub fn saveConfig(allocator: std.mem.Allocator, config_param: Config) !void {
    var config = config_param;

    // Expand envriornment variables if availible
    var expanded_path: ?[]u8 = null;
    defer if (expanded_path) |p| allocator.free(p);

    if (std.mem.indexOf(u8, config.config_path, "$") != null) {
        expanded_path = try expandPath(allocator, config.config_path);
        config.config_path = expanded_path.?;
    }

    // 1. Create the full config file path (similar to snapshot_path)
    const config_path = try std.fmt.allocPrint(
        allocator,
        "{s}/config.json",
        .{config.config_path},
    );
    defer allocator.free(config_path);

    // Convert blacklist to JSON
    const blacklist_json = try arrayToJson(allocator, config.blacklist);
    defer allocator.free(blacklist_json);

    // 2. Open the file
    const file = try std.fs.cwd().createFile(config_path, .{});
    defer file.close();

    // Write the config JSON to the file
    const config_json = try std.fmt.allocPrint(allocator,
        \\{{
        \\  "root": "{s}",
        \\  "config_path": "{s}",
        \\  "blacklist": {s},
        \\  "output_dir": "{s}",
        \\  "chunk_extension": "{s}",
        \\  "repo_dir": "{s}",
        \\  "index_file": "{s}",
        \\  "compression_level": {},
        \\  "no_compression": {}
        \\}}
    , .{
        config.root,
        config.config_path,
        blacklist_json,
        config.output_dir,
        config.chunk_extension,
        config.repo_dir,
        config.index_file,
        config.compression_level,
        config.no_compression,
    });
    defer allocator.free(config_json);

    // 4. Write the entire JSON string to the file
    try file.writeAll(config_json);

    std.debug.print("Config saved: {s}\n", .{config_path});
}

pub fn configExists(allocator: std.mem.Allocator, config_path: []const u8) !bool {
    var expanded_path: ?[]u8 = null;
    defer if (expanded_path) |path| allocator.free(path);

    // 1. Introduce a mutable local variable
    var path_to_open: []const u8 = config_path;

    if (std.mem.containsAtLeast(u8, path_to_open, 1, "$")) {
        expanded_path = try expandPath(allocator, path_to_open);
        // 2. Assign the new value to the mutable variable
        path_to_open = expanded_path.?;
    }

    // 3. Use the mutable variable - catch file access errors
    const file = std.fs.cwd().openFile(path_to_open, .{}) catch |err| switch (err) {
        error.FileNotFound, error.NotDir, error.AccessDenied, error.IsDir => return false,
        else => return err,
    };
    defer file.close();

    return true;
}

fn arrayToJson(allocator: std.mem.Allocator, array: []const []const u8) ![]u8 {
    var json_str = try std.ArrayList(u8).initCapacity(allocator, 0);
    defer json_str.deinit(allocator);

    try json_str.append(allocator, '[');
    for (array, 0..) |item, i| {
        if (i > 0) try json_str.appendSlice(allocator, ", ");
        try json_str.appendSlice(allocator, "\"");
        try json_str.appendSlice(allocator, item);
        try json_str.appendSlice(allocator, "\"");
    }
    try json_str.append(allocator, ']');

    return json_str.toOwnedSlice(allocator);
}

pub fn depthFirstProcess(allocator: std.mem.Allocator, config: Config) !void {
    // Initialize directory iterator
    var dir = try fs.cwd().openDir(config.root, .{ .iterate = true });
    defer dir.close();

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        switch (entry.kind) {
            .File => {
                // Process file according to configuration
                if (!isBlacklisted(entry.name, config.blacklist)) {
                    // Build full file path
                    const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ config.root, entry.name });
                    defer allocator.free(file_path);

                    // Process the file into chunks
                    const file_node = try processFileIntoChunks(allocator, &chunk_store, &chunk_index, file_path, entry.name, config);
                    try file_nodes.append(allocator, file_node);
                }
            },
            .Directory => {
                // Process directory according to configuration
                if (!isBlacklisted(entry.name, config.blacklist)) {
                    try depthFirstProcess(allocator, entry.name);
                }
            },
            .Symlink => {
                // Process symlink according to configuration
                break;
            },
            .Unknown => {
                // Process unknown entry according to configuration
                break;
            },
        }
    }
}

// TODO: Implement S3 upload functionality
// fn uploadChunkToS3(bucket: []const u8, hash: [32]u8, chunk_data: []const u8) !void {
//     // Initialize S3 client
//     // Upload chunk with hash as key
//     // Handle errors appropriately
// }
