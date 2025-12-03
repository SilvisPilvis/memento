Instead of pure diffs, consider content-defined chunking:

1. Split ALL files (text and binary) into chunks using rolling hash
2. Only store unique chunks (deduplication)
3. Compress chunks with zstd
4. Store chunk references

```zig
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
```

This gives you:

Deduplication across ALL files automatically
No diff chain complexity
Better handling of file moves/renames
Works well for binary files too

Practical recommendation:
For a first version, I'd suggest:

Start simpler: full snapshots + incremental file-level backups
Add zstd compression
Only add diffing later if space savings justify the complexity

Or, use content-chunking from the start (like restic does) - it's more robust.
Want me to show you example code for content-defined chunking, or would you prefer to stick with your diff-based approach?
