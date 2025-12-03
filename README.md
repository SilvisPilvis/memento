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

# TODO

Looking at your backup program, it has a solid foundation with content-defined chunking and deduplication, but there are several areas where it could be more efficient for a production backup system:
Current Strengths:

SHA-256 hashing for deduplication is excellent
Fixed 4KB chunks are reasonable and simple
Basic structure is clean

Areas for Improvement:
1. Chunking Strategy
Your fixed 4KB chunks are simple but inefficient. If you insert even one byte at the start of a file, all chunk boundaries shift, and you lose all deduplication benefits. Consider:

Content-Defined Chunking (CDC) using rolling hash algorithms (like Rabin fingerprinting or FastCDC) - this finds chunk boundaries based on content, so inserting data only affects nearby chunks
Variable chunk sizes (2KB-8KB average) work better in practice

2. Missing Critical Features

- No compression - you should compress chunks before storage (especially for text files)
- No encryption - backup data should be encrypted at rest
- No metadata storage - you need to persist the FileNode mappings so you can restore files later. Currently you build file_nodes but never save it
- No incremental backups - you process all files every time. Need to track what's changed
- No recursive directory traversal - you have a TODO comment but only process one level

3. Performance Issues

Reading entire files into memory (allocator.alloc(u8, file_size)) won't scale to large files
Should stream files and process chunks incrementally
No parallelization - could process multiple files concurrently

4. Reliability Concerns

No error recovery or retry logic
No verification that chunks were written correctly
Missing S3 upload implementation (you have a TODO)
No handling of file permissions, symlinks, or special files

Recommended Next Steps:

Implement content-defined chunking (FastCDC is a good choice)
Add compression (zstd is fast and effective)
Save metadata to a JSON/database file for restore capability
Add incremental backup logic (only process modified files)
Stream large files instead of loading entirely into memory
Complete the S3 integration
