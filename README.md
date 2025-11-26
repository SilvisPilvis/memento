Instead of pure diffs, consider content-defined chunking:

1. Split ALL files (text and binary) into chunks using rolling hash
2. Only store unique chunks (deduplication)
3. Compress chunks with zstd
4. Store chunk references

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
