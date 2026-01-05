# Chapter 7

Snapshots make multi-version reads feel natural. A snapshot captures a single sequence number, and every read through that snapshot resolves values as they existed at that point in time. Writes can continue, but the snapshot stays stable, which makes it a simple building block for consistent reads and long-running operations.

The API keeps things direct: `db.NewSnapshot()` returns a handle with its own `Get`. There is no extra options struct or hidden state; the snapshot carries the sequence and delegates to the same read path with an explicit cutoff. This keeps the surface small while enabling future features like transactions or read-only iterators.

Snapshots should be released when they are no longer needed. Releasing a snapshot allows compaction to drop older versions that no active snapshot can see, which keeps storage growth under control.
