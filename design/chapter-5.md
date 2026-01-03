# Chapter 5

Sequence numbers turn a stream of writes into a consistent timeline. Each update gets a monotonically increasing sequence, which makes it possible to decide which version of a key is the newest even when multiple tables contain it.

The key space now uses internal keys that combine the user key with a sequence number and a value type. Internal keys are ordered by user key first, then by sequence in descending order, which means the newest version appears first in both the memtable and SSTables. Reads can ask for the "latest" version by seeking to the highest possible sequence for a user key.

This change also lays the groundwork for snapshots and transactions. With a stable sequence timeline, a snapshot can simply read values "as of" a particular sequence number. That capability is not implemented yet, but the storage layer now has the structure to support it.
