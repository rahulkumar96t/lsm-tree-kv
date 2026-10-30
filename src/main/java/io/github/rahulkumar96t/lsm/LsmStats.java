package io.github.rahulkumar96t.lsm;

import java.util.List;

/**
 * Point-in-time engine statistics.
 *
 * @param lastSequence sequence number of the latest write
 * @param memtableBytes approximate size of the active memtable
 * @param immutableMemtables frozen memtables waiting to be flushed
 * @param tableSizes SSTable sizes in bytes, oldest first
 * @param tableEntries total entries across SSTables (including shadowed versions and tombstones)
 * @param flushes memtable flushes since open
 * @param compactions compactions since open
 * @param compactionBytesRead SSTable bytes read by compactions since open
 * @param compactionBytesWritten SSTable bytes written by compactions since open
 * @param shadowedVersionsDropped older versions discarded by compactions since open
 * @param tombstonesDropped expired tombstones discarded by compactions since open
 * @param writeStalls writes that had to wait for a flush since open
 * @param walRecordsReplayed WAL records applied during the last open
 * @param walBytesDiscarded torn or corrupt WAL bytes ignored during the last open
 */
public record LsmStats(
    long lastSequence,
    long memtableBytes,
    int immutableMemtables,
    List<Long> tableSizes,
    long tableEntries,
    long flushes,
    long compactions,
    long compactionBytesRead,
    long compactionBytesWritten,
    long shadowedVersionsDropped,
    long tombstonesDropped,
    long writeStalls,
    long walRecordsReplayed,
    long walBytesDiscarded) {

  public LsmStats {
    tableSizes = List.copyOf(tableSizes);
  }

  public int tableCount() {
    return tableSizes.size();
  }

  public long tableBytes() {
    return tableSizes.stream().mapToLong(Long::longValue).sum();
  }
}
