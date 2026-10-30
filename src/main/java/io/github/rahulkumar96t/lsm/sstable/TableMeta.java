package io.github.rahulkumar96t.lsm.sstable;

import io.github.rahulkumar96t.lsm.CorruptionException;
import java.nio.ByteBuffer;

/**
 * Summary statistics of one SSTable, stored in its meta block.
 *
 * @param entryCount number of entries (one per key)
 * @param tombstoneCount how many of them are tombstones
 * @param minSeq lowest sequence number in the table
 * @param maxSeq highest sequence number in the table
 * @param minKey smallest key
 * @param maxKey largest key
 */
public record TableMeta(
    long entryCount, long tombstoneCount, long minSeq, long maxSeq, byte[] minKey, byte[] maxKey) {

  byte[] serialize() {
    ByteBuffer buf =
        ByteBuffer.allocate(4 * Long.BYTES + 2 * Integer.BYTES + minKey.length + maxKey.length);
    buf.putLong(entryCount).putLong(tombstoneCount).putLong(minSeq).putLong(maxSeq);
    buf.putInt(minKey.length).put(minKey).putInt(maxKey.length).put(maxKey);
    return buf.array();
  }

  static TableMeta deserialize(ByteBuffer buf) throws CorruptionException {
    try {
      long entries = buf.getLong();
      long tombstones = buf.getLong();
      long minSeq = buf.getLong();
      long maxSeq = buf.getLong();
      byte[] minKey = new byte[buf.getInt()];
      buf.get(minKey);
      byte[] maxKey = new byte[buf.getInt()];
      buf.get(maxKey);
      if (buf.hasRemaining() || entries < 1 || tombstones > entries || minSeq > maxSeq) {
        throw new CorruptionException("meta block is inconsistent");
      }
      return new TableMeta(entries, tombstones, minSeq, maxSeq, minKey, maxKey);
    } catch (RuntimeException e) {
      throw new CorruptionException("meta block is malformed: " + e);
    }
  }
}
