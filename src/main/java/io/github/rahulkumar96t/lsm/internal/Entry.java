package io.github.rahulkumar96t.lsm.internal;

import io.github.rahulkumar96t.lsm.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * One versioned record: a key, the sequence number of the write that produced it, its type and its
 * value. Sequence numbers are assigned by the engine in write order, so for two entries with the
 * same key the one with the higher {@code seq} is newer and wins.
 *
 * <p>For a {@link ValueType#DELETE tombstone} the value holds the 8-byte deletion time in epoch
 * milliseconds; compaction uses it to decide when a tombstone has expired.
 *
 * <p>Arrays are not copied: callers hand over ownership and must not mutate them afterwards.
 */
public record Entry(byte[] key, long seq, ValueType type, byte[] value) {

  /** Fixed per-entry overhead used for memory accounting (object headers, seq, references). */
  public static final int OVERHEAD_BYTES = 48;

  public Entry {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(value, "value");
  }

  public static Entry put(byte[] key, long seq, byte[] value) {
    return new Entry(key, seq, ValueType.PUT, value);
  }

  public static Entry tombstone(byte[] key, long seq, long deletedAtMillis) {
    byte[] when = ByteBuffer.allocate(Long.BYTES).putLong(deletedAtMillis).array();
    return new Entry(key, seq, ValueType.DELETE, when);
  }

  public boolean isTombstone() {
    return type == ValueType.DELETE;
  }

  /** Deletion time of a tombstone in epoch millis, or {@code 0} if none was recorded. */
  public long deletedAtMillis() {
    if (!isTombstone()) {
      throw new IllegalStateException("not a tombstone");
    }
    return value.length == Long.BYTES ? ByteBuffer.wrap(value).getLong() : 0L;
  }

  /** Approximate heap footprint, used to decide when a memtable is full. */
  public long approximateBytes() {
    return (long) key.length + value.length + OVERHEAD_BYTES;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Entry e
        && seq == e.seq
        && type == e.type
        && Arrays.equals(key, e.key)
        && Arrays.equals(value, e.value);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(key) + Long.hashCode(seq);
  }

  @Override
  public String toString() {
    return "Entry[key=" + Bytes.string(key) + ", seq=" + seq + ", type=" + type + "]";
  }
}
