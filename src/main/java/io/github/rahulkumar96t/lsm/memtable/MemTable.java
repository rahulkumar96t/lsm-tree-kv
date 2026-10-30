package io.github.rahulkumar96t.lsm.memtable;

import io.github.rahulkumar96t.lsm.internal.Entry;
import io.github.rahulkumar96t.lsm.util.Bytes;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory, sorted write buffer backed by a {@link ConcurrentSkipListMap}.
 *
 * <p>The memtable keeps only the latest version of each key: writes are serialised by the engine
 * and carry increasing sequence numbers, so an overwrite always replaces an older version. Readers
 * never block; iteration is weakly consistent (it may or may not observe writes that happen while
 * it runs). Once the engine freezes a memtable for flushing it is never written again, so iterating
 * a frozen memtable is fully stable.
 */
public final class MemTable {

  private final ConcurrentSkipListMap<byte[], Entry> entries =
      new ConcurrentSkipListMap<>(Bytes.COMPARATOR);
  private final AtomicLong approximateBytes = new AtomicLong();
  private final long walId;
  private volatile long maxSeq;

  /**
   * @param walId id of the write-ahead log that holds this memtable's writes; it can be deleted
   *     once the memtable has been flushed
   */
  public MemTable(long walId) {
    this.walId = walId;
  }

  public long walId() {
    return walId;
  }

  /** Inserts or replaces the entry for its key. Callers must serialise writes. */
  public void put(Entry entry) {
    Entry previous = entries.put(entry.key(), entry);
    long delta = entry.approximateBytes() - (previous == null ? 0 : previous.approximateBytes());
    approximateBytes.addAndGet(delta);
    if (entry.seq() > maxSeq) {
      maxSeq = entry.seq();
    }
  }

  /** Latest entry for {@code key} (possibly a tombstone), or {@code null}. */
  public Entry get(byte[] key) {
    return entries.get(key);
  }

  /** Entries with {@code from <= key < to} in key order; a {@code null} bound is unbounded. */
  public Iterator<Entry> iterator(byte[] from, byte[] to) {
    NavigableMap<byte[], Entry> view = entries;
    if (from != null && to != null) {
      if (Bytes.compare(from, to) >= 0) {
        return Collections.emptyIterator();
      }
      view = entries.subMap(from, true, to, false);
    } else if (from != null) {
      view = entries.tailMap(from, true);
    } else if (to != null) {
      view = entries.headMap(to, false);
    }
    return view.values().iterator();
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }

  public int size() {
    return entries.size();
  }

  public long approximateBytes() {
    return approximateBytes.get();
  }

  /** Highest sequence number written to this memtable, {@code 0} if empty. */
  public long maxSeq() {
    return maxSeq;
  }
}
