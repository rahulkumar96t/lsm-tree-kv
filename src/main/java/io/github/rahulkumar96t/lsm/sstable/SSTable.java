package io.github.rahulkumar96t.lsm.sstable;

import io.github.rahulkumar96t.lsm.CorruptionException;
import io.github.rahulkumar96t.lsm.internal.Entry;
import io.github.rahulkumar96t.lsm.internal.ValueType;
import io.github.rahulkumar96t.lsm.util.Bytes;
import io.github.rahulkumar96t.lsm.util.FileUtil;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

/**
 * Read side of an immutable SSTable (format: see {@link SSTableWriter}).
 *
 * <p>Opening a table validates the footer and loads the sparse index, bloom filter and meta block
 * into memory; data blocks are read on demand with positional reads, so one table can serve any
 * number of concurrent readers. Every block read verifies the block's CRC32.
 *
 * <p>Lifetime is reference counted. The engine's current version holds one reference; readers
 * {@link #tryRetain() retain} the table for the duration of a get or scan. When compaction replaces
 * a table it is {@link #markObsolete() marked obsolete} and released, and the file is deleted when
 * the last reader releases it.
 */
public final class SSTable {

  private final long id;
  private final Path path;
  private final long fileSize;
  private final byte[][] blockFirstKeys;
  private final long[] blockOffsets;
  private final int[] blockLengths;
  private final BloomFilter bloom;
  private final TableMeta meta;

  private final AtomicInteger refs = new AtomicInteger(1);
  private final Object channelLock = new Object();
  private volatile FileChannel channel;
  private volatile boolean obsolete;

  private SSTable(
      long id,
      Path path,
      FileChannel channel,
      long fileSize,
      byte[][] blockFirstKeys,
      long[] blockOffsets,
      int[] blockLengths,
      BloomFilter bloom,
      TableMeta meta) {
    this.id = id;
    this.path = path;
    this.channel = channel;
    this.fileSize = fileSize;
    this.blockFirstKeys = blockFirstKeys;
    this.blockOffsets = blockOffsets;
    this.blockLengths = blockLengths;
    this.bloom = bloom;
    this.meta = meta;
  }

  /** Opens and validates the table at {@code path}. */
  public static SSTable open(long id, Path path) throws IOException {
    FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
    try {
      long size = channel.size();
      if (size < Footer.SIZE_BYTES) {
        throw new CorruptionException(path.getFileName() + ": file too short for a footer");
      }
      Footer footer =
          Footer.deserialize(read(channel, size - Footer.SIZE_BYTES, Footer.SIZE_BYTES), size);
      ByteBuffer index = checkedBlock(channel, footer.index(), "index");
      ByteBuffer bloomBlock = checkedBlock(channel, footer.bloom(), "bloom");
      ByteBuffer metaBlock = checkedBlock(channel, footer.meta(), "meta");

      int blocks = index.getInt();
      if (blocks < 1 || blocks > index.remaining() / (Integer.BYTES + Long.BYTES + Integer.BYTES)) {
        throw new CorruptionException(path.getFileName() + ": bad block count " + blocks);
      }
      byte[][] firstKeys = new byte[blocks][];
      long[] offsets = new long[blocks];
      int[] lengths = new int[blocks];
      for (int i = 0; i < blocks; i++) {
        firstKeys[i] = new byte[index.getInt()];
        index.get(firstKeys[i]);
        offsets[i] = index.getLong();
        lengths[i] = index.getInt();
        if (offsets[i] < 0
            || lengths[i] <= SSTableWriter.BLOCK_TRAILER_BYTES
            || offsets[i] + lengths[i] > footer.index().offset()) {
          throw new CorruptionException(path.getFileName() + ": index entry " + i + " is invalid");
        }
      }
      return new SSTable(
          id,
          path,
          channel,
          size,
          firstKeys,
          offsets,
          lengths,
          BloomFilter.deserialize(bloomBlock),
          TableMeta.deserialize(metaBlock));
    } catch (IOException | RuntimeException e) {
      channel.close();
      if (e instanceof RuntimeException) {
        throw new CorruptionException(path.getFileName() + ": malformed table: " + e);
      }
      throw e;
    }
  }

  public long id() {
    return id;
  }

  public Path path() {
    return path;
  }

  public long sizeBytes() {
    return fileSize;
  }

  public TableMeta meta() {
    return meta;
  }

  public int blockCount() {
    return blockOffsets.length;
  }

  /** Whether the bloom filter admits {@code key} (false means the key is definitely absent). */
  public boolean mightContain(byte[] key) {
    return bloom.mightContain(key);
  }

  /** The entry for {@code key} (possibly a tombstone), or {@code null} if the table lacks it. */
  public Entry get(byte[] key) throws IOException {
    if (Bytes.compare(key, meta.minKey()) < 0 || Bytes.compare(key, meta.maxKey()) > 0) {
      return null;
    }
    if (!bloom.mightContain(key)) {
      return null;
    }
    int block = floorBlock(key);
    if (block < 0) {
      return null;
    }
    ByteBuffer buf = readBlock(block);
    byte[] array = buf.array();
    try {
      // Compare keys in place and skip non-matching values without copying them.
      while (buf.hasRemaining()) {
        int keyLength = buf.getInt();
        int keyStart = buf.position();
        int cmp = Arrays.compareUnsigned(array, keyStart, keyStart + keyLength, key, 0, key.length);
        if (cmp > 0) {
          return null; // keys are sorted: the key is not in this block
        }
        if (cmp == 0) {
          return readEntryAfterKeyLength(buf, keyLength);
        }
        buf.position(keyStart + keyLength + Long.BYTES + 1);
        int valueLength = buf.getInt();
        buf.position(buf.position() + valueLength);
      }
      return null;
    } catch (RuntimeException e) {
      throw new CorruptionException(path.getFileName() + ": malformed data block " + block);
    }
  }

  /** Entries with {@code from <= key < to} in key order; a {@code null} bound is unbounded. */
  public Iterator<Entry> iterator(byte[] from, byte[] to) {
    boolean disjoint =
        (from != null && Bytes.compare(meta.maxKey(), from) < 0)
            || (to != null && Bytes.compare(meta.minKey(), to) >= 0)
            || (from != null && to != null && Bytes.compare(from, to) >= 0);
    return disjoint ? Collections.emptyIterator() : new TableIterator(from, to);
  }

  /** Index of the last block whose first key is {@code <= key}, or -1 if there is none. */
  private int floorBlock(byte[] key) {
    int lo = 0;
    int hi = blockFirstKeys.length - 1;
    int result = -1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      if (Bytes.compare(blockFirstKeys[mid], key) <= 0) {
        result = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return result;
  }

  /** Reads data block {@code i}, verifies its CRC and returns a buffer over its entries. */
  private ByteBuffer readBlock(int i) throws IOException {
    int length = blockLengths[i];
    ByteBuffer buf = ByteBuffer.allocate(length);
    readFully(buf, blockOffsets[i]);
    int payload = length - SSTableWriter.BLOCK_TRAILER_BYTES;
    CRC32 crc = new CRC32();
    crc.update(buf.array(), 0, payload);
    if ((int) crc.getValue() != buf.getInt(payload)) {
      throw new CorruptionException(path.getFileName() + ": checksum mismatch in data block " + i);
    }
    return buf.clear().limit(payload);
  }

  private Entry readEntry(ByteBuffer buf) throws CorruptionException {
    return readEntryAfterKeyLength(buf, buf.getInt());
  }

  private Entry readEntryAfterKeyLength(ByteBuffer buf, int keyLength) throws CorruptionException {
    try {
      byte[] key = new byte[keyLength];
      buf.get(key);
      long seq = buf.getLong();
      ValueType type = ValueType.fromCode(buf.get());
      byte[] value = new byte[buf.getInt()];
      buf.get(value);
      if (type == null) {
        throw new CorruptionException(path.getFileName() + ": unknown value type");
      }
      return new Entry(key, seq, type, value);
    } catch (RuntimeException e) {
      throw new CorruptionException(path.getFileName() + ": malformed entry: " + e);
    }
  }

  /**
   * Positional read that survives interrupts of <em>other</em> threads: a {@link FileChannel} is
   * closed when any thread blocked on it is interrupted. If that happens while the table is still
   * referenced, the channel is reopened and the read retried; the interrupted thread itself gets an
   * {@link InterruptedIOException}.
   */
  private void readFully(ByteBuffer dst, long position) throws IOException {
    for (int attempt = 0; ; attempt++) {
      FileChannel ch = channel;
      try {
        while (dst.hasRemaining()) {
          if (ch.read(dst, position + dst.position()) < 0) {
            throw new EOFException(path.getFileName() + ": unexpected end of file");
          }
        }
        return;
      } catch (ClosedChannelException e) {
        if (refs.get() <= 0 || attempt >= 3) {
          throw e;
        }
        reopen(ch);
        if (Thread.currentThread().isInterrupted()) {
          InterruptedIOException interrupted = new InterruptedIOException("read interrupted");
          interrupted.initCause(e);
          throw interrupted;
        }
        dst.clear();
      }
    }
  }

  private void reopen(FileChannel closed) throws IOException {
    synchronized (channelLock) {
      if (channel == closed && refs.get() > 0) {
        channel = FileChannel.open(path, StandardOpenOption.READ);
      }
    }
  }

  private static byte[] read(FileChannel channel, long position, int length) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(length);
    while (buf.hasRemaining()) {
      if (channel.read(buf, position + buf.position()) < 0) {
        throw new EOFException("unexpected end of file");
      }
    }
    return buf.array();
  }

  private static ByteBuffer checkedBlock(FileChannel channel, Footer.BlockHandle h, String name)
      throws IOException {
    byte[] bytes = read(channel, h.offset(), h.length());
    if (Footer.crc(bytes, bytes.length) != h.crc()) {
      throw new CorruptionException(name + " block checksum mismatch");
    }
    return ByteBuffer.wrap(bytes);
  }

  // ---- reference counting ---------------------------------------------------------------------

  /** Acquires a reference; fails if the table has already been closed. */
  public boolean tryRetain() {
    for (; ; ) {
      int current = refs.get();
      if (current <= 0) {
        return false;
      }
      if (refs.compareAndSet(current, current + 1)) {
        return true;
      }
    }
  }

  /** Drops a reference. The last release closes the file and deletes it if obsolete. */
  public void release() {
    int remaining = refs.decrementAndGet();
    if (remaining < 0) {
      throw new IllegalStateException("SSTable " + id + " released too often");
    }
    if (remaining == 0) {
      closeChannel();
      if (obsolete) {
        FileUtil.deleteQuietly(path);
      }
    }
  }

  /** Marks the file for deletion once the last reference is released. */
  public void markObsolete() {
    obsolete = true;
  }

  /** Closes the file without deleting it, regardless of references (crash simulation). */
  public void closeChannel() {
    synchronized (channelLock) {
      try {
        channel.close();
      } catch (IOException ignored) {
        // Nothing useful to do: the table is going away.
      }
    }
  }

  @Override
  public String toString() {
    return "SSTable["
        + path.getFileName()
        + ", "
        + meta.entryCount()
        + " entries, "
        + fileSize
        + " B]";
  }

  /** Lazily decodes blocks; reads are positional, so concurrent iterators are independent. */
  private final class TableIterator implements Iterator<Entry> {
    private final byte[] to;
    private int blockIndex;
    private ByteBuffer block;
    private Entry next;

    TableIterator(byte[] from, byte[] to) {
      this.to = to;
      this.blockIndex = from == null ? 0 : Math.max(0, floorBlock(from));
      try {
        this.block = readBlock(blockIndex);
        advance();
        while (next != null && from != null && Bytes.compare(next.key(), from) < 0) {
          advance();
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private void advance() throws IOException {
      while (!block.hasRemaining()) {
        if (++blockIndex >= blockOffsets.length) {
          next = null;
          return;
        }
        block = readBlock(blockIndex);
      }
      Entry e = readEntry(block);
      next = to != null && Bytes.compare(e.key(), to) >= 0 ? null : e;
      if (next == null) {
        block = ByteBuffer.allocate(0);
        blockIndex = blockOffsets.length;
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public Entry next() {
      Entry current = next;
      if (current == null) {
        throw new NoSuchElementException();
      }
      try {
        advance();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return current;
    }
  }
}
