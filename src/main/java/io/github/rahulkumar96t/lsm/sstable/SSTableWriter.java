package io.github.rahulkumar96t.lsm.sstable;

import io.github.rahulkumar96t.lsm.internal.Entry;
import io.github.rahulkumar96t.lsm.util.Bytes;
import io.github.rahulkumar96t.lsm.util.FileUtil;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Writes a sorted stream of entries as an immutable SSTable file.
 *
 * <p>File layout:
 *
 * <pre>
 * [data block 0] ... [data block n-1] [index block] [bloom block] [meta block] [footer]
 * </pre>
 *
 * <ul>
 *   <li><b>Data block</b>: entries {@code keyLen:int key seq:long type:byte valueLen:int value},
 *       sorted by key, followed by a CRC32 of the block. A block is closed once it reaches the
 *       target block size.
 *   <li><b>Index block</b> (sparse index): {@code count:int} then, per data block, {@code
 *       firstKeyLen:int firstKey offset:long length:int}.
 *   <li><b>Bloom block</b>: see {@link BloomFilter}.
 *   <li><b>Meta block</b>: see {@link TableMeta}.
 *   <li><b>Footer</b>: see {@link Footer}; it checksums the three blocks above and itself.
 * </ul>
 *
 * <p>The table is written to {@code <name>.tmp}, fsynced, and atomically renamed into place, so a
 * crash never leaves a partially written table under its final name.
 */
public final class SSTableWriter {

  static final int BLOCK_TRAILER_BYTES = Integer.BYTES;

  private SSTableWriter() {}

  /**
   * Writes {@code entries} (strictly ascending keys) to {@code path}.
   *
   * @return the table's metadata, or {@code null} if {@code entries} was empty (no file is created)
   */
  public static TableMeta write(Path path, Iterator<Entry> entries, int blockSize, int bitsPerKey)
      throws IOException {
    Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
    TableMeta meta;
    try (FileOutputStream file = new FileOutputStream(tmp.toFile());
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(file, 1 << 16))) {
      meta = new BlockWriter(out, blockSize, bitsPerKey).writeAll(entries);
      out.flush();
      if (meta != null) {
        file.getFD().sync();
      }
    } catch (IOException | RuntimeException e) {
      FileUtil.deleteQuietly(tmp);
      throw e;
    }
    if (meta == null) {
      Files.deleteIfExists(tmp);
      return null;
    }
    FileUtil.atomicMove(tmp, path);
    return meta;
  }

  /** Streams entries into blocks and writes the trailing index, bloom, meta and footer. */
  private static final class BlockWriter {
    private final DataOutputStream out;
    private final int blockSize;
    private final int bitsPerKey;

    private final ByteArrayOutputStream blockBytes = new ByteArrayOutputStream();
    private final DataOutputStream block = new DataOutputStream(blockBytes);
    private final List<byte[]> indexKeys = new ArrayList<>();
    private final List<long[]> indexHandles = new ArrayList<>(); // {offset, length}
    private long[] hashPairs = new long[1024];
    private byte[] blockFirstKey;
    private long offset;

    private long count;
    private long tombstones;
    private long minSeq = Long.MAX_VALUE;
    private long maxSeq = Long.MIN_VALUE;
    private byte[] firstKey;
    private byte[] lastKey;

    BlockWriter(DataOutputStream out, int blockSize, int bitsPerKey) {
      this.out = out;
      this.blockSize = blockSize;
      this.bitsPerKey = bitsPerKey;
    }

    TableMeta writeAll(Iterator<Entry> entries) throws IOException {
      while (entries.hasNext()) {
        add(entries.next());
      }
      if (count == 0) {
        return null;
      }
      finishBlock();
      Footer.BlockHandle index = writeBlock(indexBlock());
      Footer.BlockHandle bloom =
          writeBlock(BloomFilter.build(hashPairs, (int) count, bitsPerKey).serialize());
      TableMeta meta = new TableMeta(count, tombstones, minSeq, maxSeq, firstKey, lastKey);
      Footer.BlockHandle metaHandle = writeBlock(meta.serialize());
      out.write(new Footer(index, bloom, metaHandle).serialize());
      return meta;
    }

    private void add(Entry e) throws IOException {
      if (lastKey != null && Bytes.compare(lastKey, e.key()) >= 0) {
        throw new IllegalStateException("keys must be strictly ascending");
      }
      if (count >= Integer.MAX_VALUE / 2) {
        throw new IllegalStateException("too many entries for one table");
      }
      if (blockFirstKey == null) {
        blockFirstKey = e.key();
      }
      block.writeInt(e.key().length);
      block.write(e.key());
      block.writeLong(e.seq());
      block.writeByte(e.type().code());
      block.writeInt(e.value().length);
      block.write(e.value());

      long[] h = BloomFilter.hashes(e.key());
      int slot = (int) (2 * count);
      if (slot + 2 > hashPairs.length) {
        hashPairs = Arrays.copyOf(hashPairs, hashPairs.length * 2);
      }
      hashPairs[slot] = h[0];
      hashPairs[slot + 1] = h[1];

      if (firstKey == null) {
        firstKey = e.key();
      }
      lastKey = e.key();
      count++;
      if (e.isTombstone()) {
        tombstones++;
      }
      minSeq = Math.min(minSeq, e.seq());
      maxSeq = Math.max(maxSeq, e.seq());
      if (blockBytes.size() >= blockSize) {
        finishBlock();
      }
    }

    private void finishBlock() throws IOException {
      if (blockBytes.size() == 0) {
        return;
      }
      byte[] bytes = blockBytes.toByteArray();
      CRC32 crc = new CRC32();
      crc.update(bytes);
      out.write(bytes);
      out.writeInt((int) crc.getValue());
      int length = bytes.length + BLOCK_TRAILER_BYTES;
      indexKeys.add(blockFirstKey);
      indexHandles.add(new long[] {offset, length});
      offset += length;
      blockBytes.reset();
      blockFirstKey = null;
    }

    private byte[] indexBlock() throws IOException {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream index = new DataOutputStream(bytes);
      index.writeInt(indexKeys.size());
      for (int i = 0; i < indexKeys.size(); i++) {
        index.writeInt(indexKeys.get(i).length);
        index.write(indexKeys.get(i));
        index.writeLong(indexHandles.get(i)[0]);
        index.writeInt((int) indexHandles.get(i)[1]);
      }
      return bytes.toByteArray();
    }

    private Footer.BlockHandle writeBlock(byte[] bytes) throws IOException {
      CRC32 crc = new CRC32();
      crc.update(bytes);
      Footer.BlockHandle handle =
          new Footer.BlockHandle(offset, bytes.length, (int) crc.getValue());
      out.write(bytes);
      offset += bytes.length;
      return handle;
    }
  }
}
