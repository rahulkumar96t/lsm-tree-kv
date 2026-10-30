package io.github.rahulkumar96t.lsm.sstable;

import io.github.rahulkumar96t.lsm.CorruptionException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Fixed-size trailer of an SSTable that locates and checksums the index, bloom and meta blocks.
 *
 * <pre>
 * indexOffset 8 | indexLength 4 | indexCrc 4
 * bloomOffset 8 | bloomLength 4 | bloomCrc 4
 * metaOffset  8 | metaLength  4 | metaCrc  4
 * formatVersion 4 | magic 8 | footerCrc 4          (64 bytes in total)
 * </pre>
 */
record Footer(BlockHandle index, BlockHandle bloom, BlockHandle meta) {

  static final int SIZE_BYTES = 64;
  static final long MAGIC = 0x4C534D5353544231L; // "LSMSSTB1"
  static final int FORMAT_VERSION = 1;

  /** Location, length and CRC32 of a block. */
  record BlockHandle(long offset, int length, int crc) {
    void putTo(ByteBuffer buf) {
      buf.putLong(offset).putInt(length).putInt(crc);
    }

    static BlockHandle readFrom(ByteBuffer buf) {
      return new BlockHandle(buf.getLong(), buf.getInt(), buf.getInt());
    }
  }

  byte[] serialize() {
    ByteBuffer buf = ByteBuffer.allocate(SIZE_BYTES);
    index.putTo(buf);
    bloom.putTo(buf);
    meta.putTo(buf);
    buf.putInt(FORMAT_VERSION).putLong(MAGIC);
    buf.putInt(crc(buf.array(), SIZE_BYTES - Integer.BYTES));
    return buf.array();
  }

  static Footer deserialize(byte[] bytes, long fileSize) throws CorruptionException {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    BlockHandle index = BlockHandle.readFrom(buf);
    BlockHandle bloom = BlockHandle.readFrom(buf);
    BlockHandle meta = BlockHandle.readFrom(buf);
    int version = buf.getInt();
    long magic = buf.getLong();
    int storedCrc = buf.getInt();
    if (magic != MAGIC) {
      throw new CorruptionException("not an SSTable (bad magic)");
    }
    if (storedCrc != crc(bytes, SIZE_BYTES - Integer.BYTES)) {
      throw new CorruptionException("footer checksum mismatch");
    }
    if (version != FORMAT_VERSION) {
      throw new CorruptionException("unsupported SSTable format version " + version);
    }
    long dataEnd = fileSize - SIZE_BYTES;
    for (BlockHandle h : new BlockHandle[] {index, bloom, meta}) {
      if (h.offset() < 0 || h.length() < 0 || h.offset() + h.length() > dataEnd) {
        throw new CorruptionException("footer points outside the file");
      }
    }
    return new Footer(index, bloom, meta);
  }

  static int crc(byte[] bytes, int length) {
    CRC32 crc = new CRC32();
    crc.update(bytes, 0, length);
    return (int) crc.getValue();
  }
}
