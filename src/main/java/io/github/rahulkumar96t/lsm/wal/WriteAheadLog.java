package io.github.rahulkumar96t.lsm.wal;

import io.github.rahulkumar96t.lsm.CorruptionException;
import io.github.rahulkumar96t.lsm.WalRecoveryMode;
import io.github.rahulkumar96t.lsm.internal.Entry;
import io.github.rahulkumar96t.lsm.internal.ValueType;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * Append-only write-ahead log. Every write is appended here before it becomes visible in the
 * memtable, so the memtable can be rebuilt after a crash.
 *
 * <p>Record layout (big-endian):
 *
 * <pre>
 * +---------+-----------+------------------------------------------------------------+
 * | crc32 4 | length 4  | payload: type 1 | seq 8 | keyLen 4 | key | valueLen 4 | value |
 * +---------+-----------+------------------------------------------------------------+
 * </pre>
 *
 * The CRC covers the length field and the payload, so a corrupted length is detected as well. Each
 * record goes straight to the file without user-space buffering: once {@link #append} returns, the
 * record is in the OS page cache and survives a process crash. With {@code syncEachWrite} it is
 * also forced to the device, which additionally survives power loss.
 *
 * <p>The log is written through a {@link FileOutputStream} rather than a {@code FileChannel}: an
 * interrupt delivered to a thread blocked in a {@code FileChannel} operation closes the channel,
 * which would turn a cancelled caller into a dead log.
 *
 * <p>Not thread-safe; the engine serialises appends under its write lock.
 */
public final class WriteAheadLog implements Closeable {

  /** CRC (4 bytes) + payload length (4 bytes). */
  public static final int HEADER_BYTES = 8;

  /** Payload bytes of a record with an empty key and empty value. */
  static final int MIN_PAYLOAD_BYTES = 1 + Long.BYTES + Integer.BYTES + Integer.BYTES;

  /** Upper bound on a record, guards against allocating huge buffers for a corrupt length. */
  public static final int MAX_PAYLOAD_BYTES = 128 << 20;

  private final Path path;
  private final FileOutputStream out;
  private final boolean syncEachWrite;
  private long sizeBytes;

  private WriteAheadLog(Path path, FileOutputStream out, boolean syncEachWrite) {
    this.path = path;
    this.out = out;
    this.syncEachWrite = syncEachWrite;
  }

  /** Creates a new, empty log. Fails if the file already exists. */
  public static WriteAheadLog create(Path path, boolean syncEachWrite) throws IOException {
    Files.createFile(path);
    return new WriteAheadLog(path, new FileOutputStream(path.toFile(), true), syncEachWrite);
  }

  public Path path() {
    return path;
  }

  public long sizeBytes() {
    return sizeBytes;
  }

  public void append(Entry entry) throws IOException {
    ByteBuffer record = encode(entry);
    out.write(record.array(), 0, record.limit());
    sizeBytes += record.limit();
    if (syncEachWrite) {
      out.getFD().sync();
    }
  }

  /** Forces everything appended so far to the storage device. */
  public void sync() throws IOException {
    out.getFD().sync();
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  static ByteBuffer encode(Entry entry) {
    byte[] key = entry.key();
    byte[] value = entry.value();
    int payload = MIN_PAYLOAD_BYTES + key.length + value.length;
    if (payload > MAX_PAYLOAD_BYTES) {
      throw new IllegalArgumentException("record too large: " + payload + " bytes");
    }
    ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + payload);
    buf.position(Integer.BYTES);
    buf.putInt(payload)
        .put(entry.type().code())
        .putLong(entry.seq())
        .putInt(key.length)
        .put(key)
        .putInt(value.length)
        .put(value);
    CRC32 crc = new CRC32();
    crc.update(buf.array(), Integer.BYTES, Integer.BYTES + payload);
    buf.putInt(0, (int) crc.getValue());
    return buf.flip();
  }

  /**
   * Replays every intact record of the log at {@code path}, in append order.
   *
   * <p>Replay stops at the first record that is incomplete (torn by a crash during append) or that
   * fails its checksum. In {@link WalRecoveryMode#TOLERATE_CORRUPTED_TAIL} the remainder is
   * reported as discarded; in {@link WalRecoveryMode#ABSOLUTE_CONSISTENCY} a {@link
   * CorruptionException} is thrown instead.
   */
  public static ReplayResult replay(Path path, WalRecoveryMode mode, Consumer<Entry> sink)
      throws IOException {
    byte[] data = Files.readAllBytes(path);
    ByteBuffer buf = ByteBuffer.wrap(data);
    int pos = 0;
    int records = 0;
    long maxSeq = 0;
    String problem = null;
    while (pos < data.length) {
      int remaining = data.length - pos;
      if (remaining < HEADER_BYTES) {
        problem = "truncated record header";
        break;
      }
      int storedCrc = buf.getInt(pos);
      int payload = buf.getInt(pos + Integer.BYTES);
      if (payload < MIN_PAYLOAD_BYTES || payload > MAX_PAYLOAD_BYTES) {
        problem = "invalid record length " + payload;
        break;
      }
      if (payload > remaining - HEADER_BYTES) {
        problem = "truncated record payload";
        break;
      }
      CRC32 crc = new CRC32();
      crc.update(data, pos + Integer.BYTES, Integer.BYTES + payload);
      if ((int) crc.getValue() != storedCrc) {
        problem = "checksum mismatch";
        break;
      }
      Entry entry = decode(buf, pos + HEADER_BYTES, payload);
      if (entry == null) {
        problem = "malformed record";
        break;
      }
      sink.accept(entry);
      records++;
      maxSeq = Math.max(maxSeq, entry.seq());
      pos += HEADER_BYTES + payload;
    }
    if (problem != null && mode == WalRecoveryMode.ABSOLUTE_CONSISTENCY) {
      throw new CorruptionException(path.getFileName() + " at offset " + pos + ": " + problem);
    }
    return new ReplayResult(records, maxSeq, pos, data.length - pos, problem);
  }

  private static Entry decode(ByteBuffer buf, int offset, int payload) {
    ByteBuffer p = buf.slice(offset, payload);
    ValueType type = ValueType.fromCode(p.get());
    long seq = p.getLong();
    int keyLength = p.getInt();
    if (type == null || keyLength < 0 || keyLength > payload - MIN_PAYLOAD_BYTES) {
      return null;
    }
    byte[] key = new byte[keyLength];
    p.get(key);
    int valueLength = p.getInt();
    if (valueLength != payload - MIN_PAYLOAD_BYTES - keyLength) {
      return null;
    }
    byte[] value = new byte[valueLength];
    p.get(value);
    return new Entry(key, seq, type, value);
  }

  /**
   * Outcome of a replay.
   *
   * @param records intact records delivered to the sink
   * @param maxSeq highest sequence number seen, {@code 0} if none
   * @param validBytes length of the intact prefix of the log
   * @param discardedBytes bytes after the intact prefix that were ignored
   * @param problem why replay stopped early, or {@code null} if the whole log was intact
   */
  public record ReplayResult(
      int records, long maxSeq, long validBytes, long discardedBytes, String problem) {}
}
