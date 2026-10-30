package io.github.rahulkumar96t.lsm.manifest;

import io.github.rahulkumar96t.lsm.CorruptionException;
import io.github.rahulkumar96t.lsm.util.FileUtil;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * The engine's durable catalogue: which SSTables are live (oldest first), the next file number to
 * allocate, and the highest sequence number already persisted in SSTables.
 *
 * <p>Stored as a small text file that is rewritten in full on every change (write to a temp file,
 * fsync, atomic rename), so a crash leaves either the old or the new manifest, never a mix. A
 * trailing CRC32 line detects corruption.
 *
 * <pre>
 * lsm-tree-kv manifest v1
 * next-file-number 17
 * last-flushed-seq 1234
 * tables 5 9 12
 * crc32 1a2b3c4d
 * </pre>
 *
 * @param nextFileNumber next id for a WAL or SSTable file
 * @param lastFlushedSeq every write with a sequence number up to this one is in an SSTable, so WAL
 *     replay skips it
 * @param tableIds live SSTable ids, ordered oldest to newest
 */
public record Manifest(long nextFileNumber, long lastFlushedSeq, List<Long> tableIds) {

  public static final String FILE_NAME = "MANIFEST";
  private static final String HEADER = "lsm-tree-kv manifest v1";

  public Manifest {
    tableIds = List.copyOf(tableIds);
  }

  public static Manifest empty() {
    return new Manifest(1, 0, List.of());
  }

  /** Loads the manifest from {@code dir}, or returns empty if the database is new. */
  public static Optional<Manifest> load(Path dir) throws IOException {
    Path file = dir.resolve(FILE_NAME);
    if (!Files.exists(file)) {
      return Optional.empty();
    }
    String text = Files.readString(file, StandardCharsets.UTF_8);
    int crcLine = text.lastIndexOf("crc32 ");
    if (crcLine < 0) {
      throw new CorruptionException("manifest has no checksum");
    }
    String body = text.substring(0, crcLine);
    String storedCrc = text.substring(crcLine + "crc32 ".length()).strip();
    if (!storedCrc.equals(crcHex(body))) {
      throw new CorruptionException("manifest checksum mismatch");
    }
    try {
      List<String> lines = body.lines().toList();
      if (lines.size() != 4 || !lines.get(0).equals(HEADER)) {
        throw new CorruptionException("manifest has an unexpected layout");
      }
      long next = Long.parseLong(value(lines.get(1), "next-file-number"));
      long flushed = Long.parseLong(value(lines.get(2), "last-flushed-seq"));
      List<Long> ids = new ArrayList<>();
      String tables = value(lines.get(3), "tables");
      if (!tables.isEmpty()) {
        for (String id : tables.split(" ")) {
          ids.add(Long.parseLong(id));
        }
      }
      return Optional.of(new Manifest(next, flushed, ids));
    } catch (NumberFormatException e) {
      throw new CorruptionException("manifest has a malformed number: " + e.getMessage());
    }
  }

  /** Durably replaces the manifest in {@code dir}. */
  public void store(Path dir) throws IOException {
    StringBuilder body = new StringBuilder();
    body.append(HEADER).append('\n');
    body.append("next-file-number ").append(nextFileNumber).append('\n');
    body.append("last-flushed-seq ").append(lastFlushedSeq).append('\n');
    body.append("tables");
    for (long id : tableIds) {
      body.append(' ').append(id);
    }
    body.append('\n');
    String text = body + "crc32 " + crcHex(body.toString()) + "\n";

    Path tmp = dir.resolve(FILE_NAME + ".tmp");
    try (FileOutputStream out = new FileOutputStream(tmp.toFile())) {
      out.write(text.getBytes(StandardCharsets.UTF_8));
      out.getFD().sync();
    }
    FileUtil.atomicMove(tmp, dir.resolve(FILE_NAME));
  }

  private static String value(String line, String key) throws CorruptionException {
    if (line.equals(key)) {
      return "";
    }
    if (!line.startsWith(key + " ")) {
      throw new CorruptionException("manifest: expected '" + key + "'");
    }
    return line.substring(key.length() + 1);
  }

  private static String crcHex(String body) {
    CRC32 crc = new CRC32();
    crc.update(body.getBytes(StandardCharsets.UTF_8));
    return String.format("%08x", crc.getValue());
  }
}
