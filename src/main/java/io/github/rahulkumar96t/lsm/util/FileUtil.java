package io.github.rahulkumar96t.lsm.util;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/** File-system helpers for durable, atomic file replacement. */
public final class FileUtil {

  private FileUtil() {}

  /**
   * Makes a rename or create in {@code dir} durable by forcing the directory entry. Not every
   * platform allows opening a directory for this; there the call is a best-effort no-op.
   */
  public static void syncDirectory(Path dir) {
    try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
      channel.force(true);
    } catch (IOException | UnsupportedOperationException ignored) {
      // Directory fsync is not supported on this platform/file system.
    }
  }

  /** Atomically replaces {@code target} with {@code source} and syncs the parent directory. */
  public static void atomicMove(Path source, Path target) throws IOException {
    Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    syncDirectory(target.toAbsolutePath().getParent());
  }

  public static void deleteQuietly(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException ignored) {
      // Best effort: a leftover file is cleaned up on the next open.
    }
  }
}
