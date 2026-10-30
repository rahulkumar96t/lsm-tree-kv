package io.github.rahulkumar96t.lsm.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rahulkumar96t.lsm.CorruptionException;
import io.github.rahulkumar96t.lsm.WalRecoveryMode;
import io.github.rahulkumar96t.lsm.internal.Entry;
import io.github.rahulkumar96t.lsm.util.Bytes;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteAheadLogTest {

  @TempDir Path dir;

  private Path writeLog(List<Entry> entries) throws IOException {
    Path path = dir.resolve("000001.wal");
    try (WriteAheadLog wal = WriteAheadLog.create(path, false)) {
      for (Entry e : entries) {
        wal.append(e);
      }
    }
    return path;
  }

  private static List<Entry> sampleEntries(int n) {
    List<Entry> entries = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      entries.add(
          i % 4 == 0
              ? Entry.tombstone(Bytes.utf8("k" + i), i, 1_000 + i)
              : Entry.put(Bytes.utf8("k" + i), i, Bytes.utf8("value-" + i)));
    }
    return entries;
  }

  private static List<Entry> replay(Path path, WalRecoveryMode mode) throws IOException {
    List<Entry> out = new ArrayList<>();
    WriteAheadLog.replay(path, mode, out::add);
    return out;
  }

  private static void truncate(Path path, long newSize) throws IOException {
    try (RandomAccessFile f = new RandomAccessFile(path.toFile(), "rw")) {
      f.setLength(newSize);
    }
  }

  private static void flipByte(Path path, long offset) throws IOException {
    try (RandomAccessFile f = new RandomAccessFile(path.toFile(), "rw")) {
      f.seek(offset);
      int b = f.read();
      f.seek(offset);
      f.write(b ^ 0xff);
    }
  }

  @Test
  void replaysEveryRecordInOrder() throws IOException {
    List<Entry> entries = sampleEntries(50);
    Path path = writeLog(entries);

    List<Entry> replayed = new ArrayList<>();
    WriteAheadLog.ReplayResult result =
        WriteAheadLog.replay(path, WalRecoveryMode.TOLERATE_CORRUPTED_TAIL, replayed::add);

    assertThat(replayed).containsExactlyElementsOf(entries);
    assertThat(result.records()).isEqualTo(50);
    assertThat(result.maxSeq()).isEqualTo(50);
    assertThat(result.discardedBytes()).isZero();
    assertThat(result.problem()).isNull();
  }

  @Test
  void emptyLogReplaysNothing() throws IOException {
    Path path = writeLog(List.of());
    assertThat(replay(path, WalRecoveryMode.ABSOLUTE_CONSISTENCY)).isEmpty();
  }

  @Test
  void tornLastRecordIsDroppedAtEveryCutPoint() throws IOException {
    List<Entry> entries = sampleEntries(10);
    Path path = writeLog(entries);
    long fullSize = Files.size(path);
    long lastRecordSize = WriteAheadLog.encode(entries.getLast()).remaining();
    byte[] original = Files.readAllBytes(path);

    // Simulate a crash after every possible number of bytes of the last record reached disk.
    for (long cut = fullSize - lastRecordSize; cut < fullSize; cut++) {
      Files.write(path, original);
      truncate(path, cut);
      List<Entry> replayed = new ArrayList<>();
      WriteAheadLog.ReplayResult result =
          WriteAheadLog.replay(path, WalRecoveryMode.TOLERATE_CORRUPTED_TAIL, replayed::add);
      assertThat(replayed).as("cut at %d", cut).containsExactlyElementsOf(entries.subList(0, 9));
      assertThat(result.validBytes()).isEqualTo(fullSize - lastRecordSize);
      assertThat(result.discardedBytes()).isEqualTo(cut - (fullSize - lastRecordSize));
    }
  }

  @Test
  void corruptedPayloadOfLastRecordIsDetectedByChecksum() throws IOException {
    List<Entry> entries = sampleEntries(5);
    Path path = writeLog(entries);
    flipByte(path, Files.size(path) - 2); // inside the last value

    List<Entry> replayed = new ArrayList<>();
    WriteAheadLog.ReplayResult result =
        WriteAheadLog.replay(path, WalRecoveryMode.TOLERATE_CORRUPTED_TAIL, replayed::add);

    assertThat(replayed).containsExactlyElementsOf(entries.subList(0, 4));
    assertThat(result.problem()).isEqualTo("checksum mismatch");
  }

  @Test
  void corruptedLengthFieldIsDetected() throws IOException {
    List<Entry> entries = sampleEntries(3);
    Path path = writeLog(entries);
    long lastRecordStart = Files.size(path) - WriteAheadLog.encode(entries.getLast()).remaining();
    flipByte(path, lastRecordStart + 4); // high byte of the length field

    assertThat(replay(path, WalRecoveryMode.TOLERATE_CORRUPTED_TAIL))
        .containsExactlyElementsOf(entries.subList(0, 2));
  }

  @Test
  void trailingGarbageIsIgnoredInTolerantMode() throws IOException {
    List<Entry> entries = sampleEntries(3);
    Path path = writeLog(entries);
    Files.write(
        path, new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, java.nio.file.StandardOpenOption.APPEND);

    assertThat(replay(path, WalRecoveryMode.TOLERATE_CORRUPTED_TAIL))
        .containsExactlyElementsOf(entries);
  }

  @Test
  void absoluteConsistencyModeRejectsATornRecord() throws IOException {
    Path path = writeLog(sampleEntries(3));
    truncate(path, Files.size(path) - 1);

    assertThatThrownBy(() -> replay(path, WalRecoveryMode.ABSOLUTE_CONSISTENCY))
        .isInstanceOf(CorruptionException.class)
        .hasMessageContaining("truncated record payload");
  }

  @Test
  void createRefusesToOverwriteAnExistingLog() throws IOException {
    Path path = writeLog(sampleEntries(1));
    assertThatThrownBy(() -> WriteAheadLog.create(path, false))
        .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
  }
}
