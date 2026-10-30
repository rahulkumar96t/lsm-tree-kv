package io.github.rahulkumar96t.lsm;

import static io.github.rahulkumar96t.lsm.util.Bytes.utf8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rahulkumar96t.lsm.util.Bytes;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Crash-recovery behaviour. {@link LsmTree#simulateCrash()} stops the engine without flushing or
 * cleaning up, leaving the directory exactly as a killed process would; tests then damage files the
 * way a crash or bad disk could before reopening.
 */
class CrashRecoveryTest {

  @TempDir Path dir;

  private static LsmOptions.Builder small() {
    return LsmOptions.builder().memtableFlushBytes(4 * 1024).blockSizeBytes(256);
  }

  private static Optional<String> get(LsmTree db, String key) {
    return db.get(utf8(key)).map(Bytes::string);
  }

  private List<Path> files(String suffix) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      return files.filter(p -> p.toString().endsWith(suffix)).sorted().toList();
    }
  }

  private Path onlyWal() throws IOException {
    List<Path> wals = files(".wal");
    assertThat(wals).hasSize(1);
    return wals.getFirst();
  }

  private static void truncateBy(Path file, long bytes) throws IOException {
    try (RandomAccessFile f = new RandomAccessFile(file.toFile(), "rw")) {
      f.setLength(f.length() - bytes);
    }
  }

  @Test
  void unflushedWritesAreReplayedFromTheWal() throws IOException {
    LsmTree db = LsmTree.open(dir, LsmOptions.defaults());
    for (int i = 0; i < 100; i++) {
      db.put(utf8("key" + i), utf8("value" + i));
    }
    db.delete(utf8("key5"));
    db.simulateCrash();

    try (LsmTree reopened = LsmTree.open(dir, LsmOptions.defaults())) {
      assertThat(reopened.stats().walRecordsReplayed()).isEqualTo(101);
      assertThat(reopened.stats().walBytesDiscarded()).isZero();
      assertThat(get(reopened, "key0")).contains("value0");
      assertThat(get(reopened, "key99")).contains("value99");
      assertThat(get(reopened, "key5")).isEmpty();
      assertThat(reopened.stats().lastSequence()).isEqualTo(101);
      reopened.put(utf8("after"), utf8("crash")); // sequence numbers continue
      assertThat(reopened.stats().lastSequence()).isEqualTo(102);
    }
  }

  @Test
  void tornLastRecordLosesOnlyTheLastWrite() throws IOException {
    LsmTree db = LsmTree.open(dir, LsmOptions.defaults());
    for (int i = 0; i < 20; i++) {
      db.put(utf8("key" + i), utf8("value" + i));
    }
    db.simulateCrash();
    truncateBy(onlyWal(), 3); // the crash hit in the middle of the last append

    try (LsmTree reopened = LsmTree.open(dir, LsmOptions.defaults())) {
      assertThat(reopened.stats().walRecordsReplayed()).isEqualTo(19);
      assertThat(reopened.stats().walBytesDiscarded()).isPositive();
      for (int i = 0; i < 19; i++) {
        assertThat(get(reopened, "key" + i)).contains("value" + i);
      }
      assertThat(get(reopened, "key19")).isEmpty();
    }
  }

  @Test
  void corruptLastRecordIsDiscardedButEarlierRecordsSurvive() throws IOException {
    LsmTree db = LsmTree.open(dir, LsmOptions.defaults());
    db.put(utf8("a"), utf8("1"));
    db.put(utf8("b"), utf8("2"));
    db.put(utf8("a"), utf8("3"));
    db.simulateCrash();
    Path wal = onlyWal();
    try (RandomAccessFile f = new RandomAccessFile(wal.toFile(), "rw")) {
      f.seek(f.length() - 1);
      f.write('X'); // bit rot in the last value
    }

    try (LsmTree reopened = LsmTree.open(dir, LsmOptions.defaults())) {
      assertThat(get(reopened, "a")).contains("1"); // the overwrite was lost, not half-applied
      assertThat(get(reopened, "b")).contains("2");
    }
  }

  @Test
  void absoluteConsistencyModeRefusesToOpenWithATornWal() throws IOException {
    LsmTree db = LsmTree.open(dir, LsmOptions.defaults());
    db.put(utf8("a"), utf8("1"));
    db.simulateCrash();
    truncateBy(onlyWal(), 1);

    LsmOptions strict =
        LsmOptions.builder().walRecoveryMode(WalRecoveryMode.ABSOLUTE_CONSISTENCY).build();
    assertThatThrownBy(() -> LsmTree.open(dir, strict)).isInstanceOf(CorruptionException.class);
    // The failed open released the directory lock and left the WAL for a tolerant open.
    try (LsmTree reopened = LsmTree.open(dir, LsmOptions.defaults())) {
      assertThat(get(reopened, "a")).isEmpty();
    }
  }

  @Test
  void staleWalLeftAfterAFlushDoesNotResurrectOldValues() throws IOException {
    LsmOptions options = small().autoCompaction(false).build();
    LsmTree db = LsmTree.open(dir, options);
    db.put(utf8("k"), utf8("v1"));
    Path firstWal = onlyWal();
    byte[] firstWalBytes = Files.readAllBytes(firstWal);
    db.flush(); // v1 is now in an SSTable and the WAL is deleted
    db.put(utf8("k"), utf8("v2"));
    db.flush();
    db.simulateCrash();
    // Simulate a crash between "manifest updated" and "old WAL deleted".
    Files.write(firstWal, firstWalBytes);

    try (LsmTree reopened = LsmTree.open(dir, options)) {
      assertThat(get(reopened, "k")).contains("v2");
      assertThat(reopened.stats().walRecordsReplayed()).isEqualTo(1); // read, but skipped
    }
  }

  @Test
  void leftoversOfAnInterruptedFlushAreRemoved() throws IOException {
    LsmTree db = LsmTree.open(dir, small().build());
    db.put(utf8("k"), utf8("v"));
    db.flush();
    db.simulateCrash();
    Files.write(dir.resolve("000900.sst"), new byte[] {1, 2, 3}); // renamed, not in manifest
    Files.write(dir.resolve("000901.sst.tmp"), new byte[] {4, 5, 6}); // never renamed

    try (LsmTree reopened = LsmTree.open(dir, small().build())) {
      assertThat(get(reopened, "k")).contains("v");
      assertThat(dir.resolve("000900.sst")).doesNotExist();
      assertThat(dir.resolve("000901.sst.tmp")).doesNotExist();
      reopened.put(utf8("next"), utf8("file")); // file numbers stay unique after recovery
      reopened.flush();
      assertThat(get(reopened, "next")).contains("file");
    }
  }

  @Test
  void repeatedCrashesDuringRecoveryLoseNothing() throws IOException {
    LsmTree db = LsmTree.open(dir, small().build());
    for (int i = 0; i < 300; i++) {
      db.put(utf8("key" + i), utf8("value" + i));
    }
    db.simulateCrash();
    for (int round = 0; round < 3; round++) {
      LsmTree again = LsmTree.open(dir, small().build());
      again.put(utf8("round" + round), utf8("x"));
      again.simulateCrash();
    }
    try (LsmTree reopened = LsmTree.open(dir, small().build())) {
      for (int i = 0; i < 300; i++) {
        assertThat(get(reopened, "key" + i)).contains("value" + i);
      }
      assertThat(get(reopened, "round2")).contains("x");
    }
  }

  @Test
  void crashAtArbitraryPointsDuringBackgroundWorkKeepsAcknowledgedWrites() throws IOException {
    SplittableRandom random = new SplittableRandom(7);
    LsmOptions options = small().compactionMinThreshold(2).compactionSmallTableBytes(0).build();
    int written = 0;
    for (int round = 0; round < 8; round++) {
      LsmTree db = LsmTree.open(dir, options);
      int writes = 200 + random.nextInt(400);
      for (int i = 0; i < writes; i++, written++) {
        db.put(utf8("key" + written), utf8("value" + written));
        if (random.nextInt(50) == 0) {
          db.delete(utf8("key" + random.nextInt(written + 1)));
        }
      }
      db.simulateCrash(); // flushes/compactions may be mid-flight
    }
    try (LsmTree db = LsmTree.open(dir, options)) {
      SplittableRandom replay = new SplittableRandom(7);
      java.util.Set<String> deleted = new java.util.HashSet<>();
      int n = 0;
      for (int round = 0; round < 8; round++) {
        int writes = 200 + replay.nextInt(400);
        for (int i = 0; i < writes; i++, n++) {
          deleted.remove("key" + n);
          if (replay.nextInt(50) == 0) {
            deleted.add("key" + replay.nextInt(n + 1));
          }
        }
      }
      for (int i = 0; i < written; i++) {
        String key = "key" + i;
        if (deleted.contains(key)) {
          assertThat(get(db, key)).as(key).isEmpty();
        } else {
          assertThat(get(db, key)).as(key).contains("value" + i);
        }
      }
    }
  }
}
