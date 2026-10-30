package io.github.rahulkumar96t.lsm;

import static io.github.rahulkumar96t.lsm.util.Bytes.string;
import static io.github.rahulkumar96t.lsm.util.Bytes.utf8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class LsmTreeTest {

  @TempDir Path dir;

  /** Small memtables and blocks so a few hundred writes exercise flushes and multiple blocks. */
  private static LsmOptions.Builder small() {
    return LsmOptions.builder()
        .memtableFlushBytes(4 * 1024)
        .blockSizeBytes(256)
        .compactionSmallTableBytes(1024);
  }

  private static Optional<String> get(LsmTree db, String key) {
    return db.get(utf8(key)).map(io.github.rahulkumar96t.lsm.util.Bytes::string);
  }

  private static List<String> scan(LsmTree db, String from, String to) {
    List<String> out = new ArrayList<>();
    try (CloseableIterator<KeyValue> it =
        db.scan(from == null ? null : utf8(from), to == null ? null : utf8(to))) {
      it.forEachRemaining(kv -> out.add(string(kv.key()) + "=" + string(kv.value())));
    }
    return out;
  }

  private long fileCount(String suffix) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      return files.filter(p -> p.toString().endsWith(suffix)).count();
    }
  }

  @Test
  void putGetOverwriteDelete() throws IOException {
    try (LsmTree db = LsmTree.open(dir)) {
      db.put(utf8("a"), utf8("1"));
      db.put(utf8("b"), utf8("2"));
      db.put(utf8("a"), utf8("3"));
      db.delete(utf8("b"));
      db.delete(utf8("never-existed"));

      assertThat(get(db, "a")).contains("3");
      assertThat(get(db, "b")).isEmpty();
      assertThat(get(db, "c")).isEmpty();
      assertThat(db.stats().lastSequence()).isEqualTo(5);
    }
  }

  @Test
  void callerMutationsDoNotLeakIntoTheStore() throws IOException {
    try (LsmTree db = LsmTree.open(dir)) {
      byte[] key = utf8("k");
      byte[] value = utf8("v");
      db.put(key, value);
      key[0] = 'x';
      value[0] = 'x';
      byte[] read = db.get(utf8("k")).orElseThrow();
      read[0] = 'y';

      assertThat(get(db, "k")).contains("v");
    }
  }

  @Test
  void supportsEmptyAndBinaryKeysInUnsignedOrder() throws IOException {
    try (LsmTree db = LsmTree.open(dir, small().build())) {
      db.put(new byte[0], utf8("empty"));
      db.put(new byte[] {(byte) 0xff}, utf8("high"));
      db.put(new byte[] {0x00}, utf8("zero"));
      db.put(new byte[] {0x7f}, utf8("mid"));
      db.flush();

      assertThat(db.get(new byte[0]).map(io.github.rahulkumar96t.lsm.util.Bytes::string))
          .contains("empty");
      List<String> values = new ArrayList<>();
      try (CloseableIterator<KeyValue> it = db.scan()) {
        it.forEachRemaining(kv -> values.add(string(kv.value())));
      }
      assertThat(values).containsExactly("empty", "zero", "mid", "high");
    }
  }

  @Test
  void scanMergesMemtableAndTablesHonouringBoundsAndDeletes() throws IOException {
    try (LsmTree db = LsmTree.open(dir, small().autoCompaction(false).build())) {
      for (String k : List.of("a", "b", "c", "d", "e")) {
        db.put(utf8(k), utf8(k + "1"));
      }
      db.flush();
      db.put(utf8("b"), utf8("b2")); // newer version in the memtable
      db.delete(utf8("d")); // tombstone in the memtable shadows the table
      db.put(utf8("f"), utf8("f1"));

      assertThat(scan(db, null, null)).containsExactly("a=a1", "b=b2", "c=c1", "e=e1", "f=f1");
      assertThat(scan(db, "b", "e")).containsExactly("b=b2", "c=c1");
      assertThat(scan(db, "c", null)).containsExactly("c=c1", "e=e1", "f=f1");
      assertThat(scan(db, null, "b")).containsExactly("a=a1");
      assertThat(scan(db, "x", null)).isEmpty();
    }
  }

  @Test
  void flushPersistsMemtableAndRemovesItsWal() throws IOException {
    try (LsmTree db = LsmTree.open(dir, small().autoCompaction(false).build())) {
      for (int i = 0; i < 1_000; i++) {
        db.put(utf8("key" + i), utf8("value" + i));
      }
      db.flush();
      LsmStats stats = db.stats();

      assertThat(stats.flushes()).isGreaterThan(5);
      assertThat(stats.tableCount()).isEqualTo((int) stats.flushes());
      assertThat(stats.immutableMemtables()).isZero();
      assertThat(stats.memtableBytes()).isZero();
      assertThat(fileCount(".wal")).isEqualTo(1); // only the fresh, active WAL remains
      for (int i = 0; i < 1_000; i += 37) {
        assertThat(get(db, "key" + i)).contains("value" + i);
      }
    }
  }

  @Test
  void dataSurvivesCleanReopen() throws IOException {
    try (LsmTree db = LsmTree.open(dir, small().build())) {
      for (int i = 0; i < 500; i++) {
        db.put(utf8("key" + i), utf8("value" + i));
      }
      db.delete(utf8("key7"));
    }
    assertThat(fileCount(".wal")).isZero(); // close flushes everything

    try (LsmTree db = LsmTree.open(dir, small().build())) {
      assertThat(get(db, "key0")).contains("value0");
      assertThat(get(db, "key499")).contains("value499");
      assertThat(get(db, "key7")).isEmpty();
      assertThat(scan(db, null, null)).hasSize(499);
      assertThat(db.stats().lastSequence()).isEqualTo(501);
      assertThat(db.stats().walRecordsReplayed()).isZero();
    }
  }

  @Test
  void fullCompactionDropsShadowedVersions() throws IOException {
    try (LsmTree db = LsmTree.open(dir, small().autoCompaction(false).build())) {
      for (int round = 0; round < 3; round++) {
        for (int i = 0; i < 100; i++) {
          db.put(utf8("key" + i), utf8("round" + round));
        }
        db.flush();
      }
      assertThat(db.stats().tableEntries()).isEqualTo(300);

      db.compact();
      LsmStats stats = db.stats();

      assertThat(stats.tableCount()).isEqualTo(1);
      assertThat(stats.tableEntries()).isEqualTo(100);
      assertThat(stats.shadowedVersionsDropped()).isEqualTo(200);
      assertThat(stats.compactionBytesWritten()).isLessThan(stats.compactionBytesRead());
      assertThat(get(db, "key42")).contains("round2");
      try (Stream<Path> files = Files.list(dir)) {
        assertThat(files.filter(p -> p.toString().endsWith(".sst"))).hasSize(1);
      }
    }
  }

  @Test
  void tombstonesAreKeptUntilRetentionExpires() throws IOException {
    MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    LsmOptions options =
        small().autoCompaction(false).tombstoneRetention(Duration.ofHours(1)).clock(clock).build();
    try (LsmTree db = LsmTree.open(dir, options)) {
      db.put(utf8("k"), utf8("v"));
      db.flush();
      db.delete(utf8("k"));
      db.flush();

      db.compact();
      assertThat(db.stats().tombstonesDropped()).isZero();
      assertThat(db.stats().tableEntries()).isEqualTo(1); // just the tombstone
      assertThat(get(db, "k")).isEmpty();

      clock.advance(Duration.ofHours(2));
      db.compact();
      assertThat(db.stats().tombstonesDropped()).isEqualTo(1);
      assertThat(db.stats().tableCount()).isZero();
      assertThat(get(db, "k")).isEmpty();
    }
  }

  @Test
  void compactionThatSkipsTheOldestTableKeepsTombstones() throws Exception {
    LsmOptions options = small().compactionMinThreshold(4).build();
    try (LsmTree db = LsmTree.open(dir, options)) {
      // One large, old table holding the original value of "victim".
      db.put(utf8("victim"), utf8("old"));
      for (int i = 0; i < 2_000; i++) {
        db.put(utf8("bulk" + i), new byte[16]);
      }
      db.compact(); // make the bulk a single big table
      db.flush();
      db.compact();
      // Four small tables, one of them deleting "victim": they tier together, the big one not.
      for (int t = 0; t < 4; t++) {
        db.put(utf8("small" + t), utf8("x"));
        if (t == 2) {
          db.delete(utf8("victim"));
        }
        db.flush();
      }
      db.awaitBackgroundCompactions();

      LsmStats stats = db.stats();
      assertThat(stats.compactions()).isGreaterThanOrEqualTo(2);
      assertThat(stats.tableCount()).isEqualTo(2);
      assertThat(stats.tombstonesDropped()).isZero();
      assertThat(get(db, "victim")).isEmpty(); // dropping the tombstone would resurrect "old"
    }
  }

  @Test
  void openScanKeepsCompactedTablesAliveUntilClosed() throws IOException {
    try (LsmTree db = LsmTree.open(dir, small().autoCompaction(false).build())) {
      for (int t = 0; t < 3; t++) {
        for (int i = 0; i < 50; i++) {
          db.put(utf8(String.format("k%03d", i)), utf8("t" + t));
        }
        db.flush();
      }
      CloseableIterator<KeyValue> scan = db.scan();
      db.compact(); // replaces the three tables the scan is reading
      assertThat(fileCount(".sst")).isEqualTo(4);

      int count = 0;
      while (scan.hasNext()) {
        assertThat(string(scan.next().value())).isEqualTo("t2");
        count++;
      }
      scan.close();
      assertThat(count).isEqualTo(50);
      assertThat(fileCount(".sst")).isEqualTo(1);
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void concurrentWritersReadersAndScansStayConsistent() throws Exception {
    LsmOptions options = small().maxImmutableMemtables(1).build();
    int writers = 4;
    int keysPerWriter = 400;
    int versions = 3;
    ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
    try (LsmTree db = LsmTree.open(dir, options)) {
      CountDownLatch start = new CountDownLatch(1);
      List<Thread> threads = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        int writer = w;
        threads.add(
            Thread.ofPlatform()
                .start(
                    () -> {
                      try {
                        start.await();
                        for (int v = 0; v < versions; v++) {
                          for (int i = 0; i < keysPerWriter; i++) {
                            db.put(utf8("w" + writer + "-" + i), utf8("v" + v));
                          }
                        }
                      } catch (Throwable t) {
                        failures.add(t);
                      }
                    }));
      }
      for (int r = 0; r < 2; r++) {
        threads.add(
            Thread.ofPlatform()
                .start(
                    () -> {
                      try {
                        start.await();
                        ThreadLocalRandom random = ThreadLocalRandom.current();
                        for (int n = 0; n < 3_000; n++) {
                          String key =
                              "w" + random.nextInt(writers) + "-" + random.nextInt(keysPerWriter);
                          get(db, key).ifPresent(v -> assertThat(v).matches("v[0-2]"));
                          if (n % 500 == 0) {
                            List<String> keys = new ArrayList<>();
                            try (CloseableIterator<KeyValue> it = db.scan()) {
                              it.forEachRemaining(kv -> keys.add(string(kv.key())));
                            }
                            assertThat(keys).isSorted().doesNotHaveDuplicates();
                          }
                        }
                      } catch (Throwable t) {
                        failures.add(t);
                      }
                    }));
      }
      start.countDown();
      for (Thread t : threads) {
        t.join();
      }
      assertThat(failures).isEmpty();
      for (int w = 0; w < writers; w++) {
        for (int i = 0; i < keysPerWriter; i++) {
          assertThat(get(db, "w" + w + "-" + i)).contains("v" + (versions - 1));
        }
      }
      assertThat(db.stats().flushes()).isPositive();
    }
  }

  @Test
  void directoryCanOnlyBeOpenedOnce() throws IOException {
    try (LsmTree db = LsmTree.open(dir)) {
      assertThatThrownBy(() -> LsmTree.open(dir))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("already open");
      db.put(utf8("k"), utf8("v"));
    }
    try (LsmTree db = LsmTree.open(dir)) {
      assertThat(get(db, "k")).contains("v");
    }
  }

  @Test
  void rejectsInvalidArgumentsAndUseAfterClose() throws IOException {
    LsmTree db = LsmTree.open(dir);
    assertThatThrownBy(() -> db.put(null, utf8("v"))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> db.put(utf8("k"), null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> db.get(new byte[LsmTree.MAX_KEY_BYTES + 1]))
        .isInstanceOf(IllegalArgumentException.class);
    db.close();
    db.close(); // idempotent
    assertThatThrownBy(() -> db.put(utf8("k"), utf8("v")))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> db.get(utf8("k"))).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(db::scan).isInstanceOf(IllegalStateException.class);
  }
}
