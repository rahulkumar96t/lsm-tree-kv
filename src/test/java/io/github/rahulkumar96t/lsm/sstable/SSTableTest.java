package io.github.rahulkumar96t.lsm.sstable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rahulkumar96t.lsm.CorruptionException;
import io.github.rahulkumar96t.lsm.internal.Entry;
import io.github.rahulkumar96t.lsm.util.Bytes;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableTest {

  @TempDir Path dir;

  private static String key(int i) {
    return String.format("key%05d", i);
  }

  /** Even keys 0..2(n-1); every 10th entry is a tombstone. */
  private static List<Entry> entries(int n) {
    List<Entry> entries = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      byte[] k = Bytes.utf8(key(2 * i));
      entries.add(
          i % 10 == 9 ? Entry.tombstone(k, i + 1, 42) : Entry.put(k, i + 1, Bytes.utf8("v" + i)));
    }
    return entries;
  }

  private SSTable write(String name, List<Entry> entries) throws Exception {
    Path path = dir.resolve(name);
    assertThat(SSTableWriter.write(path, entries.iterator(), 256, 10)).isNotNull();
    return SSTable.open(1, path);
  }

  private static List<String> keys(Iterator<Entry> it) {
    List<String> out = new ArrayList<>();
    it.forEachRemaining(e -> out.add(Bytes.string(e.key())));
    return out;
  }

  @Test
  void roundTripsEntriesAcrossManyBlocks() throws Exception {
    List<Entry> entries = entries(1_000);
    SSTable table = write("t.sst", entries);

    assertThat(table.blockCount()).isGreaterThan(10);
    assertThat(table.meta().entryCount()).isEqualTo(1_000);
    assertThat(table.meta().tombstoneCount()).isEqualTo(100);
    assertThat(table.meta().minSeq()).isEqualTo(1);
    assertThat(table.meta().maxSeq()).isEqualTo(1_000);
    List<Entry> read = new ArrayList<>();
    table.iterator(null, null).forEachRemaining(read::add);
    assertThat(read).containsExactlyElementsOf(entries);
    table.release();
  }

  @Test
  void pointLookupsFindPresentKeysAndRejectAbsentOnes() throws Exception {
    List<Entry> entries = entries(1_000);
    SSTable table = write("t.sst", entries);

    for (Entry e : entries) {
      assertThat(table.get(e.key())).isEqualTo(e);
    }
    for (int i = 0; i < 1_000; i++) {
      assertThat(table.get(Bytes.utf8(key(2 * i + 1)))).isNull(); // odd keys were never written
    }
    assertThat(table.get(Bytes.utf8("a"))).isNull(); // below the key range
    assertThat(table.get(Bytes.utf8("z"))).isNull(); // above the key range
    table.release();
  }

  @Test
  void rangeIteratorHonoursInclusiveAndExclusiveBounds() throws Exception {
    SSTable table = write("t.sst", entries(100));

    assertThat(keys(table.iterator(Bytes.utf8(key(10)), Bytes.utf8(key(16)))))
        .containsExactly(key(10), key(12), key(14));
    assertThat(keys(table.iterator(Bytes.utf8(key(11)), Bytes.utf8(key(15)))))
        .containsExactly(key(12), key(14));
    assertThat(keys(table.iterator(Bytes.utf8(key(196)), null)))
        .containsExactly(key(196), key(198));
    assertThat(keys(table.iterator(null, Bytes.utf8(key(3))))).containsExactly(key(0), key(2));
    assertThat(keys(table.iterator(Bytes.utf8("zzz"), null))).isEmpty();
    assertThat(keys(table.iterator(Bytes.utf8(key(20)), Bytes.utf8(key(10))))).isEmpty();
    table.release();
  }

  @Test
  void emptyInputWritesNoFile() throws Exception {
    Path path = dir.resolve("empty.sst");
    assertThat(SSTableWriter.write(path, List.<Entry>of().iterator(), 256, 10)).isNull();
    assertThat(Files.list(dir)).isEmpty();
  }

  @Test
  void rejectsUnsortedInputAndLeavesNoFile() {
    List<Entry> unsorted =
        List.of(
            Entry.put(Bytes.utf8("b"), 1, new byte[0]), Entry.put(Bytes.utf8("a"), 2, new byte[0]));
    assertThatThrownBy(
            () -> SSTableWriter.write(dir.resolve("x.sst"), unsorted.iterator(), 256, 10))
        .isInstanceOf(IllegalStateException.class);
    assertThat(dir.resolve("x.sst")).doesNotExist();
    assertThat(dir.resolve("x.sst.tmp")).doesNotExist();
  }

  @Test
  void detectsCorruptedDataBlock() throws Exception {
    List<Entry> entries = entries(100);
    SSTable table = write("t.sst", entries);
    table.release();
    try (RandomAccessFile f = new RandomAccessFile(dir.resolve("t.sst").toFile(), "rw")) {
      f.seek(20);
      int b = f.read();
      f.seek(20);
      f.write(b ^ 0x01);
    }
    SSTable reopened = SSTable.open(1, dir.resolve("t.sst"));
    assertThatThrownBy(() -> reopened.get(entries.getFirst().key()))
        .isInstanceOf(CorruptionException.class)
        .hasMessageContaining("checksum mismatch in data block 0");
    reopened.release();
  }

  @Test
  void detectsCorruptedFooterAndTruncation() throws Exception {
    write("t.sst", entries(10)).release();
    Path path = dir.resolve("t.sst");
    byte[] original = Files.readAllBytes(path);

    byte[] badMagic = original.clone();
    badMagic[badMagic.length - 6] ^= 0x01;
    Files.write(path, badMagic);
    assertThatThrownBy(() -> SSTable.open(1, path)).isInstanceOf(CorruptionException.class);

    byte[] badIndexHandle = original.clone();
    badIndexHandle[badIndexHandle.length - Footer.SIZE_BYTES + 3] ^= 0x01;
    Files.write(path, badIndexHandle);
    assertThatThrownBy(() -> SSTable.open(1, path))
        .isInstanceOf(CorruptionException.class)
        .hasMessageContaining("footer checksum mismatch");

    Files.write(path, java.util.Arrays.copyOf(original, original.length - 10));
    assertThatThrownBy(() -> SSTable.open(1, path)).isInstanceOf(CorruptionException.class);
  }

  @Test
  void obsoleteTableIsDeletedWhenTheLastReaderReleases() throws Exception {
    SSTable table = write("t.sst", entries(10));
    assertThat(table.tryRetain()).isTrue(); // a reader

    table.markObsolete();
    table.release(); // the owner lets go
    assertThat(dir.resolve("t.sst")).exists();
    assertThat(table.get(Bytes.utf8(key(0)))).isNotNull(); // reader still works

    table.release(); // the reader lets go
    assertThat(dir.resolve("t.sst")).doesNotExist();
    assertThat(table.tryRetain()).isFalse();
  }

  @Test
  void interruptingOneReaderDoesNotBreakTheTableForOthers() throws Exception {
    List<Entry> entries = entries(500);
    SSTable table = write("t.sst", entries);
    CountDownLatch done = new CountDownLatch(1);
    AtomicReference<Throwable> readerFailure = new AtomicReference<>();
    Thread reader =
        new Thread(
            () -> {
              Thread.currentThread().interrupt(); // the next channel read closes the channel
              try {
                table.get(entries.get(250).key());
              } catch (Throwable t) {
                readerFailure.set(t);
              } finally {
                done.countDown();
              }
            });
    reader.start();
    done.await();

    assertThat(readerFailure.get()).isInstanceOf(java.io.InterruptedIOException.class);
    for (Entry e : entries) {
      assertThat(table.get(e.key())).isEqualTo(e); // channel was transparently reopened
    }
    table.release();
  }
}
