package io.github.rahulkumar96t.lsm;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rahulkumar96t.lsm.util.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Model-based test: a random sequence of operations is applied both to the engine and to a {@link
 * TreeMap}, and every read is compared. Tiny memtables and blocks force frequent flushes,
 * multi-block tables and background compactions; the sequence also reopens the database cleanly and
 * after simulated crashes. A failure message includes the seed and step to replay it.
 */
class RandomizedModelTest {

  private static final int STEPS = 4_000;

  /** Small alphabet including bytes that differ between signed and unsigned ordering. */
  private static final byte[] ALPHABET = {0x00, 0x41, 0x7f, (byte) 0x80, (byte) 0xff};

  @TempDir Path dir;

  @ParameterizedTest(name = "seed {0}")
  @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8})
  void engineMatchesSortedMapModel(long seed) throws IOException {
    SplittableRandom random = new SplittableRandom(seed);
    LsmOptions options =
        LsmOptions.builder()
            .memtableFlushBytes(1024 + random.nextInt(3 * 1024))
            .blockSizeBytes(64 + random.nextInt(256))
            .maxImmutableMemtables(1 + random.nextInt(3))
            .compactionMinThreshold(2 + random.nextInt(3))
            .compactionSmallTableBytes(random.nextBoolean() ? 0 : 2048)
            .bloomBitsPerKey(1 + random.nextInt(12))
            .build();
    TreeMap<byte[], byte[]> model = new TreeMap<>(Bytes.COMPARATOR);
    LsmTree db = LsmTree.open(dir, options);
    try {
      for (int step = 0; step < STEPS; step++) {
        String where = "seed " + seed + ", step " + step;
        int op = random.nextInt(100);
        if (op < 45) {
          byte[] key = randomKey(random);
          byte[] value = randomValue(random);
          db.put(key, value);
          model.put(key, value);
        } else if (op < 60) {
          byte[] key = randomKey(random);
          db.delete(key);
          model.remove(key);
        } else if (op < 82) {
          byte[] key = randomKey(random);
          assertThat(db.get(key).orElse(null)).as(where).isEqualTo(model.get(key));
        } else if (op < 90) {
          byte[] from = random.nextInt(4) == 0 ? null : randomKey(random);
          byte[] to = random.nextInt(4) == 0 ? null : randomKey(random);
          assertThat(render(scan(db, from, to)))
              .as(where)
              .isEqualTo(render(expected(model, from, to)));
        } else if (op < 94) {
          db.flush();
        } else if (op < 96) {
          db.compact();
        } else if (op < 98) {
          db.close();
          db = LsmTree.open(dir, options);
        } else {
          db.simulateCrash(); // nothing acknowledged may be lost: WAL writes are unbuffered
          db = LsmTree.open(dir, options);
        }
      }
      assertThat(render(scan(db, null, null)))
          .as("final state, seed " + seed)
          .isEqualTo(render(expected(model, null, null)));
    } finally {
      db.close();
    }
  }

  private static byte[] randomKey(SplittableRandom random) {
    byte[] key = new byte[1 + random.nextInt(4)];
    for (int i = 0; i < key.length; i++) {
      key[i] = ALPHABET[random.nextInt(ALPHABET.length)];
    }
    return key;
  }

  private static byte[] randomValue(SplittableRandom random) {
    byte[] value = new byte[random.nextInt(64)];
    random.nextBytes(value);
    return value;
  }

  private static List<Map.Entry<byte[], byte[]>> scan(LsmTree db, byte[] from, byte[] to) {
    List<Map.Entry<byte[], byte[]>> out = new ArrayList<>();
    try (CloseableIterator<KeyValue> it = db.scan(from, to)) {
      it.forEachRemaining(kv -> out.add(Map.entry(kv.key(), kv.value())));
    }
    return out;
  }

  private static List<Map.Entry<byte[], byte[]>> expected(
      TreeMap<byte[], byte[]> model, byte[] from, byte[] to) {
    if (from != null && to != null && Bytes.compare(from, to) >= 0) {
      return List.of();
    }
    Map<byte[], byte[]> view =
        from == null && to == null
            ? model
            : from == null
                ? model.headMap(to, false)
                : to == null ? model.tailMap(from, true) : model.subMap(from, true, to, false);
    return new ArrayList<>(view.entrySet());
  }

  private static List<String> render(List<Map.Entry<byte[], byte[]>> entries) {
    HexFormat hex = HexFormat.of();
    return entries.stream()
        .map(e -> hex.formatHex(e.getKey()) + "=" + hex.formatHex(e.getValue()))
        .toList();
  }
}
