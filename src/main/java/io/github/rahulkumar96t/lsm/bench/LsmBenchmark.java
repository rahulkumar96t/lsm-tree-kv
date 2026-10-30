package io.github.rahulkumar96t.lsm.bench;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.rahulkumar96t.lsm.CloseableIterator;
import io.github.rahulkumar96t.lsm.KeyValue;
import io.github.rahulkumar96t.lsm.LsmOptions;
import io.github.rahulkumar96t.lsm.LsmStats;
import io.github.rahulkumar96t.lsm.LsmTree;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.stream.Stream;

/**
 * Single-threaded, single-shot throughput benchmark (not JMH: no forks, no warm-up iterations).
 * Good for spotting regressions and order-of-magnitude costs, not for precise comparisons.
 *
 * <pre>
 * java -cp target/classes io.github.rahulkumar96t.lsm.bench.LsmBenchmark [keys] [valueBytes]
 * </pre>
 */
public final class LsmBenchmark {

  private LsmBenchmark() {}

  public static void main(String[] args) throws IOException {
    int keys = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
    int valueBytes = args.length > 1 ? Integer.parseInt(args[1]) : 100;
    Path dir = Files.createTempDirectory("lsm-bench");
    System.out.printf(
        Locale.ROOT,
        "keys=%,d value=%dB java=%s cpus=%d dir=%s%n",
        keys,
        valueBytes,
        System.getProperty("java.version"),
        Runtime.getRuntime().availableProcessors(),
        dir);
    SplittableRandom random = new SplittableRandom(42);
    byte[] value = new byte[valueBytes];
    try (LsmTree db = LsmTree.open(dir, LsmOptions.defaults())) {
      long t0 = System.nanoTime();
      for (int i = 0; i < keys; i++) {
        db.put(key(i), value);
      }
      db.flush();
      report("fillseq (+final flush)", keys, System.nanoTime() - t0);

      t0 = System.nanoTime();
      for (int i = 0; i < keys; i++) {
        random.nextBytes(value);
        db.put(key(random.nextInt(keys)), value);
      }
      db.flush();
      report("overwrite random (+final flush)", keys, System.nanoTime() - t0);

      int reads = Math.min(keys, 200_000);
      t0 = System.nanoTime();
      int found = 0;
      for (int i = 0; i < reads; i++) {
        found += db.get(key(random.nextInt(keys))).isPresent() ? 1 : 0;
      }
      report("readrandom (hits=" + found + ")", reads, System.nanoTime() - t0);

      // Absent keys that sort between existing ones, so lookups get past the key-range check and
      // exercise the bloom filters.
      t0 = System.nanoTime();
      found = 0;
      for (int i = 0; i < reads; i++) {
        byte[] missing = (String.format("key%010d", random.nextInt(keys)) + "x").getBytes(UTF_8);
        found += db.get(missing).isPresent() ? 1 : 0;
      }
      report("readmissing (hits=" + found + ")", reads, System.nanoTime() - t0);

      t0 = System.nanoTime();
      long scanned = 0;
      try (CloseableIterator<KeyValue> it = db.scan()) {
        while (it.hasNext()) {
          it.next();
          scanned++;
        }
      }
      report("full scan", scanned, System.nanoTime() - t0);

      LsmStats s = db.stats();
      System.out.printf(
          Locale.ROOT,
          "tables=%d tableBytes=%,d flushes=%d compactions=%d compactionBytesWritten=%,d%n",
          s.tableCount(),
          s.tableBytes(),
          s.flushes(),
          s.compactions(),
          s.compactionBytesWritten());
    } finally {
      try (Stream<Path> files = Files.walk(dir)) {
        files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
      }
    }
  }

  private static byte[] key(int i) {
    return String.format("key%010d", i).getBytes(UTF_8);
  }

  private static void report(String name, long ops, long nanos) {
    double seconds = nanos / 1e9;
    System.out.printf(
        Locale.ROOT,
        "%-34s %,12.0f ops/s  %8.2f us/op%n",
        name,
        ops / seconds,
        nanos / 1e3 / Math.max(1, ops));
  }
}
