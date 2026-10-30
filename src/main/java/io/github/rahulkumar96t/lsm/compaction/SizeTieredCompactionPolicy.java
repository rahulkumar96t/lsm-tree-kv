package io.github.rahulkumar96t.lsm.compaction;

import java.util.Optional;

/**
 * Size-tiered compaction over <em>adjacent</em> tables.
 *
 * <p>Tables are ordered oldest to newest. A run of adjacent tables forms a tier when each table's
 * size is within {@code [bucketLow, bucketHigh]} times the running average of the run; tables
 * smaller than {@code smallTableBytes} are treated as that size, so freshly flushed tables always
 * tier together. Any tier with at least {@code minThreshold} tables is a candidate (capped at the
 * oldest {@code maxThreshold}); the candidate with the fewest bytes wins, which keeps write
 * amplification low by merging small tables first.
 *
 * <p>Restricting tiers to adjacent tables (unlike classic STCS, which buckets by size only) keeps
 * the table list ordered by age with non-overlapping sequence ranges. That lets point lookups stop
 * at the first table that has the key, and makes it safe to drop a tombstone when the run includes
 * the oldest table.
 */
public final class SizeTieredCompactionPolicy {

  private final int minThreshold;
  private final int maxThreshold;
  private final double bucketLow;
  private final double bucketHigh;
  private final long smallTableBytes;

  public SizeTieredCompactionPolicy(
      int minThreshold,
      int maxThreshold,
      double bucketLow,
      double bucketHigh,
      long smallTableBytes) {
    if (minThreshold < 2 || maxThreshold < minThreshold) {
      throw new IllegalArgumentException("need 2 <= minThreshold <= maxThreshold");
    }
    if (!(bucketLow > 0 && bucketLow <= 1 && bucketHigh >= 1)) {
      throw new IllegalArgumentException("need 0 < bucketLow <= 1 <= bucketHigh");
    }
    this.minThreshold = minThreshold;
    this.maxThreshold = maxThreshold;
    this.bucketLow = bucketLow;
    this.bucketHigh = bucketHigh;
    this.smallTableBytes = smallTableBytes;
  }

  /** A half-open range {@code [start, end)} of table positions (oldest first). */
  public record Run(int start, int end) {
    public int length() {
      return end - start;
    }
  }

  /**
   * Chooses the next run to compact.
   *
   * @param sizes table sizes in bytes, ordered oldest to newest
   */
  public Optional<Run> pick(long[] sizes) {
    Run best = null;
    long bestBytes = Long.MAX_VALUE;
    int i = 0;
    while (i < sizes.length) {
      double sum = effective(sizes[i]);
      int j = i + 1;
      while (j < sizes.length) {
        double average = sum / (j - i);
        long size = effective(sizes[j]);
        if (size < average * bucketLow || size > average * bucketHigh) {
          break;
        }
        sum += size;
        j++;
      }
      if (j - i >= minThreshold) {
        int end = i + Math.min(j - i, maxThreshold);
        long bytes = 0;
        for (int k = i; k < end; k++) {
          bytes += sizes[k];
        }
        if (bytes < bestBytes) {
          best = new Run(i, end);
          bestBytes = bytes;
        }
      }
      i = j;
    }
    return Optional.ofNullable(best);
  }

  /** Picks the whole table list, for a manual full compaction; empty if there are no tables. */
  public static Optional<Run> all(int tableCount) {
    return tableCount < 1 ? Optional.empty() : Optional.of(new Run(0, tableCount));
  }

  private long effective(long size) {
    return Math.max(size, smallTableBytes);
  }
}
