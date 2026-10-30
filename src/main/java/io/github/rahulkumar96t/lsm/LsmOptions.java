package io.github.rahulkumar96t.lsm;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * Tuning knobs for an {@link LsmTree}. Use {@link #defaults()} or {@link #builder()}.
 *
 * @param memtableFlushBytes approximate memtable size that triggers a flush to an SSTable
 * @param blockSizeBytes target uncompressed size of an SSTable data block
 * @param bloomBitsPerKey bloom filter bits per key (10 gives ~1% false positives)
 * @param maxImmutableMemtables frozen memtables allowed to wait for flushing before writes stall
 * @param syncWrites fsync the WAL on every write (survives power loss, much slower)
 * @param walRecoveryMode how WAL replay treats a corrupt or torn record
 * @param autoCompaction run size-tiered compaction in the background after flushes
 * @param compactionMinThreshold minimum number of similar-sized adjacent tables to compact
 * @param compactionMaxThreshold maximum number of tables merged in one compaction
 * @param compactionBucketLow a table joins a tier if its size is at least this times the average
 * @param compactionBucketHigh ... and at most this times the average
 * @param compactionSmallTableBytes tables below this size are treated as this size when tiering
 * @param tombstoneRetention minimum age before compaction may drop a tombstone
 * @param clock time source for tombstone timestamps (injectable for tests)
 */
public record LsmOptions(
    long memtableFlushBytes,
    int blockSizeBytes,
    int bloomBitsPerKey,
    int maxImmutableMemtables,
    boolean syncWrites,
    WalRecoveryMode walRecoveryMode,
    boolean autoCompaction,
    int compactionMinThreshold,
    int compactionMaxThreshold,
    double compactionBucketLow,
    double compactionBucketHigh,
    long compactionSmallTableBytes,
    Duration tombstoneRetention,
    Clock clock) {

  public LsmOptions {
    Objects.requireNonNull(walRecoveryMode, "walRecoveryMode");
    Objects.requireNonNull(tombstoneRetention, "tombstoneRetention");
    Objects.requireNonNull(clock, "clock");
    require(memtableFlushBytes >= 1024, "memtableFlushBytes must be >= 1024");
    require(blockSizeBytes >= 64, "blockSizeBytes must be >= 64");
    require(bloomBitsPerKey >= 1 && bloomBitsPerKey <= 64, "bloomBitsPerKey must be in [1, 64]");
    require(maxImmutableMemtables >= 1, "maxImmutableMemtables must be >= 1");
    require(compactionMinThreshold >= 2, "compactionMinThreshold must be >= 2");
    require(
        compactionMaxThreshold >= compactionMinThreshold,
        "compactionMaxThreshold must be >= compactionMinThreshold");
    require(compactionBucketLow > 0 && compactionBucketLow <= 1, "compactionBucketLow in (0, 1]");
    require(compactionBucketHigh >= 1, "compactionBucketHigh must be >= 1");
    require(compactionSmallTableBytes >= 0, "compactionSmallTableBytes must be >= 0");
    require(!tombstoneRetention.isNegative(), "tombstoneRetention must not be negative");
  }

  public static LsmOptions defaults() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  /** Mutable builder; every setter returns {@code this}. */
  public static final class Builder {
    private long memtableFlushBytes = 4L << 20;
    private int blockSizeBytes = 4096;
    private int bloomBitsPerKey = 10;
    private int maxImmutableMemtables = 2;
    private boolean syncWrites = false;
    private WalRecoveryMode walRecoveryMode = WalRecoveryMode.TOLERATE_CORRUPTED_TAIL;
    private boolean autoCompaction = true;
    private int compactionMinThreshold = 4;
    private int compactionMaxThreshold = 32;
    private double compactionBucketLow = 0.5;
    private double compactionBucketHigh = 1.5;
    private long compactionSmallTableBytes = 8L << 20;
    private Duration tombstoneRetention = Duration.ZERO;
    private Clock clock = Clock.systemUTC();

    private Builder() {}

    public Builder memtableFlushBytes(long v) {
      memtableFlushBytes = v;
      return this;
    }

    public Builder blockSizeBytes(int v) {
      blockSizeBytes = v;
      return this;
    }

    public Builder bloomBitsPerKey(int v) {
      bloomBitsPerKey = v;
      return this;
    }

    public Builder maxImmutableMemtables(int v) {
      maxImmutableMemtables = v;
      return this;
    }

    public Builder syncWrites(boolean v) {
      syncWrites = v;
      return this;
    }

    public Builder walRecoveryMode(WalRecoveryMode v) {
      walRecoveryMode = v;
      return this;
    }

    public Builder autoCompaction(boolean v) {
      autoCompaction = v;
      return this;
    }

    public Builder compactionMinThreshold(int v) {
      compactionMinThreshold = v;
      return this;
    }

    public Builder compactionMaxThreshold(int v) {
      compactionMaxThreshold = v;
      return this;
    }

    public Builder compactionBucketLow(double v) {
      compactionBucketLow = v;
      return this;
    }

    public Builder compactionBucketHigh(double v) {
      compactionBucketHigh = v;
      return this;
    }

    public Builder compactionSmallTableBytes(long v) {
      compactionSmallTableBytes = v;
      return this;
    }

    public Builder tombstoneRetention(Duration v) {
      tombstoneRetention = v;
      return this;
    }

    public Builder clock(Clock v) {
      clock = v;
      return this;
    }

    public LsmOptions build() {
      return new LsmOptions(
          memtableFlushBytes,
          blockSizeBytes,
          bloomBitsPerKey,
          maxImmutableMemtables,
          syncWrites,
          walRecoveryMode,
          autoCompaction,
          compactionMinThreshold,
          compactionMaxThreshold,
          compactionBucketLow,
          compactionBucketHigh,
          compactionSmallTableBytes,
          tombstoneRetention,
          clock);
    }
  }
}
