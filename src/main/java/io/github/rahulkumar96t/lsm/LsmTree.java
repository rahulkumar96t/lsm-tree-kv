package io.github.rahulkumar96t.lsm;

import io.github.rahulkumar96t.lsm.compaction.SizeTieredCompactionPolicy;
import io.github.rahulkumar96t.lsm.compaction.SizeTieredCompactionPolicy.Run;
import io.github.rahulkumar96t.lsm.internal.Entry;
import io.github.rahulkumar96t.lsm.internal.MergingIterator;
import io.github.rahulkumar96t.lsm.manifest.Manifest;
import io.github.rahulkumar96t.lsm.memtable.MemTable;
import io.github.rahulkumar96t.lsm.sstable.SSTable;
import io.github.rahulkumar96t.lsm.sstable.SSTableWriter;
import io.github.rahulkumar96t.lsm.sstable.TableMeta;
import io.github.rahulkumar96t.lsm.util.FileUtil;
import io.github.rahulkumar96t.lsm.wal.WriteAheadLog;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An embeddable log-structured merge-tree key-value store.
 *
 * <h2>Write path</h2>
 *
 * A write gets the next sequence number, is appended to the write-ahead log and inserted into the
 * active memtable. When the memtable reaches {@link LsmOptions#memtableFlushBytes()} it is frozen,
 * a new WAL and memtable take over, and a background thread writes the frozen memtable to an
 * SSTable. Once the manifest lists the new table, the frozen memtable's WAL is deleted.
 *
 * <h2>Read path</h2>
 *
 * A point lookup consults the active memtable, the frozen memtables (newest first) and the SSTables
 * (newest first, skipping tables via key range and bloom filter) and stops at the first hit. A
 * range scan k-way merges all of them; for each key the version with the highest sequence number
 * wins and tombstones are hidden.
 *
 * <h2>Compaction</h2>
 *
 * After every flush a background thread runs {@link SizeTieredCompactionPolicy size-tiered
 * compaction} over adjacent SSTables, which drops shadowed versions and expired tombstones.
 *
 * <h2>Concurrency</h2>
 *
 * All methods are thread-safe. Writes are serialised by a lock; reads are lock-free apart from
 * reference counting on the SSTables they touch. Scans see a stable set of SSTables and frozen
 * memtables but are weakly consistent with respect to concurrent writes to the active memtable.
 *
 * <h2>Errors</h2>
 *
 * I/O failures surface as {@link UncheckedIOException}. A failed WAL append or background
 * flush/compaction puts the engine into a read-only failed state; reopen it to recover from the
 * WAL.
 */
public final class LsmTree implements Closeable {

  /** Largest accepted key. */
  public static final int MAX_KEY_BYTES = 64 * 1024;

  /** Largest accepted value. */
  public static final int MAX_VALUE_BYTES = 64 * 1024 * 1024;

  private static final System.Logger LOG = System.getLogger(LsmTree.class.getName());
  private static final String WAL_SUFFIX = ".wal";
  private static final String TABLE_SUFFIX = ".sst";
  private static final String LOCK_FILE = "LOCK";

  /**
   * Immutable snapshot of the engine's in-memory and on-disk components.
   *
   * @param immutables frozen memtables, newest first
   * @param tables live SSTables, oldest first, with non-overlapping sequence ranges
   */
  private record Version(MemTable active, List<MemTable> immutables, List<SSTable> tables) {}

  private final Path dir;
  private final LsmOptions options;
  private final SizeTieredCompactionPolicy policy;
  private final FileChannel lockChannel;
  private final FileLock fileLock;

  private final ReentrantLock writeLock = new ReentrantLock();
  private final Condition backgroundProgress = writeLock.newCondition();
  private final Object versionLock = new Object();
  private final AtomicLong nextFileNumber = new AtomicLong();
  private volatile Version version;
  private WriteAheadLog wal; // guarded by writeLock
  private long lastSeq; // guarded by writeLock
  private long lastFlushedSeq; // guarded by versionLock

  private final ExecutorService flushExecutor;
  private final ExecutorService compactionExecutor;
  private volatile boolean closed;
  private volatile Throwable backgroundError;

  private final AtomicLong flushes = new AtomicLong();
  private final AtomicLong compactions = new AtomicLong();
  private final AtomicLong compactionBytesRead = new AtomicLong();
  private final AtomicLong compactionBytesWritten = new AtomicLong();
  private final AtomicLong shadowedDropped = new AtomicLong();
  private final AtomicLong tombstonesDropped = new AtomicLong();
  private final AtomicLong writeStalls = new AtomicLong();
  private volatile long walRecordsReplayed;
  private volatile long walBytesDiscarded;

  /** Opens (creating if needed) the database in {@code dir} with default options. */
  public static LsmTree open(Path dir) throws IOException {
    return open(dir, LsmOptions.defaults());
  }

  /**
   * Opens (creating if needed) the database in {@code dir}, replaying any write-ahead logs left by
   * a previous process. Only one {@code LsmTree} may have a directory open at a time.
   */
  public static LsmTree open(Path dir, LsmOptions options) throws IOException {
    Files.createDirectories(dir);
    return new LsmTree(dir, options);
  }

  private LsmTree(Path dir, LsmOptions options) throws IOException {
    this.dir = dir;
    this.options = options;
    this.policy =
        new SizeTieredCompactionPolicy(
            options.compactionMinThreshold(),
            options.compactionMaxThreshold(),
            options.compactionBucketLow(),
            options.compactionBucketHigh(),
            options.compactionSmallTableBytes());
    this.lockChannel =
        FileChannel.open(
            dir.resolve(LOCK_FILE), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    try {
      this.fileLock = lockChannel.tryLock();
    } catch (OverlappingFileLockException e) {
      lockChannel.close();
      throw new IOException(dir + " is already open in this process", e);
    }
    if (fileLock == null) {
      lockChannel.close();
      throw new IOException(dir + " is locked by another process");
    }
    List<SSTable> tables = new ArrayList<>();
    try {
      recover(tables);
    } catch (IOException | RuntimeException e) {
      tables.forEach(SSTable::closeChannel);
      fileLock.release();
      lockChannel.close();
      throw e;
    }
    this.flushExecutor = Executors.newSingleThreadExecutor(daemon("lsm-flush"));
    this.compactionExecutor = Executors.newSingleThreadExecutor(daemon("lsm-compaction"));
    scheduleCompaction();
  }

  // ---- recovery -------------------------------------------------------------------------------

  private void recover(List<SSTable> tables) throws IOException {
    Manifest manifest = Manifest.load(dir).orElse(Manifest.empty());
    Set<Long> live = new HashSet<>(manifest.tableIds());
    long maxSeq = manifest.lastFlushedSeq();
    for (long id : manifest.tableIds()) {
      SSTable table = SSTable.open(id, tablePath(id));
      tables.add(table);
      maxSeq = Math.max(maxSeq, table.meta().maxSeq());
    }

    // Remove leftovers of interrupted flushes/compactions and collect WALs to replay.
    long maxFileId = manifest.nextFileNumber() - 1;
    List<Long> walIds = new ArrayList<>();
    try (Stream<Path> files = Files.list(dir)) {
      for (Path file : (Iterable<Path>) files::iterator) {
        String name = file.getFileName().toString();
        if (name.endsWith(".tmp")) {
          FileUtil.deleteQuietly(file);
        } else if (name.endsWith(TABLE_SUFFIX) || name.endsWith(WAL_SUFFIX)) {
          long id = parseId(name);
          if (id < 0) {
            continue;
          }
          maxFileId = Math.max(maxFileId, id);
          if (name.endsWith(WAL_SUFFIX)) {
            walIds.add(id);
          } else if (!live.contains(id)) {
            FileUtil.deleteQuietly(file); // written but never committed to the manifest
          }
        }
      }
    }
    walIds.sort(null);
    nextFileNumber.set(maxFileId + 1);

    // Replay WALs in creation order, skipping writes that already reached an SSTable.
    long flushedSeq = manifest.lastFlushedSeq();
    MemTable recovered = new MemTable(-1);
    for (long walId : walIds) {
      WriteAheadLog.ReplayResult result =
          WriteAheadLog.replay(
              walPath(walId),
              options.walRecoveryMode(),
              e -> {
                if (e.seq() > flushedSeq) {
                  recovered.put(e);
                }
              });
      walRecordsReplayed += result.records();
      walBytesDiscarded += result.discardedBytes();
      maxSeq = Math.max(maxSeq, result.maxSeq());
      if (result.problem() != null) {
        LOG.log(
            System.Logger.Level.WARNING,
            "WAL {0}: {1}; discarded {2} trailing bytes",
            walId,
            result.problem(),
            result.discardedBytes());
      }
    }
    lastSeq = maxSeq;

    // Persist recovered writes as an SSTable so the old WALs can go.
    if (!recovered.isEmpty()) {
      SSTable table = writeTable(nextFileNumber.getAndIncrement(), recovered.iterator(null, null));
      if (table != null) {
        tables.add(table);
      }
    }
    lastFlushedSeq = maxSeq;
    new Manifest(nextFileNumber.get(), lastFlushedSeq, ids(tables)).store(dir);
    for (long walId : walIds) {
      Files.deleteIfExists(walPath(walId));
    }

    long walId = nextFileNumber.getAndIncrement();
    wal = WriteAheadLog.create(walPath(walId), options.syncWrites());
    version = new Version(new MemTable(walId), List.of(), List.copyOf(tables));
  }

  // ---- writes ---------------------------------------------------------------------------------

  /**
   * Inserts or overwrites {@code key}. Both arrays are copied. If this throws, the write may or may
   * not have been applied.
   */
  public void put(byte[] key, byte[] value) {
    checkKey(key);
    if (value == null || value.length > MAX_VALUE_BYTES) {
      throw new IllegalArgumentException("value must be non-null and at most 64 MiB");
    }
    write(key.clone(), value.clone());
  }

  /** Deletes {@code key} by writing a tombstone. Deleting an absent key is a no-op. */
  public void delete(byte[] key) {
    checkKey(key);
    write(key.clone(), null);
  }

  private void write(byte[] key, byte[] valueOrNullForDelete) {
    writeLock.lock();
    try {
      ensureWritable();
      awaitFlushCapacity();
      long seq = lastSeq + 1;
      Entry entry =
          valueOrNullForDelete == null
              ? Entry.tombstone(key, seq, options.clock().millis())
              : Entry.put(key, seq, valueOrNullForDelete);
      try {
        wal.append(entry);
      } catch (IOException e) {
        fail(e); // a partially written record would hide every later record from recovery
        throw new UncheckedIOException(e);
      }
      lastSeq = seq;
      version.active().put(entry);
      if (version.active().approximateBytes() >= options.memtableFlushBytes()) {
        freezeActiveMemtable(true);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /** Stalls the caller while the maximum number of frozen memtables is waiting to be flushed. */
  private void awaitFlushCapacity() {
    if (version.immutables().size() >= options.maxImmutableMemtables()) {
      writeStalls.incrementAndGet();
      while (version.immutables().size() >= options.maxImmutableMemtables()) {
        awaitBackgroundProgress();
        ensureWritable();
      }
    }
  }

  /** Swaps in a fresh memtable (and WAL) and hands the old one to the flush thread. */
  private void freezeActiveMemtable(boolean openNewWal) {
    try {
      wal.close();
      MemTable fresh;
      if (openNewWal) {
        long walId = nextFileNumber.getAndIncrement();
        wal = WriteAheadLog.create(walPath(walId), options.syncWrites());
        fresh = new MemTable(walId);
      } else {
        wal = null;
        fresh = new MemTable(-1);
      }
      synchronized (versionLock) {
        Version v = version;
        List<MemTable> immutables = new ArrayList<>(v.immutables().size() + 1);
        immutables.add(v.active());
        immutables.addAll(v.immutables());
        version = new Version(fresh, List.copyOf(immutables), v.tables());
      }
      flushExecutor.execute(this::flushOldestImmutable);
    } catch (IOException e) {
      fail(e);
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Flushes the active memtable and waits until every frozen memtable is in an SSTable. After this
   * returns, no data depends on the write-ahead log.
   */
  public void flush() {
    writeLock.lock();
    try {
      ensureWritable();
      awaitFlushCapacity();
      if (!version.active().isEmpty()) {
        freezeActiveMemtable(true);
      }
      while (!version.immutables().isEmpty()) {
        awaitBackgroundProgress();
        ensureWritable();
      }
    } finally {
      writeLock.unlock();
    }
  }

  // ---- reads ----------------------------------------------------------------------------------

  /** The current value of {@code key}, or empty if absent or deleted. */
  public Optional<byte[]> get(byte[] key) {
    checkKey(key);
    Version v = acquire();
    try {
      Entry entry = v.active().get(key);
      for (int i = 0; entry == null && i < v.immutables().size(); i++) {
        entry = v.immutables().get(i).get(key);
      }
      // Tables are ordered by age with disjoint sequence ranges: the newest hit wins.
      for (int i = v.tables().size() - 1; entry == null && i >= 0; i--) {
        entry = v.tables().get(i).get(key);
      }
      return entry == null || entry.isTombstone()
          ? Optional.empty()
          : Optional.of(entry.value().clone());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      release(v);
    }
  }

  /**
   * Iterates live keys with {@code fromInclusive <= key < toExclusive} in ascending order; a {@code
   * null} bound is unbounded. The iterator must be closed.
   */
  public CloseableIterator<KeyValue> scan(byte[] fromInclusive, byte[] toExclusive) {
    Version v = acquire();
    try {
      List<Iterator<Entry>> sources = new ArrayList<>();
      sources.add(v.active().iterator(fromInclusive, toExclusive));
      for (MemTable m : v.immutables()) {
        sources.add(m.iterator(fromInclusive, toExclusive));
      }
      for (SSTable t : v.tables()) {
        sources.add(t.iterator(fromInclusive, toExclusive));
      }
      return new ScanIterator(new MergingIterator(sources), v);
    } catch (RuntimeException e) {
      release(v);
      throw e;
    }
  }

  /** Iterates all live keys in ascending order. The iterator must be closed. */
  public CloseableIterator<KeyValue> scan() {
    return scan(null, null);
  }

  /** Retains every SSTable of the current version, retrying if a compaction swapped it. */
  private Version acquire() {
    for (; ; ) {
      ensureOpen();
      Version v = version;
      List<SSTable> tables = v.tables();
      int retained = 0;
      while (retained < tables.size() && tables.get(retained).tryRetain()) {
        retained++;
      }
      if (retained == tables.size()) {
        return v;
      }
      for (int i = 0; i < retained; i++) {
        tables.get(i).release();
      }
      Thread.onSpinWait();
    }
  }

  private static void release(Version v) {
    for (SSTable t : v.tables()) {
      t.release();
    }
  }

  // ---- compaction -----------------------------------------------------------------------------

  /**
   * Merges all SSTables into one, dropping shadowed versions and expired tombstones. Memtable
   * contents are not included; call {@link #flush()} first to compact everything.
   */
  public void compact() {
    ensureWritable();
    Future<Boolean> result;
    try {
      result = compactionExecutor.submit(() -> compactOnce(true));
    } catch (RejectedExecutionException e) {
      throw new IllegalStateException("database is closed", e);
    }
    try {
      result.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException io) {
        throw new UncheckedIOException(io);
      }
      throw cause instanceof RuntimeException re ? re : new IllegalStateException(cause);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedIOException(
          new InterruptedIOException("interrupted waiting for compaction"));
    }
  }

  private void scheduleCompaction() {
    if (!options.autoCompaction() || closed) {
      return;
    }
    try {
      compactionExecutor.execute(this::compactWhilePossible);
    } catch (RejectedExecutionException ignored) {
      // Closing: compaction is no longer needed.
    }
  }

  private void compactWhilePossible() {
    try {
      while (!closed && backgroundError == null && compactOnce(false)) {
        // Keep going: a compaction can create a new tier that is itself ready to compact.
      }
    } catch (Throwable t) {
      fail(t);
    }
  }

  /** Runs one compaction if the policy picks a run; returns whether it did. */
  private boolean compactOnce(boolean all) throws IOException {
    List<SSTable> snapshot = version.tables();
    Optional<Run> pick =
        all ? SizeTieredCompactionPolicy.all(snapshot.size()) : policy.pick(sizes(snapshot));
    if (pick.isEmpty()) {
      return false;
    }
    Run run = pick.get();
    List<SSTable> inputs = List.copyOf(snapshot.subList(run.start(), run.end()));
    if (inputs.size() == 1 && inputs.getFirst().meta().tombstoneCount() == 0) {
      return false; // nothing to gain from rewriting a single table without tombstones
    }
    // Only this thread removes tables and flushes only append, so the inputs stay live and
    // adjacent until we swap them out below.

    // A tombstone may only be dropped if no older version can exist below it, i.e. the run
    // includes the oldest table, and it has outlived the retention period.
    boolean includesOldest = run.start() == 0;
    long now = options.clock().millis();
    long retentionMillis = options.tombstoneRetention().toMillis();
    long[] droppedTombstones = {0};
    List<Iterator<Entry>> sources = new ArrayList<>();
    long inputEntries = 0;
    long inputBytes = 0;
    for (SSTable t : inputs) {
      sources.add(t.iterator(null, null));
      inputEntries += t.meta().entryCount();
      inputBytes += t.sizeBytes();
    }
    Iterator<Entry> merged =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                    new MergingIterator(sources), Spliterator.ORDERED | Spliterator.SORTED),
                false)
            .filter(
                e -> {
                  boolean drop =
                      includesOldest
                          && e.isTombstone()
                          && now - e.deletedAtMillis() >= retentionMillis;
                  if (drop) {
                    droppedTombstones[0]++;
                  }
                  return !drop;
                })
            .iterator();
    SSTable output = writeTable(nextFileNumber.getAndIncrement(), merged);

    synchronized (versionLock) {
      Version v = version;
      List<SSTable> tables = v.tables();
      int start = tables.indexOf(inputs.getFirst());
      if (start < 0 || !tables.subList(start, start + inputs.size()).equals(inputs)) {
        throw new IllegalStateException("compaction inputs are no longer adjacent");
      }
      List<SSTable> next = new ArrayList<>(tables.subList(0, start));
      if (output != null) {
        next.add(output);
      }
      next.addAll(tables.subList(start + inputs.size(), tables.size()));
      new Manifest(nextFileNumber.get(), lastFlushedSeq, ids(next)).store(dir);
      version = new Version(v.active(), v.immutables(), List.copyOf(next));
    }
    for (SSTable t : inputs) {
      t.markObsolete();
      t.release(); // the version's reference; the file goes once readers are done
    }

    long outputEntries = output == null ? 0 : output.meta().entryCount();
    compactions.incrementAndGet();
    compactionBytesRead.addAndGet(inputBytes);
    compactionBytesWritten.addAndGet(output == null ? 0 : output.sizeBytes());
    tombstonesDropped.addAndGet(droppedTombstones[0]);
    shadowedDropped.addAndGet(inputEntries - outputEntries - droppedTombstones[0]);
    return true;
  }

  // ---- flush ----------------------------------------------------------------------------------

  private void flushOldestImmutable() {
    try {
      MemTable frozen;
      synchronized (versionLock) {
        List<MemTable> immutables = version.immutables();
        if (immutables.isEmpty()) {
          return;
        }
        frozen = immutables.getLast();
      }
      SSTable table =
          frozen.isEmpty()
              ? null
              : writeTable(nextFileNumber.getAndIncrement(), frozen.iterator(null, null));
      synchronized (versionLock) {
        Version v = version;
        List<SSTable> tables = new ArrayList<>(v.tables());
        if (table != null) {
          tables.add(table);
        }
        long flushedSeq = Math.max(lastFlushedSeq, frozen.maxSeq());
        new Manifest(nextFileNumber.get(), flushedSeq, ids(tables)).store(dir);
        lastFlushedSeq = flushedSeq;
        List<MemTable> immutables = new ArrayList<>(v.immutables());
        immutables.remove(frozen);
        version = new Version(v.active(), List.copyOf(immutables), List.copyOf(tables));
      }
      if (frozen.walId() >= 0) {
        FileUtil.deleteQuietly(walPath(frozen.walId()));
      }
      flushes.incrementAndGet();
      scheduleCompaction();
    } catch (Throwable t) {
      fail(t);
    } finally {
      writeLock.lock();
      try {
        backgroundProgress.signalAll();
      } finally {
        writeLock.unlock();
      }
    }
  }

  private SSTable writeTable(long id, Iterator<Entry> entries) throws IOException {
    Path path = tablePath(id);
    TableMeta meta =
        SSTableWriter.write(path, entries, options.blockSizeBytes(), options.bloomBitsPerKey());
    return meta == null ? null : SSTable.open(id, path);
  }

  // ---- lifecycle ------------------------------------------------------------------------------

  public LsmStats stats() {
    Version v = version;
    long entries = 0;
    for (SSTable t : v.tables()) {
      entries += t.meta().entryCount();
    }
    long seq;
    writeLock.lock();
    try {
      seq = lastSeq;
    } finally {
      writeLock.unlock();
    }
    return new LsmStats(
        seq,
        v.active().approximateBytes(),
        v.immutables().size(),
        v.tables().stream().map(SSTable::sizeBytes).toList(),
        entries,
        flushes.get(),
        compactions.get(),
        compactionBytesRead.get(),
        compactionBytesWritten.get(),
        shadowedDropped.get(),
        tombstonesDropped.get(),
        writeStalls.get(),
        walRecordsReplayed,
        walBytesDiscarded);
  }

  /** Flushes the memtable, waits for background work and releases the directory. */
  @Override
  public void close() throws IOException {
    writeLock.lock();
    try {
      if (closed) {
        return;
      }
      if (backgroundError == null && !version.active().isEmpty()) {
        awaitFlushCapacity();
        freezeActiveMemtable(false);
      } else if (wal != null) {
        wal.close();
        if (version.active().isEmpty()) {
          Files.deleteIfExists(wal.path());
        }
        wal = null;
      }
      closed = true;
    } finally {
      writeLock.unlock();
    }
    shutdown(flushExecutor);
    shutdown(compactionExecutor);
    release(version);
    fileLock.release();
    lockChannel.close();
    Throwable error = backgroundError;
    if (error != null) {
      throw new IOException("closed after a background failure", error);
    }
  }

  /** Test hook: waits until every compaction scheduled so far has finished. */
  void awaitBackgroundCompactions() throws Exception {
    compactionExecutor.submit(() -> {}).get();
  }

  /**
   * Test hook: stops the engine as abruptly as a process crash would, without flushing memtables or
   * deleting WALs. File handles are closed so the directory can be reopened in this JVM.
   */
  void simulateCrash() throws IOException {
    closed = true;
    flushExecutor.shutdownNow();
    compactionExecutor.shutdownNow();
    awaitQuietly(flushExecutor);
    awaitQuietly(compactionExecutor);
    writeLock.lock();
    try {
      if (wal != null) {
        wal.close(); // unbuffered: everything appended is already in the OS
        wal = null;
      }
    } finally {
      writeLock.unlock();
    }
    version.tables().forEach(SSTable::closeChannel);
    fileLock.release();
    lockChannel.close();
  }

  private static void shutdown(ExecutorService executor) {
    executor.shutdown();
    awaitQuietly(executor);
  }

  private static void awaitQuietly(ExecutorService executor) {
    boolean interrupted = false;
    while (true) {
      try {
        if (executor.awaitTermination(1, TimeUnit.MINUTES)) {
          break;
        }
        LOG.log(System.Logger.Level.WARNING, "still waiting for background work to finish");
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  // ---- helpers --------------------------------------------------------------------------------

  private void awaitBackgroundProgress() {
    try {
      backgroundProgress.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedIOException(new InterruptedIOException("interrupted while stalled"));
    }
  }

  private void fail(Throwable t) {
    if (backgroundError == null) {
      backgroundError = t;
    }
    // Failures caused by an abrupt shutdown (interrupted background threads) are expected.
    System.Logger.Level level = closed ? System.Logger.Level.DEBUG : System.Logger.Level.ERROR;
    LOG.log(level, "storage failure; database is now read-only", t);
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("database is closed");
    }
  }

  private void ensureWritable() {
    ensureOpen();
    Throwable error = backgroundError;
    if (error != null) {
      throw new UncheckedIOException(
          new IOException("database is read-only after a storage failure", error));
    }
  }

  private static void checkKey(byte[] key) {
    if (key == null || key.length > MAX_KEY_BYTES) {
      throw new IllegalArgumentException("key must be non-null and at most 64 KiB");
    }
  }

  private Path walPath(long id) {
    return dir.resolve(String.format("%06d%s", id, WAL_SUFFIX));
  }

  private Path tablePath(long id) {
    return dir.resolve(String.format("%06d%s", id, TABLE_SUFFIX));
  }

  private static long parseId(String fileName) {
    try {
      return Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
    } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
      return -1;
    }
  }

  private static List<Long> ids(List<SSTable> tables) {
    return tables.stream().map(SSTable::id).toList();
  }

  private static long[] sizes(List<SSTable> tables) {
    return tables.stream().mapToLong(SSTable::sizeBytes).toArray();
  }

  private static ThreadFactory daemon(String name) {
    return r -> {
      Thread t = new Thread(r, name);
      t.setDaemon(true);
      return t;
    };
  }

  /** Hides tombstones, copies arrays and releases the pinned SSTables on close. */
  private static final class ScanIterator implements CloseableIterator<KeyValue> {
    private final Iterator<Entry> merged;
    private final Version pinned;
    private KeyValue next;
    private boolean closed;

    ScanIterator(Iterator<Entry> merged, Version pinned) {
      this.merged = merged;
      this.pinned = pinned;
      advance();
    }

    private void advance() {
      next = null;
      while (merged.hasNext()) {
        Entry e = merged.next();
        if (!e.isTombstone()) {
          next = new KeyValue(e.key().clone(), e.value().clone());
          return;
        }
      }
    }

    @Override
    public boolean hasNext() {
      return !closed && next != null;
    }

    @Override
    public KeyValue next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      KeyValue current = next;
      advance();
      return current;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        release(pinned);
      }
    }
  }
}
