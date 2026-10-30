package io.github.rahulkumar96t.lsm;

/** How write-ahead-log replay treats an incomplete or corrupt record. */
public enum WalRecoveryMode {
  /**
   * Stop replay at the first incomplete or corrupt record and discard the rest of the log. This is
   * what a crash in the middle of an append produces (a torn last record), so it is the default.
   */
  TOLERATE_CORRUPTED_TAIL,

  /** Fail {@code open} if any record is incomplete or corrupt. */
  ABSOLUTE_CONSISTENCY
}
