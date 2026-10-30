package io.github.rahulkumar96t.lsm;

import java.io.IOException;

/** On-disk data failed a checksum or structural check. */
public class CorruptionException extends IOException {
  private static final long serialVersionUID = 1L;

  public CorruptionException(String message) {
    super(message);
  }
}
