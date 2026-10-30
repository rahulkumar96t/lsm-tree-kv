package io.github.rahulkumar96t.lsm;

import java.util.Iterator;

/**
 * An iterator that pins resources (here: SSTable files) until closed. Always use it in a
 * try-with-resources block; an unclosed scan keeps compacted-away files on disk.
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {
  @Override
  void close();
}
