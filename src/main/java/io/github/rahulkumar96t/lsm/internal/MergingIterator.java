package io.github.rahulkumar96t.lsm.internal;

import io.github.rahulkumar96t.lsm.util.Bytes;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * K-way merge of sorted entry streams with newest-wins semantics.
 *
 * <p>Every source must yield entries in ascending key order with at most one entry per key (true
 * for memtables and SSTables). The merged stream is ascending by key and contains exactly one entry
 * per key: the one with the highest sequence number across all sources. Older versions are consumed
 * and discarded. Tombstones are passed through; callers decide whether to hide or keep them.
 *
 * <p>Cost: O(log k) per entry consumed from a source, for k sources.
 */
public final class MergingIterator implements Iterator<Entry> {

  /** Ascending key; for equal keys the newest version (highest seq) comes first. */
  private static final Comparator<Head> ORDER =
      (a, b) -> {
        int c = Bytes.compare(a.current.key(), b.current.key());
        return c != 0 ? c : Long.compare(b.current.seq(), a.current.seq());
      };

  private final PriorityQueue<Head> heap;

  public MergingIterator(List<? extends Iterator<Entry>> sources) {
    this.heap = new PriorityQueue<>(Math.max(1, sources.size()), ORDER);
    for (Iterator<Entry> source : sources) {
      if (source.hasNext()) {
        heap.add(new Head(source.next(), source));
      }
    }
  }

  @Override
  public boolean hasNext() {
    return !heap.isEmpty();
  }

  @Override
  public Entry next() {
    Head top = heap.poll();
    if (top == null) {
      throw new NoSuchElementException();
    }
    Entry newest = top.current;
    advance(top);
    // Every other head positioned on the same key holds an older version: skip it.
    while (!heap.isEmpty() && Bytes.compare(heap.peek().current.key(), newest.key()) == 0) {
      advance(heap.poll());
    }
    return newest;
  }

  private void advance(Head head) {
    if (head.source.hasNext()) {
      head.current = head.source.next();
      heap.add(head);
    }
  }

  private static final class Head {
    Entry current;
    final Iterator<Entry> source;

    Head(Entry current, Iterator<Entry> source) {
      this.current = current;
      this.source = source;
    }
  }
}
