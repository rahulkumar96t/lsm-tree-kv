package io.github.rahulkumar96t.lsm.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rahulkumar96t.lsm.util.Bytes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

class MergingIteratorTest {

  private static Entry put(String key, long seq) {
    return Entry.put(Bytes.utf8(key), seq, Bytes.utf8(key + "@" + seq));
  }

  private static List<String> drain(Iterator<Entry> it) {
    List<String> out = new ArrayList<>();
    it.forEachRemaining(
        e -> out.add(e.isTombstone() ? Bytes.string(e.key()) + "@del" : Bytes.string(e.value())));
    return out;
  }

  @Test
  void mergesSortedSourcesNewestVersionWins() {
    List<Entry> old = List.of(put("a", 1), put("c", 2), put("e", 3));
    List<Entry> mid = List.of(put("b", 4), put("c", 5));
    List<Entry> young = List.of(put("a", 7), put("d", 6));

    // Source order must not matter: only sequence numbers decide.
    MergingIterator merged =
        new MergingIterator(List.of(old.iterator(), young.iterator(), mid.iterator()));

    assertThat(drain(merged)).containsExactly("a@7", "b@4", "c@5", "d@6", "e@3");
  }

  @Test
  void passesTombstonesThroughWhenTheyAreNewest() {
    List<Entry> data = List.of(put("a", 1), put("b", 2));
    List<Entry> deletes = List.of(Entry.tombstone(Bytes.utf8("a"), 3, 0));

    assertThat(drain(new MergingIterator(List.of(data.iterator(), deletes.iterator()))))
        .containsExactly("a@del", "b@2");
  }

  @Test
  void handlesEmptyAndNoSources() {
    assertThat(new MergingIterator(List.of()).hasNext()).isFalse();
    List<Entry> one = List.of(put("x", 1));
    assertThat(drain(new MergingIterator(List.of(List.<Entry>of().iterator(), one.iterator()))))
        .containsExactly("x@1");
  }
}
