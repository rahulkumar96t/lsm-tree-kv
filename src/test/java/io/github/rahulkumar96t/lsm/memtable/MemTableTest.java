package io.github.rahulkumar96t.lsm.memtable;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rahulkumar96t.lsm.internal.Entry;
import io.github.rahulkumar96t.lsm.util.Bytes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

class MemTableTest {

  private static List<String> keys(Iterator<Entry> it) {
    List<String> out = new ArrayList<>();
    it.forEachRemaining(e -> out.add(Bytes.string(e.key())));
    return out;
  }

  @Test
  void keepsOnlyTheLatestVersionPerKey() {
    MemTable m = new MemTable(1);
    m.put(Entry.put(Bytes.utf8("a"), 1, Bytes.utf8("v1")));
    m.put(Entry.put(Bytes.utf8("a"), 2, Bytes.utf8("v2")));
    m.put(Entry.tombstone(Bytes.utf8("b"), 3, 0));

    assertThat(Bytes.string(m.get(Bytes.utf8("a")).value())).isEqualTo("v2");
    assertThat(m.get(Bytes.utf8("b")).isTombstone()).isTrue();
    assertThat(m.get(Bytes.utf8("c"))).isNull();
    assertThat(m.size()).isEqualTo(2);
    assertThat(m.maxSeq()).isEqualTo(3);
  }

  @Test
  void iteratesInUnsignedKeyOrderWithinBounds() {
    MemTable m = new MemTable(1);
    long seq = 0;
    for (String k : List.of("d", "a", "c", "e", "b")) {
      m.put(Entry.put(Bytes.utf8(k), ++seq, Bytes.utf8(k)));
    }
    m.put(Entry.put(new byte[] {(byte) 0xff}, ++seq, new byte[0]));

    assertThat(keys(m.iterator(null, null))).containsExactly("a", "b", "c", "d", "e", "�");
    assertThat(keys(m.iterator(Bytes.utf8("b"), Bytes.utf8("d")))).containsExactly("b", "c");
    assertThat(keys(m.iterator(Bytes.utf8("c"), null))).startsWith("c", "d", "e");
    assertThat(keys(m.iterator(null, Bytes.utf8("b")))).containsExactly("a");
    assertThat(keys(m.iterator(Bytes.utf8("d"), Bytes.utf8("b")))).isEmpty();
  }

  @Test
  void tracksApproximateSizeAcrossOverwrites() {
    MemTable m = new MemTable(1);
    m.put(Entry.put(Bytes.utf8("k"), 1, new byte[100]));
    long afterFirst = m.approximateBytes();
    m.put(Entry.put(Bytes.utf8("k"), 2, new byte[10]));

    assertThat(afterFirst).isEqualTo(1 + 100 + Entry.OVERHEAD_BYTES);
    assertThat(m.approximateBytes()).isEqualTo(1 + 10 + Entry.OVERHEAD_BYTES);
  }
}
