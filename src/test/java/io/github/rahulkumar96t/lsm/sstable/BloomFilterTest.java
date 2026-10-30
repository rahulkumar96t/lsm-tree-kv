package io.github.rahulkumar96t.lsm.sstable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rahulkumar96t.lsm.CorruptionException;
import io.github.rahulkumar96t.lsm.util.Bytes;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class BloomFilterTest {

  private static BloomFilter filterFor(int keys, int bitsPerKey) {
    long[] hashes = new long[2 * keys];
    for (int i = 0; i < keys; i++) {
      long[] h = BloomFilter.hashes(Bytes.utf8("key-" + i));
      hashes[2 * i] = h[0];
      hashes[2 * i + 1] = h[1];
    }
    return BloomFilter.build(hashes, keys, bitsPerKey);
  }

  @Test
  void neverReportsFalseNegatives() {
    BloomFilter filter = filterFor(10_000, 10);
    for (int i = 0; i < 10_000; i++) {
      assertThat(filter.mightContain(Bytes.utf8("key-" + i))).isTrue();
    }
  }

  @Test
  void falsePositiveRateMatchesBitsPerKey() {
    BloomFilter filter = filterFor(10_000, 10);
    int falsePositives = 0;
    int probes = 100_000;
    for (int i = 0; i < probes; i++) {
      if (filter.mightContain(Bytes.utf8("absent-" + i))) {
        falsePositives++;
      }
    }
    // Theory for 10 bits/key and 7 probes: ~0.82%. Allow generous slack, the inputs are fixed.
    assertThat(falsePositives / (double) probes).isLessThan(0.02);
    assertThat(filter.numProbes()).isEqualTo(7);
  }

  @Test
  void roundTripsThroughSerialization() throws Exception {
    BloomFilter filter = filterFor(500, 8);
    BloomFilter copy = BloomFilter.deserialize(ByteBuffer.wrap(filter.serialize()));
    assertThat(copy.numBits()).isEqualTo(filter.numBits());
    for (int i = 0; i < 2_000; i++) {
      byte[] key = Bytes.utf8("key-" + i);
      assertThat(copy.mightContain(key)).isEqualTo(filter.mightContain(key));
    }
  }

  @Test
  void rejectsInconsistentHeader() {
    byte[] bytes = filterFor(10, 10).serialize();
    ByteBuffer.wrap(bytes).putInt(4, 9999); // word count no longer matches the payload
    assertThatThrownBy(() -> BloomFilter.deserialize(ByteBuffer.wrap(bytes)))
        .isInstanceOf(CorruptionException.class);
  }
}
