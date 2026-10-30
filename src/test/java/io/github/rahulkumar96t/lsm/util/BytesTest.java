package io.github.rahulkumar96t.lsm.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BytesTest {

  @Test
  void ordersBytesAsUnsigned() {
    assertThat(Bytes.compare(new byte[] {(byte) 0x80}, new byte[] {0x7f})).isPositive();
    assertThat(Bytes.compare(new byte[] {1, 2}, new byte[] {1, 2, 0})).isNegative();
    assertThat(Bytes.compare(new byte[0], new byte[] {0})).isNegative();
    assertThat(Bytes.compare(Bytes.utf8("abc"), Bytes.utf8("abc"))).isZero();
  }

  @Test
  void hashIsDeterministicAndSeedDependent() {
    byte[] key = Bytes.utf8("some-key-longer-than-eight-bytes");
    assertThat(Bytes.hash64(key, 1)).isEqualTo(Bytes.hash64(key.clone(), 1));
    assertThat(Bytes.hash64(key, 1)).isNotEqualTo(Bytes.hash64(key, 2));
    assertThat(Bytes.hash64(Bytes.utf8("a"), 1)).isNotEqualTo(Bytes.hash64(Bytes.utf8("b"), 1));
  }

  @Test
  void hashCoversEveryTailLength() {
    // Inputs that differ only in the tail (length % 8 != 0) must hash differently.
    for (int length = 1; length < 17; length++) {
      byte[] a = new byte[length];
      byte[] b = new byte[length];
      b[length - 1] = 1;
      assertThat(Bytes.hash64(a, 7)).as("length %d", length).isNotEqualTo(Bytes.hash64(b, 7));
    }
  }
}
