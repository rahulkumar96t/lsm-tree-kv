package io.github.rahulkumar96t.lsm.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

/** Byte-array helpers: unsigned lexicographic ordering, hashing and display. */
public final class Bytes {

  /** Unsigned lexicographic order, the sort order of every key in the engine. */
  public static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;

  private Bytes() {}

  public static int compare(byte[] a, byte[] b) {
    return Arrays.compareUnsigned(a, b);
  }

  public static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  public static String string(byte[] b) {
    return new String(b, StandardCharsets.UTF_8);
  }

  /**
   * 64-bit hash of {@code data} (MurmurHash64A construction). Used by the bloom filter, so the
   * output must never change once SSTables have been written with it.
   */
  public static long hash64(byte[] data, long seed) {
    final long m = 0xc6a4a7935bd1e995L;
    final int r = 47;
    long h = seed ^ (data.length * m);
    int i = 0;
    int blocks = data.length / 8;
    for (int b = 0; b < blocks; b++, i += 8) {
      long k = littleEndianLong(data, i);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h ^= k;
      h *= m;
    }
    int remaining = data.length - i;
    if (remaining > 0) {
      for (int j = remaining - 1; j >= 0; j--) {
        h ^= (data[i + j] & 0xffL) << (8 * j);
      }
      h *= m;
    }
    h ^= h >>> r;
    h *= m;
    h ^= h >>> r;
    return h;
  }

  private static long littleEndianLong(byte[] b, int off) {
    return (b[off] & 0xffL)
        | (b[off + 1] & 0xffL) << 8
        | (b[off + 2] & 0xffL) << 16
        | (b[off + 3] & 0xffL) << 24
        | (b[off + 4] & 0xffL) << 32
        | (b[off + 5] & 0xffL) << 40
        | (b[off + 6] & 0xffL) << 48
        | (b[off + 7] & 0xffL) << 56;
  }
}
