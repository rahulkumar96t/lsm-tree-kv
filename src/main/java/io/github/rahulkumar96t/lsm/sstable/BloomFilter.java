package io.github.rahulkumar96t.lsm.sstable;

import io.github.rahulkumar96t.lsm.CorruptionException;
import io.github.rahulkumar96t.lsm.util.Bytes;
import java.nio.ByteBuffer;

/**
 * Bloom filter over the keys of one SSTable, so point lookups can skip tables that certainly do not
 * contain a key.
 *
 * <p>Probe positions use double hashing (Kirsch–Mitzenmacher): {@code h1 + i * h2} for {@code i in
 * [0, k)}, derived from two 64-bit hashes of the key. With {@code b} bits per key the optimal probe
 * count is {@code k = b * ln 2}; 10 bits per key gives roughly a 1% false-positive rate.
 *
 * <p>Serialized form: {@code numProbes:int, numWords:int, words:long[numWords]}.
 */
public final class BloomFilter {

  private static final long SEED_1 = 0x9E3779B97F4A7C15L;
  private static final long SEED_2 = 0xC2B2AE3D27D4EB4FL;

  private final long[] words;
  private final long numBits;
  private final int numProbes;

  private BloomFilter(long[] words, int numProbes) {
    this.words = words;
    this.numBits = (long) words.length * Long.SIZE;
    this.numProbes = numProbes;
  }

  /** Builds a filter from pre-computed key hashes (see {@link #hashes(byte[])}). */
  public static BloomFilter build(long[] hashPairs, int count, int bitsPerKey) {
    if (bitsPerKey < 1) {
      throw new IllegalArgumentException("bitsPerKey must be >= 1");
    }
    long bits = Math.max(Long.SIZE, (long) count * bitsPerKey);
    int numWords = (int) Math.min(Integer.MAX_VALUE - 8, (bits + Long.SIZE - 1) / Long.SIZE);
    int probes = (int) Math.max(1, Math.min(30, Math.round(bitsPerKey * Math.log(2))));
    BloomFilter filter = new BloomFilter(new long[numWords], probes);
    for (int i = 0; i < count; i++) {
      filter.add(hashPairs[2 * i], hashPairs[2 * i + 1]);
    }
    return filter;
  }

  /** The two independent 64-bit hashes of a key used for probing. */
  public static long[] hashes(byte[] key) {
    return new long[] {Bytes.hash64(key, SEED_1), Bytes.hash64(key, SEED_2)};
  }

  public boolean mightContain(byte[] key) {
    return mightContain(Bytes.hash64(key, SEED_1), Bytes.hash64(key, SEED_2));
  }

  private boolean mightContain(long h1, long h2) {
    long combined = h1;
    for (int i = 0; i < numProbes; i++) {
      long bit = Math.floorMod(combined, numBits);
      if ((words[(int) (bit >>> 6)] & (1L << bit)) == 0) {
        return false;
      }
      combined += h2;
    }
    return true;
  }

  private void add(long h1, long h2) {
    long combined = h1;
    for (int i = 0; i < numProbes; i++) {
      long bit = Math.floorMod(combined, numBits);
      words[(int) (bit >>> 6)] |= 1L << bit;
      combined += h2;
    }
  }

  public int numProbes() {
    return numProbes;
  }

  public long numBits() {
    return numBits;
  }

  public byte[] serialize() {
    ByteBuffer buf = ByteBuffer.allocate(2 * Integer.BYTES + words.length * Long.BYTES);
    buf.putInt(numProbes).putInt(words.length);
    for (long word : words) {
      buf.putLong(word);
    }
    return buf.array();
  }

  public static BloomFilter deserialize(ByteBuffer buf) throws CorruptionException {
    if (buf.remaining() < 2 * Integer.BYTES) {
      throw new CorruptionException("bloom block too short");
    }
    int probes = buf.getInt();
    int numWords = buf.getInt();
    if (probes < 1
        || probes > 30
        || numWords < 1
        || numWords * (long) Long.BYTES != buf.remaining()) {
      throw new CorruptionException("bloom block has inconsistent header");
    }
    long[] words = new long[numWords];
    buf.asLongBuffer().get(words);
    return new BloomFilter(words, probes);
  }
}
