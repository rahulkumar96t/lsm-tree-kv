package io.github.rahulkumar96t.lsm;

import io.github.rahulkumar96t.lsm.util.Bytes;
import java.util.Arrays;

/**
 * A key and its current value, as returned by a scan. The arrays are copies owned by the caller.
 */
public record KeyValue(byte[] key, byte[] value) {

  @Override
  public boolean equals(Object o) {
    return o instanceof KeyValue kv && Arrays.equals(key, kv.key) && Arrays.equals(value, kv.value);
  }

  @Override
  public int hashCode() {
    return 31 * Arrays.hashCode(key) + Arrays.hashCode(value);
  }

  @Override
  public String toString() {
    return Bytes.string(key) + "=" + Bytes.string(value);
  }
}
