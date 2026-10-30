package io.github.rahulkumar96t.lsm.internal;

/** Kind of a versioned record. The byte code is part of the on-disk format. */
public enum ValueType {
  PUT((byte) 1),
  DELETE((byte) 2);

  private final byte code;

  ValueType(byte code) {
    this.code = code;
  }

  public byte code() {
    return code;
  }

  /** Returns the type for {@code code}, or {@code null} if the code is unknown (corruption). */
  public static ValueType fromCode(byte code) {
    return switch (code) {
      case 1 -> PUT;
      case 2 -> DELETE;
      default -> null;
    };
  }
}
