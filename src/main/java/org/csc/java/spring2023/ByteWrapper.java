package org.csc.java.spring2023;

import java.util.Arrays;

/**
 * Вспомогательная обертка над массивом байтов, понадобится для хранения Map<ByteWrapper,
 * List<FileBlockLocation>> в {@link IndexManager}
 */
final class ByteWrapper {

  private final byte[] data;

  ByteWrapper(byte[] data) {
    this.data = data;
  }

  byte[] getBytes() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    return o instanceof ByteWrapper that && Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }
}