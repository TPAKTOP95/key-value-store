package org.csc.java.spring2023;

/**
 * Класс-дескриптор блока, в котором хранится значение.
 * <p>
 * Если вам это потребуется, можете заменить этот record на class.
 */
record FileBlockLocation(String fileName, int offset, int size) {

  private static final FileBlockLocation EMPTY = new FileBlockLocation("EmptyValuesFile", 0, 0);

  public static FileBlockLocation getEmpty() {
    return EMPTY;
  }
}
