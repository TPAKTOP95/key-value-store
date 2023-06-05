package org.csc.java.spring2023;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public interface KeyValueStore extends Closeable {

  /**
   * Проверяет, есть ли такой ключ в хранилище
   */
  boolean contains(byte[] key) throws IOException;

  /**
   * По ключу возвращает входной поток из которого можно (лениво) читать значение
   */
  InputStream openValueStream(byte[] key) throws IOException;

  /**
   * Полностью считывает значение в массив байтов и возвращает его
   */
  byte[] loadValue(byte[] key) throws IOException;

  /**
   * Записывает новое значение по ключу. Если ключ уже существует в базе, тогда перезаписывает
   * старое значение
   */
  void upsert(byte[] key, byte[] value) throws IOException;

  /**
   * Удаляет значение из базы. Если значение существовало, то возвращает true, иначе false.
   */
  boolean remove(byte[] key) throws IOException;

  /**
   * TestOnly
   * <p>
   * Возвращает IndexManager, соответствующий текущему хранилищу
   */
  IndexManager getIndexManager();
}
