package org.csc.java.spring2023;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KeyValueStoreTest {

  @Test
  public void testUpsert(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 1);

    byte[] key = "a".getBytes();
    kvStore.upsert(key, "1".getBytes());

    assertTrue(kvStore.contains(key));

    kvStore.close();
  }

  @Test
  public void testUpsertAndRemove(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 1);

    byte[] key = "a".getBytes();

    assertFalse(kvStore.contains(key));

    kvStore.upsert(key, "1".getBytes());
    assertTrue(kvStore.remove(key));

    assertFalse(kvStore.contains(key));

    kvStore.close();
  }

  @Test
  void testRemoveOnEmptyStore(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 1);

    byte[] key = "a".getBytes();
    assertFalse(kvStore.remove(key));

    kvStore.close();
  }

  @Test
  public void testUpsertAndLoad(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 2);

    byte[] key = "a".getBytes();
    byte[] value = "1".getBytes();

    kvStore.upsert(key, value);

    assertArrayEquals(kvStore.loadValue(key), value);

    kvStore.close();
  }

  @Test
  public void testOpenStreamAndRead(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 2);

    byte[] key = "a".getBytes();
    byte[] value = "1234567".getBytes();

    kvStore.upsert(key, value);

    try (InputStream valueStream = kvStore.openValueStream(key)) {
      assertThat(valueStream).hasBinaryContent(value);
    }

    kvStore.close();
  }

  @Test
  public void testUpsertTwoEntries(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 8);

    byte[] key1 = "a".getBytes();
    byte[] value1 = "1234".getBytes();
    kvStore.upsert(key1, value1);

    byte[] key2 = "b".getBytes();
    byte[] value2 = "5678".getBytes();
    kvStore.upsert(key2, value2);

    assertArrayEquals(kvStore.loadValue(key1), value1);
    assertArrayEquals(kvStore.loadValue(key2), value2);

    kvStore.close();
  }

  @Test
  public void testUpsertBigValue(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 1);

    byte[] key = "a".getBytes();
    byte[] value = "1234567890".getBytes();
    kvStore.upsert(key, value); // should create 10 files

    assertArrayEquals(kvStore.loadValue(key), value);

    kvStore.close();
  }

  @Test
  public void testUpsertAndLoadEmptyValue(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 1);

    kvStore.upsert("a".getBytes(), new byte[0]);

    assertArrayEquals(kvStore.loadValue("a".getBytes()), new byte[0]);

    kvStore.close();
  }

  @Test
  void testKeyValueStoreApiWithIllegalArguments(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 10);

    // мы советуем предпочесть Objects.requireNonNull в качестве проверки на null

    assertThatThrownBy(() -> kvStore.contains(null))
        .isInstanceOfAny(NullPointerException.class, IllegalArgumentException.class);

    assertThatThrownBy(() -> kvStore.remove(null))
        .isInstanceOfAny(NullPointerException.class, IllegalArgumentException.class);

    assertThatThrownBy(() -> kvStore.upsert(null, new byte[]{42}))
        .isInstanceOfAny(NullPointerException.class, IllegalArgumentException.class);

    assertThatThrownBy(() -> kvStore.upsert(new byte[]{42}, null))
        .isInstanceOfAny(NullPointerException.class, IllegalArgumentException.class);

    assertThatThrownBy(() -> kvStore.loadValue(null))
        .isInstanceOfAny(NullPointerException.class, IllegalArgumentException.class);

    assertThatThrownBy(() -> kvStore.openValueStream(null))
        .isInstanceOfAny(NullPointerException.class, IllegalArgumentException.class);

    kvStore.close();
  }

  @Test
  void testInitStoreOnInvalidArguments(@TempDir Path tempDir) throws IOException {
    assertThrows(IllegalArgumentException.class, () -> initStore(tempDir, 0),
        "maxValueFileSize must be prohibited to be <= 0");

    assertThrows(IllegalArgumentException.class, () -> initStore(tempDir, -1),
        "maxValueFileSize must be prohibited to be <= 0");

    assertThatThrownBy(() -> initStore(null, 1))
        .withFailMessage("workingDir should be prohibited to be null")
        .isInstanceOfAny(NullPointerException.class, IllegalArgumentException.class);

    Path nonExistentFolder = tempDir.resolve("nonExistentFolder");

    assertThat(nonExistentFolder)
        .withFailMessage("We expect this folder to not exist!")
        .doesNotExist();

    Path notDirectory = tempDir.resolve("existentFile");
    Files.write(notDirectory, new byte[]{42});

    assertThat(notDirectory)
        .withFailMessage("We expect this file to exist, to be a file and to be not empty!")
        .isNotEmptyFile();

    assertThatThrownBy(() -> initStore(nonExistentFolder, 1))
        .withFailMessage("workingDir should be checked to be existing")
        .isInstanceOfAny(NoSuchFileException.class, IllegalArgumentException.class);

    assertThatThrownBy(() -> initStore(notDirectory, 1))
        .withFailMessage("workingDir should be checked to be a directory, not a file")
        .isInstanceOfAny(NotDirectoryException.class, IllegalArgumentException.class);
  }

  @Test
  public void testLoadNonExistentEntry(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 2);

    assertThrows(IOException.class, () -> kvStore.loadValue("a".getBytes()));

    kvStore.close();
  }

  @Test
  public void testOpenStreamToNonExistentEntry(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 2);

    assertThrows(IOException.class, () -> kvStore.openValueStream("a".getBytes()));

    kvStore.close();
  }

  @Test
  public void testRewriteWithSmallerValue(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 1);

    byte[] key = "a".getBytes();
    kvStore.upsert(key, "42".getBytes());
    kvStore.upsert(key, "1".getBytes());

    assertArrayEquals(kvStore.loadValue(key), "1".getBytes());

    kvStore.close();
  }

  @Test
  public void testRewriteWithBiggerValue(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 1);

    byte[] key = "a".getBytes();
    kvStore.upsert(key, "1".getBytes());
    kvStore.upsert(key, "42".getBytes());

    assertArrayEquals(kvStore.loadValue(key), "42".getBytes());

    kvStore.close();
  }

  @Test
  public void testUpsertAndGetSeveralKVSmallFile(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 1);

    byte[] key1 = "a".getBytes();
    byte[] value1 = "12".getBytes();

    byte[] key2 = "b".getBytes();
    byte[] value2 = "34".getBytes();

    kvStore.upsert(key1, value1);
    kvStore.upsert(key2, value2);

    assertArrayEquals(kvStore.loadValue(key1), value1);
    assertArrayEquals(kvStore.loadValue(key2), value2);

    kvStore.close();
  }

  @Test
  public void testUpsertCloseUpsertCloseRead(@TempDir Path tempDir) throws IOException {
    byte[] key1 = "a".getBytes();
    byte[] value1 = "123".getBytes();
    byte[] key2 = "b".getBytes();
    byte[] value2 = "345".getBytes();

    try (KeyValueStore kvStore = initStore(tempDir, 5)) {
      kvStore.upsert(key1, value1);
      kvStore.upsert(key2, value2);

      assertArrayEquals(kvStore.loadValue(key2), value2);
      assertArrayEquals(kvStore.loadValue(key1), value1);
    }

    byte[] newValue1 = "321".getBytes();
    byte[] newValue2 = "654".getBytes();

    try (KeyValueStore kvStore = initStore(tempDir, 5)) {
      kvStore.upsert(key1, newValue1);
      kvStore.upsert(key2, newValue2);

      assertArrayEquals(kvStore.loadValue(key2), newValue2);
      assertArrayEquals(kvStore.loadValue(key1), newValue1);
    }

    try (KeyValueStore kvStore = initStore(tempDir, 5)) {
      assertArrayEquals(kvStore.loadValue(key2), newValue2);
      assertArrayEquals(kvStore.loadValue(key1), newValue1);
    }
  }

  @Test
  public void testUpsertRemoveUpsertRead(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 5);

    byte[] key1 = "a".getBytes();
    byte[] value1 = "123".getBytes();
    byte[] key2 = "b".getBytes();
    byte[] value2 = "345".getBytes();

    kvStore.upsert(key1, value1);
    kvStore.upsert(key2, value2);
    assertTrue(kvStore.remove(key2));

    byte[] newValue2 = "654".getBytes();

    kvStore.upsert(key2, newValue2);
    kvStore.upsert(key2, newValue2);

    assertArrayEquals(kvStore.loadValue(key2), newValue2);
    assertArrayEquals(kvStore.loadValue(key1), value1);

    kvStore.close();
  }

  @Test
  public void testUpsertCloseRemoveCloseContains(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 5);

    byte[] key = "a".getBytes();

    kvStore.upsert(key, "123".getBytes());

    kvStore.close();
    kvStore = initStore(tempDir, 5);

    assertTrue(kvStore.contains(key));

    assertTrue(kvStore.remove(key));

    kvStore.close();
    kvStore = initStore(tempDir, 5);

    assertFalse(kvStore.contains(key));

    kvStore.close();
  }

  @Test
  public void testIndexManagerStateLoading(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 5);

    byte[] key1 = "a".getBytes();
    byte[] key2 = "b".getBytes();
    byte[] key3 = "c".getBytes();
    byte[] value1 = "123".getBytes();
    byte[] value2 = "234".getBytes();
    byte[] value3 = "345".getBytes();

    kvStore.upsert(key1, value1);
    kvStore.upsert(key2, value2);
    kvStore.upsert(key3, value3);
    kvStore.close();

    kvStore = initStore(tempDir, 5);
    kvStore.upsert(key2, value3);
    kvStore.close();

    kvStore = initStore(tempDir, 5);
    assertArrayEquals(kvStore.loadValue(key1), value1);
    kvStore.close();
  }

  @Test
  public void testMultipleFilesSupport(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 1);

    byte[] bytes = "a".getBytes();
    kvStore.upsert(bytes, "123".getBytes());

    int blocksNum = kvStore.getIndexManager().getFileBlocksLocations(bytes).size();
    assertEquals(blocksNum, 3);

    kvStore.close();
  }

  @Test
  public void testEmptyBlockNotRapidlyGrowing(@TempDir Path tempDir) throws IOException {
    int maxValueFileSize = 5;
    byte[] key1 = "a".getBytes();
    byte[] key2 = "b".getBytes();

    List<byte[]> values = new ArrayList<>();
    values.add("1234".getBytes());
    values.add("123456".getBytes());

    for (int i = 0; i < 1000; i++) {
      try (KeyValueStore kvStore = initStore(tempDir, maxValueFileSize)) {
        Collections.reverse(values);

        kvStore.upsert(key1, values.get(0));
        kvStore.upsert(key2, values.get(1));

        assertArrayEquals(kvStore.loadValue(key1), values.get(0));
        assertArrayEquals(kvStore.loadValue(key2), values.get(1));

        kvStore.remove(key1);
        kvStore.remove(key2);
      }
    }

    long bytesLength = Files.walk(tempDir)
        .filter(p -> p.toFile().isFile())
        .mapToLong(p -> p.toFile().length())
        .sum();

    assertThat(bytesLength).isLessThan(200); // only index file without multiple block ones
  }

  @Test
  void storeCannotBeUsedAfterBeingClosed(@TempDir Path tempDir) throws IOException {
    KeyValueStore kvStore = initStore(tempDir, 1);

    byte[] key = {42};
    byte[] value = {1, 2, 3};
    kvStore.upsert(key, value);

    kvStore.close();

    assertThatThrownBy(() -> kvStore.contains(key))
        .isInstanceOf(IllegalStateException.class);

    assertThatThrownBy(() -> kvStore.loadValue(key))
        .isInstanceOf(IllegalStateException.class);

    assertThatThrownBy(() -> kvStore.openValueStream(key))
        .isInstanceOf(IllegalStateException.class);

    assertThatThrownBy(() -> kvStore.upsert(key, new byte[]{4, 5, 6}))
        .isInstanceOf(IllegalStateException.class);

    assertThatThrownBy(() -> kvStore.remove(key))
        .isInstanceOf(IllegalStateException.class);
  }

  private KeyValueStore initStore(Path workingDir, int maxValueFileSize) throws IOException {
    return KeyValueStoreFactory.create(workingDir, maxValueFileSize);
  }
}
