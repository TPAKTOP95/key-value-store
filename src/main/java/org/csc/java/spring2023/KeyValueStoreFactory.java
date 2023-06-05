package org.csc.java.spring2023;

import java.io.IOException;
import java.nio.file.Path;

public final class KeyValueStoreFactory {

  private KeyValueStoreFactory() {
  }

  public static KeyValueStore create(Path workingDir, int valueFileSize) throws IOException {
    return new KeyValueStoreImpl(workingDir, valueFileSize);
  }
}
