package org.csc.java.spring2023;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class KeyValueStoreImpl implements KeyValueStore {

  private final IndexManagerImpl indexManager;
  private final ValueStoreManagerImpl valManager;
  private final Path path;
  private static final String INDEX_FILENAME = "index.info";
  private static final String FREE_BLOCKS_INDEX_FILENAME = "FreeBlocks.info";

  private boolean isClosed = false;

  KeyValueStoreImpl(Path workingDir, int valueFileSize) throws IOException {
    if (!Files.exists(workingDir)) {
      throw new IllegalArgumentException("Working directory does not exist");
    } else if (valueFileSize <= 0) {
      throw new IllegalArgumentException("valueFileSize must be positive");
    } else if (!Files.isDirectory(workingDir)) {
      throw new IllegalArgumentException("workingDir must be a directory");
    }
    indexManager = new IndexManagerImpl(workingDir.resolve(INDEX_FILENAME));
    valManager = new ValueStoreManagerImpl(workingDir, FREE_BLOCKS_INDEX_FILENAME, valueFileSize);
    path = workingDir;
  }

  private void checkState() {
    if (isClosed) {
      throw new IllegalStateException("Don't use after close");
    }
  }

  @Override
  public boolean contains(byte[] key) {
    checkState();
    return !indexManager.getFileBlocksLocations(Objects.requireNonNull(key)).isEmpty();
  }

  @Override
  public InputStream openValueStream(byte[] key) throws IOException {
    checkState();
    List<InputStream> inputStreams = new ArrayList<>();
    for (FileBlockLocation fileBlockLocation : indexManager.getFileBlocksLocations(Objects.requireNonNull(key))) {
      inputStreams.add(valManager.openBlockStream(fileBlockLocation));
    }
    return inputStreams.stream().reduce(SequenceInputStream::new)
        .orElseThrow(() -> new IOException("Stream is empty"));
  }

  @Override
  public void upsert(byte[] key, byte[] value) throws IOException {
    Objects.requireNonNull(key);
    checkState();
    if (this.contains(key)) {
      List<FileBlockLocation> blocksToFree = indexManager.getFileBlocksLocations(key);
      indexManager.remove(key);
      valManager.remove(blocksToFree);
    }
    indexManager.add(key, valManager.add(Objects.requireNonNull(value)));
  }

  @Override
  public byte[] loadValue(byte[] key) throws IOException {
    Objects.requireNonNull(key);
    checkState();
    if (!contains(key)) {
      throw new IOException("Now such key");
    }
    List<FileBlockLocation> blocks = indexManager.getFileBlocksLocations(key);
    if (blocks.size() == 1 && blocks.get(0).equals(FileBlockLocation.getEmpty())) {
      return new byte[0];
    }
    ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
    for (var block : blocks) {
      //Can don't create new RAF when blocks in one file, but not now
      try (RandomAccessFile file = new RandomAccessFile(path.resolve(block.fileName()).toFile(),
          "r")) {
        file.seek(block.offset());
        byte[] data = new byte[block.size()];
        file.read(data, 0, block.size());
        dataStream.write(data);
      }

    }
    return dataStream.toByteArray();
  }

  @Override
  public boolean remove(byte[] key) {
    Objects.requireNonNull(key);
    checkState();
    if (indexManager.getFileBlocksLocations(key).isEmpty()) {
      return false;
    }
    valManager.remove(indexManager.getFileBlocksLocations(key));
    indexManager.remove(key);
    return true;
  }

  @Override
  public IndexManager getIndexManager() {
    return indexManager;
  }

  @Override
  public void close() throws IOException {
    if(isClosed){
      throw new IllegalStateException("Attempt to close an already closed resource");
    }
    indexManager.close();
    valManager.close();
    isClosed = true;
  }

}
