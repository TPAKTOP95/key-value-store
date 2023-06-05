package org.csc.java.spring2023;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ValueStoreManagerImpl implements ValueStoreManager {

  private final IndexManagerImpl freeBlocks;
  private final byte[] keyInFreeBlocksIndex = "default".getBytes(StandardCharsets.UTF_8);
  private final Path path;
  private final int maxFileSize;

  ValueStoreManagerImpl(Path path, String indexFilename, int maxFileSize) throws IOException {
    this.path = path;
    this.maxFileSize = maxFileSize;
    freeBlocks = new IndexManagerImpl(path.resolve(indexFilename));
  }

  @Override
  public List<FileBlockLocation> add(byte[] value) throws IOException, IllegalArgumentException {
    int dataPointer = 0;
    List<FileBlockLocation> filledBlocks = new ArrayList<>();
    List<FileBlockLocation> freeBlocksList = freeBlocks.getFileBlocksLocations(
        keyInFreeBlocksIndex);

    if (value.length == 0) {
      final List<FileBlockLocation> result = new ArrayList<>();
      result.add(FileBlockLocation.getEmpty());
      return result;
    }

    while (dataPointer < value.length && !freeBlocksList.isEmpty()) {
      FileBlockLocation blockToFill = freeBlocksList.get(freeBlocksList.size() - 1);
      freeBlocksList.remove(freeBlocksList.size() - 1);
      try (RandomAccessFile fileToWrite = new RandomAccessFile(
          path.resolve(blockToFill.fileName()).toFile(), "rw")) {
        fileToWrite.seek(blockToFill.offset());
        final int amountOfWrittenData = Math.min(value.length - dataPointer, blockToFill.size());
        fileToWrite.write(value, dataPointer, amountOfWrittenData);
        dataPointer += amountOfWrittenData;
        if (amountOfWrittenData < blockToFill.size()) {
          freeBlocksList.add(new FileBlockLocation(blockToFill.fileName(),
              blockToFill.offset() + amountOfWrittenData,
              blockToFill.size() - amountOfWrittenData));
        }
        filledBlocks.add(new FileBlockLocation(blockToFill.fileName(), blockToFill.offset(),
            amountOfWrittenData));
      }
    }
    while (dataPointer < value.length) {
      String filename = getFreshFilename(path);
      try (RandomAccessFile file = new RandomAccessFile(path.resolve(filename).toFile(), "rw")) {
        int amountOfWrittenData = Math.min(maxFileSize, value.length - dataPointer);
        file.write(value, dataPointer, amountOfWrittenData);
        filledBlocks.add(new FileBlockLocation(filename, 0, amountOfWrittenData));
        if (amountOfWrittenData < maxFileSize) {
          freeBlocksList.add(new FileBlockLocation(filename, amountOfWrittenData,
              maxFileSize - amountOfWrittenData));
        }
        dataPointer += amountOfWrittenData;
      }
    }
    freeBlocks.add(keyInFreeBlocksIndex, freeBlocksList);
    return filledBlocks;
  }


  @Override
  public InputStream openBlockStream(FileBlockLocation location) throws IOException {
    return new BufferedInputStream(new FileInputStream(path.resolve(location.fileName()).toFile()));
  }

  @Override
  public void remove(List<FileBlockLocation> valueBlocksLocations) {

    var freeBlock = freeBlocks.getFileBlocksLocations(keyInFreeBlocksIndex);
    freeBlock.addAll(valueBlocksLocations);
    freeBlocks.add(keyInFreeBlocksIndex, freeBlock);

  }

  @Override
  public void close() throws IOException {
    freeBlocks.close();
  }

  private static String getFreshFilename(Path path) throws IOException {
    try (var fileList = Files.list(path)) {
      return String.format("data_%d", fileList.count());
    }
  }
}
