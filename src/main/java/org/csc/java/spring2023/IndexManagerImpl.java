package org.csc.java.spring2023;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexManagerImpl implements IndexManager {

  //private final RandomAccessFile indexFile;
  private final Path path;
  private final Map<ByteWrapper, List<FileBlockLocation>> data = new HashMap<>();

  public IndexManagerImpl(Path path) throws IOException {
    this.path = path;
    try {
      Files.createFile(path);
    } catch (FileAlreadyExistsException ignored) {
      //do nothing
    }
    loadIndexFromFile();
  }

  private void loadIndexFromFile()
      throws IOException {
    //stringed key, numbers of block, list of blocks
    List<String> fileLines = Files.readAllLines(path);
    for (String line : fileLines) {
      String[] keyInfo = line.split(" ");
      if (!(keyInfo.length >= 2 && keyInfo.length == 3*Integer.parseInt(keyInfo[1]) + 2)) {
        throw new IOException("Invalid file format");
      }
      byte[] key = Base64.getDecoder().decode(keyInfo[0]);
      List<FileBlockLocation> locations = new ArrayList<>();
      int locationsCnt = Integer.parseInt(keyInfo[1]);
      for (int i = 0; i < locationsCnt; i++) {
        String fileName = keyInfo[2 + 3 * i];
        int offset = Integer.parseInt(keyInfo[2 + 3 * i + 1]);
        int size = Integer.parseInt(keyInfo[2 + 3 * i + 2]);
        locations.add(new FileBlockLocation(fileName, offset, size));
      }

      data.put(new ByteWrapper(key), locations);
    }
  }

  @Override
  public void add(byte[] key, List<FileBlockLocation> writtenBlocks) {
    data.put(new ByteWrapper(key), writtenBlocks);
  }

  @Override
  public void remove(byte[] key) {
    data.remove(new ByteWrapper(key));
  }

  @Override
  public List<FileBlockLocation> getFileBlocksLocations(byte[] key) {
    if (data.containsKey(new ByteWrapper(key))) {
      return new ArrayList<>(data
          .get(new ByteWrapper(key)));
    }
    return new ArrayList<>();
  }

  @Override
  public void close() throws IOException {
    try (Writer indexFile = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(path.toFile()), StandardCharsets.UTF_8))) {
      for (var info : data.entrySet()) {
        List<FileBlockLocation> valueList = info.getValue();
        indexFile.write(new String(Base64.getEncoder().encode(info.getKey().getBytes())) + " ");
        indexFile.write(valueList.size() + " ");
        for (var value : valueList) {
          indexFile.write(value.fileName() + " ");
          indexFile.write(value.offset() + " ");
          indexFile.write(value.size() + " ");
        }
        indexFile.write(System.lineSeparator());
      }
    }
  }
}
