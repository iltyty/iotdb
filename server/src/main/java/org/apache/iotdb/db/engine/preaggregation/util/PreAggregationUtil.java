package org.apache.iotdb.db.engine.preaggregation.util;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class PreAggregationUtil {

  public static long getFileVersion(String tsFilePath) {
    TsFileResource tsFileResource = new TsFileResource(new File(tsFilePath));
    long tsFileSize = tsFileResource.getTsFileSize();
    long modFileSize = new File(tsFileResource.getModFile().getFilePath()).length();
    return tsFileSize + modFileSize;
  }

  public static List<Pair<Long, IChunkReader>> getChunkReaders(
      Path seriesPath, TsFileSequenceReader reader, Collection<Modification> allModifications)
      throws IOException {
    List<Modification> modifications = new ArrayList<>();
    for (Modification modification : allModifications) {
      if (modification.getPath().matchFullPath((PartialPath) seriesPath)) {
        modifications.add(modification);
      }
    }

    List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(seriesPath, true);
    if (!modifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(chunkMetadataList, modifications);
    }

    List<Pair<Long, IChunkReader>> res = new LinkedList<>();
    for (ChunkMetadata metadata : chunkMetadataList) {
      Chunk chunk = reader.readMemChunk(metadata);
      IChunkReader chunkReader = new ChunkReader(chunk, null);
      res.add(new Pair<>(metadata.getOffsetOfChunkHeader(), chunkReader));
    }
    return res;
  }

  public static Map<String, Long> getAllTsFiles(String dataDir) {
    Map<String, Long> allTsFiles = new HashMap<>();
    File tmpFile = new File(dataDir);
    File[] files = tmpFile.listFiles();
    if (files == null || files.length == 0) {
      return allTsFiles;
    }

    for (File file : files) {
      if (!file.isDirectory() && file.getName().endsWith("tsfile")) {
        String filePath = file.getAbsolutePath();
        Long fileVersion = getFileVersion(filePath);
        allTsFiles.put(filePath, fileVersion);
      }
    }
    return allTsFiles;
  }
}
