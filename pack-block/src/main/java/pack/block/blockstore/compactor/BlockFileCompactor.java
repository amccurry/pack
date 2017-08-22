package pack.block.blockstore.compactor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.Reader;
import pack.block.blockstore.hdfs.file.BlockFile.Writer;

public class BlockFileCompactor {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockFileCompactor.class);

  private static final Joiner JOINER = Joiner.on('.');
  private static final Splitter SPLITTER = Splitter.on('.');
  private final Path _blockPath;
  private final FileSystem _fileSystem;

  public BlockFileCompactor(FileSystem fileSystem, Path path) {
    _fileSystem = fileSystem;
    _blockPath = new Path(path, HdfsBlockStore.BLOCK);
  }

  public void runCompaction() throws IOException {
    if (!_fileSystem.exists(_blockPath)) {
      LOGGER.info("Path {} does not exist, exiting.", _blockPath);
      return;
    }
    FileStatus[] listStatus = getBlockFiles();
    if (listStatus.length < 2) {
      LOGGER.info("Path {} contains less than 2 block files, exiting.", _blockPath);
      return;
    }
    Arrays.sort(listStatus, Collections.reverseOrder());
    List<Reader> readers = new ArrayList<>();
    try {
      LOGGER.info("Starting merge for path {}.", _blockPath);
      List<String> sourceFileList = new ArrayList<>();
      for (FileStatus fileStatus : listStatus) {
        Path path = fileStatus.getPath();
        LOGGER.info("Adding block file for merge {}.", path);
        readers.add(BlockFile.open(_fileSystem, path));
        sourceFileList.add(path.getName());
      }
      Reader reader = readers.get(0);
      Path newPath = getNewPath(reader.getPath());
      Path tmpPath = new Path(_blockPath, UUID.randomUUID()
                                              .toString()
          + ".tmp");
      LOGGER.info("New merged output path {}.", tmpPath);
      try (Writer writer = BlockFile.create(_fileSystem, tmpPath, reader.getBlockSize(), sourceFileList)) {
        BlockFile.merge(readers, writer);
      }
      LOGGER.info("Merged complete path {}.", tmpPath);
      if (_fileSystem.rename(tmpPath, newPath)) {
        LOGGER.info("Merged commit path {}.", newPath);
      } else {
        throw new IOException("Merge failed.");
      }
    } finally {
      readers.forEach(r -> IOUtils.closeQuietly(r));
    }
  }

  private FileStatus[] getBlockFiles() throws FileNotFoundException, IOException {
    return _fileSystem.listStatus(_blockPath, (PathFilter) p -> p.getName()
                                                                 .endsWith("." + HdfsBlockStore.BLOCK));
  }

  private Path getNewPath(Path path) throws IOException {
    String name = path.getName();
    List<String> list = SPLITTER.splitToList(name);
    String newName;
    if (list.size() == 2) {
      newName = JOINER.join(list.get(0), "0", list.get(1));
    } else if (list.size() == 3) {
      long gen = Long.parseLong(list.get(2));
      newName = JOINER.join(list.get(0), Long.toString(gen + 1), list.get(2));
    } else {
      throw new IOException("Path " + path + " invalid");
    }
    return new Path(path.getParent(), newName);
  }

}
