package pack.block.blockstore.hdfs.blockstore;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import pack.block.blockstore.hdfs.HdfsMetaData;

public abstract class WalFileFactory {

  protected final FileSystem _fileSystem;
  protected final HdfsMetaData _metaData;

  public WalFileFactory(FileSystem fileSystem, HdfsMetaData metaData) {
    _fileSystem = fileSystem;
    _metaData = metaData;
  }

  public abstract WalFile.Writer create(Path path) throws IOException;

  public abstract WalFile.Reader open(Path path) throws IOException;

  public static WalFileFactory create(FileSystem fileSystem, HdfsMetaData metaData) {
    return new WalFileFactoryPackFile(fileSystem, metaData);
  }

  public abstract void recover(Path src, Path dst) throws IOException;
}
