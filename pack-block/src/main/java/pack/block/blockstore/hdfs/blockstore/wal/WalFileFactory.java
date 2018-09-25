package pack.block.blockstore.hdfs.blockstore.wal;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import pack.block.blockstore.BlockStoreMetaData;

public abstract class WalFileFactory {

  protected final FileSystem _fileSystem;
  protected final BlockStoreMetaData _metaData;

  public WalFileFactory(FileSystem fileSystem, BlockStoreMetaData metaData) {
    _fileSystem = fileSystem;
    _metaData = metaData;
  }

  public abstract WalFile.Writer create(Path path) throws IOException;

  public abstract WalFile.Reader open(Path path) throws IOException;

  public static WalFileFactory create(FileSystem fileSystem, BlockStoreMetaData metaData) {
    return new WalFileFactoryPackFileSync(fileSystem, metaData);
  }

  public abstract void recover(Path src, Path dst) throws IOException;
}
