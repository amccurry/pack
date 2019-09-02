package pack.iscsi.external.local;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;

import pack.iscsi.partitioned.storagemanager.BlockStore;
import pack.iscsi.partitioned.storagemanager.VolumeMetadata;
import pack.iscsi.partitioned.storagemanager.VolumeStore;
import pack.util.IOUtils;

public class LocalVolumeStore implements VolumeStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalVolumeStore.class);

  private final File _blockStoreDir;
  private final LoadingCache<Long, LocalBlockStore> _blockStores;

  public LocalVolumeStore(File blockStoreDir) {
    _blockStoreDir = blockStoreDir;
    _blockStoreDir.mkdirs();

    CacheLoader<Long, LocalBlockStore> loader = key -> {
      File file = new File(_blockStoreDir, Long.toString(key));
      VolumeMetadata metadata = getVolumeMetadata(key);
      int blockSize = metadata.getBlockSize();
      long lengthInBytes = metadata.getLengthInBytes();
      return new LocalBlockStore(file, blockSize, lengthInBytes);
    };

    RemovalListener<Long, LocalBlockStore> removalListener = (key, value, cause) -> {
      if (value != null) {
        IOUtils.close(LOGGER, value);
      }
    };

    _blockStores = Caffeine.newBuilder()
                           .removalListener(removalListener)
                           .build(loader);
  }

  @Override
  public List<String> getVolumeNames() {
    return Arrays.asList(_blockStoreDir.list());
  }

  @Override
  public long getVolumeId(String name) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public VolumeMetadata getVolumeMetadata(long volumeId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BlockStore getBlockStore(long volumeId) throws IOException {
    return _blockStores.get(volumeId);
  }

  @Override
  public void createVolume(String name, int blockSize, long lengthInBytes) throws IOException {
    throw new RuntimeException("Not impl");
  }

  @Override
  public void destroyVolume(long volumeId) throws IOException {
    throw new RuntimeException("Not impl");
  }

  @Override
  public void renameVolume(long volumeId, String name) throws IOException {
    throw new RuntimeException("Not impl");
  }

  @Override
  public void growVolume(long volumeId, long lengthInBytes) throws IOException {
    throw new RuntimeException("Not impl");
  }

}
