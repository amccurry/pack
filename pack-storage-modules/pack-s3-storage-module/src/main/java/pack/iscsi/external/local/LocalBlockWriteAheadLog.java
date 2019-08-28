package pack.iscsi.external.local;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import pack.iscsi.partitioned.block.BlockIOExecutor;
import pack.iscsi.partitioned.block.BlockIORequest;
import pack.iscsi.partitioned.block.BlockIOResponse;
import pack.iscsi.partitioned.block.BlockState;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;
import pack.util.IOUtils;

public class LocalBlockWriteAheadLog implements BlockWriteAheadLog {

  @EqualsAndHashCode
  @Builder
  @Value
  static class LogKey {
    long volumeId;
    long blockId;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalBlockWriteAheadLog.class);

  private final LoadingCache<LogKey, Log> _cache;
  private final File _walLogDir;

  public LocalBlockWriteAheadLog(File walLogDir) throws IOException {
    _walLogDir = walLogDir;
    _walLogDir.mkdirs();
    if (!walLogDir.exists()) {
      throw new IOException("dir " + walLogDir + " does not exist");
    }
    CacheLoader<LogKey, Log> loader = key -> {
      File blockLogDir = getBlockLogDir(key);
      blockLogDir.mkdirs();
      long volumeId = key.getVolumeId();
      long blockId = key.getBlockId();
      return new LocalLog(blockLogDir, volumeId, blockId);
    };
    RemovalListener<LogKey, Log> removalListener = (key, log, cause) -> IOUtils.close(LOGGER, log);
    _cache = Caffeine.newBuilder()
                     .expireAfterWrite(10, TimeUnit.SECONDS)
                     .removalListener(removalListener)
                     .build(loader);
  }

  @Override
  public void write(long volumeId, long blockId, long generation, long position, byte[] bytes, int offset, int len)
      throws IOException {
    Log log = getLog(volumeId, blockId);
    log.append(generation, position, bytes, offset, len);
  }

  @Override
  public void release(long volumeId, long blockId, long generation) throws IOException {
    Log log = getLog(volumeId, blockId);
    log.release(generation);
  }

  @Override
  public BlockIOExecutor getWriteAheadLogReader() {
    return new BlockIOExecutor() {
      @Override
      public BlockIOResponse exec(BlockIORequest request) throws IOException {
        long onDiskGeneration = request.getOnDiskGeneration();
        long lastStoredGeneration = request.getLastStoredGeneration();
        if (onDiskGeneration < lastStoredGeneration) {
          throw new IOException("On disk generation " + onDiskGeneration + " less than last store generation "
              + lastStoredGeneration + " something is wrong");
        }
        long volumeId = request.getVolumeId();
        long blockId = request.getBlockId();
        Log log = getLog(volumeId, blockId);
        long generation = log.recover(request.getChannel(), request.getOnDiskGeneration());
        return BlockIOResponse.builder()
                              .lastStoredGeneration(lastStoredGeneration)
                              .onDiskBlockState(BlockState.DIRTY)
                              .onDiskGeneration(generation)
                              .build();
      }
    };
  }

  private Log getLog(long volumeId, long blockId) {
    return _cache.get(LogKey.builder()
                            .blockId(blockId)
                            .volumeId(volumeId)
                            .build());
  }

  private File getBlockLogDir(LogKey key) {
    File volumeDir = new File(_walLogDir, Long.toString(key.getVolumeId()));
    File blockDir = new File(volumeDir, Long.toString(key.getBlockId()));
    return blockDir;
  }

}
