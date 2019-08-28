package pack.iscsi.partitioned.storagemanager.cache;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;

import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.block.LocalBlock;
import pack.iscsi.partitioned.storagemanager.BlockKey;
import pack.iscsi.partitioned.storagemanager.BlockStore;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;
import pack.iscsi.partitioned.util.Utils;

public class BlockCacheLoader implements CacheLoader<BlockKey, Block> {

  private static Logger LOGGER = LoggerFactory.getLogger(BlockCacheLoader.class);

  private final BlockStore _blockStore;
  private final BlockWriteAheadLog _writeAheadLog;
  private final File _blockDataDir;
  private final ExternalBlockIOFactory _externalBlockStoreFactory;

  public BlockCacheLoader(BlockStore blockStore, BlockWriteAheadLog writeAheadLog, File blockDataDir,
      ExternalBlockIOFactory externalBlockStoreFactory) {
    _blockDataDir = blockDataDir;
    _blockStore = blockStore;
    _writeAheadLog = writeAheadLog;
    _externalBlockStoreFactory = externalBlockStoreFactory;
  }

  @Override
  public Block load(BlockKey key) throws Exception {
    long volumeId = key.getVolumeId();
    int blockSize = _blockStore.getBlockSize(volumeId);
    LocalBlock block = new LocalBlock(_blockDataDir, volumeId, key.getBlockId(), blockSize, _blockStore,
        _writeAheadLog);
    Utils.runUntilSuccess(LOGGER, () -> {
      block.execIO(_externalBlockStoreFactory.getBlockReader());
      return null;
    });
    Utils.runUntilSuccess(LOGGER, () -> {
      // recover if needed
      block.execIO(_writeAheadLog.getWriteAheadLogReader());
      return null;
    });
    return block;
  }

}
