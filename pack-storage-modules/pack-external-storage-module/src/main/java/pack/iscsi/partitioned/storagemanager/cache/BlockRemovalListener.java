package pack.iscsi.partitioned.storagemanager.cache;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.storagemanager.BlockKey;
import pack.iscsi.partitioned.storagemanager.BlockIOFactory;
import pack.iscsi.partitioned.util.Utils;

public class BlockRemovalListener implements RemovalListener<BlockKey, Block> {

  private static Logger LOGGER = LoggerFactory.getLogger(BlockRemovalListener.class);

  private final BlockIOFactory _externalBlockStoreFactory;
  private final ConcurrentMap<BlockKey, Block> _blockMap;
  private final Object _lock = new Object();

  public BlockRemovalListener(BlockRemovalListenerConfig config) {
    _externalBlockStoreFactory = config.getExternalBlockStoreFactory();
    _blockMap = new ConcurrentHashMap<>();
  }

  public Block stealBlock(BlockKey key) {
    synchronized (_lock) {
      return takeBlock(key);
    }
  }

  @Override
  public void onRemoval(BlockKey key, Block block, RemovalCause cause) {
    if (block == null) {
      return;
    }
    add(key, block);
    if (sync(key)) {
      destroy(key);
    }
  }

  private boolean destroy(BlockKey key) {
    synchronized (_lock) {
      Block block = takeBlock(key);
      if (block == null) {
        return false;
      }
      try {
        block.close();
      } catch (IOException e) {
        LOGGER.error("Unknown error", e);
      }
      try {
        block.cleanUp();
      } catch (IOException e) {
        LOGGER.error("Unknown error", e);
      }
      return true;
    }
  }

  private boolean sync(BlockKey key) {
    Block block = getBlock(key);
    if (block == null) {
      return false;
    }
    Utils.runUntilSuccess(LOGGER, () -> {
      block.execIO(_externalBlockStoreFactory.getBlockWriter());
      return null;
    });
    return true;
  }

  private Block getBlock(BlockKey key) {
    return _blockMap.get(key);
  }

  private Block takeBlock(BlockKey key) {
    return _blockMap.remove(key);
  }

  private void add(BlockKey key, Block block) {
    _blockMap.put(key, block);
  }

}
