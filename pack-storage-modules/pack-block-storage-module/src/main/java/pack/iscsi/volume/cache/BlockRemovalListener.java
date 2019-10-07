package pack.iscsi.volume.cache;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

import io.opentracing.Scope;
import pack.iscsi.spi.BlockKey;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.util.Utils;
import pack.util.tracer.TracerUtil;

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
      Block block = takeBlock(key);
      if (block != null) {
        LOGGER.info("volume id {} block id {} stolen from removal", key.getVolumeId(), key.getBlockId());
      }
      return block;
    }
  }

  @Override
  public void onRemoval(BlockKey key, Block block, RemovalCause cause) {
    if (block == null) {
      return;
    }
    try (Scope scope = TracerUtil.trace(BlockRemovalListener.class, "block cache removal add")) {
      add(key, block);
    }
    try (Scope scope1 = TracerUtil.trace(BlockRemovalListener.class, "block cache removal sync")) {
      if (sync(key)) {
        try (Scope scope2 = TracerUtil.trace(BlockRemovalListener.class, "block cache removal destroy")) {
          destroy(key);
        }
      }
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
