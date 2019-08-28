package pack.iscsi.partitioned.storagemanager.cache;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.storagemanager.BlockKey;
import pack.iscsi.partitioned.util.Utils;

public class BlockRemovalListener implements RemovalListener<BlockKey, Block> {

  private static Logger LOGGER = LoggerFactory.getLogger(BlockRemovalListener.class);

  private final ExternalBlockIOFactory _externalBlockStoreFactory;

  public BlockRemovalListener(ExternalBlockIOFactory externalBlockStoreFactory) {
    _externalBlockStoreFactory = externalBlockStoreFactory;
  }

  @Override
  public void onRemoval(BlockKey key, Block block, RemovalCause cause) {
    Utils.runUntilSuccess(LOGGER, () -> {
      block.execIO(_externalBlockStoreFactory.getBlockWriter());
      return null;
    });
    // add to async close and cleanup
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
  }

}
