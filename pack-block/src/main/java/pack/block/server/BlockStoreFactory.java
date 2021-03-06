package pack.block.server;

import java.io.File;
import java.io.IOException;

import com.codahale.metrics.MetricRegistry;

import pack.block.blockstore.BlockStore;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImpl;
import pack.block.blockstore.hdfs.util.RetryBlockStore;
import pack.block.blockstore.hdfs.util.UgiHdfsBlockStore;
import pack.block.server.admin.BlockPackAdmin;
import pack.block.server.admin.Status;
import pack.block.server.json.BlockPackFuseConfig;
import pack.block.server.json.BlockPackFuseConfigInternal;

public abstract class BlockStoreFactory {

  public static final BlockStoreFactory DEFAULT = new BlockStoreFactoryImpl();

  public abstract BlockStore getHdfsBlockStore(BlockPackAdmin blockPackAdmin,
      BlockPackFuseConfigInternal packFuseConfig, MetricRegistry registry) throws IOException;

  public static class BlockStoreFactoryImpl extends BlockStoreFactory {
    private static final String BRICK = "brick";

    @Override
    public BlockStore getHdfsBlockStore(BlockPackAdmin blockPackAdmin, BlockPackFuseConfigInternal packFuseConfig,
        MetricRegistry registry) throws IOException {
      blockPackAdmin.setStatus(Status.INITIALIZATION, "Opening Blockstore");
      BlockPackFuseConfig blockPackFuseConfig = packFuseConfig.getBlockPackFuseConfig();
      String fsLocalCache = blockPackFuseConfig.getFsLocalCache();
      File brick = new File(blockPackFuseConfig.getFuseMountLocation(), BRICK);
      File cacheDir = new File(fsLocalCache);
      cacheDir.mkdirs();
      HdfsBlockStoreImpl blockStore = new HdfsBlockStoreImpl(registry, cacheDir, packFuseConfig.getFileSystem(),
          packFuseConfig.getPath(), packFuseConfig.getConfig(), brick);
      UgiHdfsBlockStore ugiHdfsBlockStore = UgiHdfsBlockStore.wrap(blockStore);
      RetryBlockStore retryBlockStore = RetryBlockStore.wrap(ugiHdfsBlockStore);
      return retryBlockStore;
    }
  }

}
