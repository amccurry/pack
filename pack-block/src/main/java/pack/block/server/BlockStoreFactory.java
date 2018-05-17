package pack.block.server;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;

import com.codahale.metrics.MetricRegistry;

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.UgiHdfsBlockStore;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImpl;
import pack.block.server.admin.BlockPackAdmin;
import pack.block.server.admin.Status;
import pack.block.server.json.BlockPackFuseConfigInternal;

public abstract class BlockStoreFactory {

  public static final BlockStoreFactory DEFAULT = new BlockStoreFactoryImpl();

  public abstract HdfsBlockStore getHdfsBlockStore(BlockPackAdmin blockPackAdmin,
      BlockPackFuseConfigInternal packFuseConfig, UserGroupInformation ugi, MetricRegistry registry) throws IOException;

  public static class BlockStoreFactoryImpl extends BlockStoreFactory {
    @Override
    public HdfsBlockStore getHdfsBlockStore(BlockPackAdmin blockPackAdmin, BlockPackFuseConfigInternal packFuseConfig,
        UserGroupInformation ugi, MetricRegistry registry) throws IOException {
      blockPackAdmin.setStatus(Status.INITIALIZATION, "Opening Blockstore");
      String fsLocalCache = packFuseConfig.getBlockPackFuseConfig()
                                          .getFsLocalCache();
      File cacheDir = new File(fsLocalCache);
      cacheDir.mkdirs();
      return UgiHdfsBlockStore.wrap(ugi, new HdfsBlockStoreImpl(registry, cacheDir, packFuseConfig.getFileSystem(),
          packFuseConfig.getPath(), packFuseConfig.getConfig()));
    }
  }

}
