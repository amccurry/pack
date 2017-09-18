package pack.block.server;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;

import com.codahale.metrics.MetricRegistry;

import pack.block.blockstore.hdfs.HdfsBlockStore;
import pack.block.blockstore.hdfs.UgiHdfsBlockStore;
import pack.block.blockstore.hdfs.v1.HdfsBlockStoreV1;
import pack.block.blockstore.hdfs.v2.HdfsBlockStoreV2;
import pack.block.blockstore.hdfs.v3.HdfsBlockStoreV3;
import pack.block.server.admin.BlockPackAdmin;
import pack.block.server.admin.Status;

public abstract class BlockStoreFactory {

  public static final BlockStoreFactory DEFAULT = new V3BlockStoreFactory();

  public abstract HdfsBlockStore getHdfsBlockStore(BlockPackAdmin blockPackAdmin, BlockPackFuseConfig packFuseConfig,
      UserGroupInformation ugi, MetricRegistry registry) throws IOException;

  public static class V1BlockStoreFactory extends BlockStoreFactory {
    @Override
    public HdfsBlockStore getHdfsBlockStore(BlockPackAdmin blockPackAdmin, BlockPackFuseConfig packFuseConfig,
        UserGroupInformation ugi, MetricRegistry registry) throws IOException {
      blockPackAdmin.setStatus(Status.INITIALIZATION, "Opening Blockstore");
      return UgiHdfsBlockStore.wrap(ugi, new HdfsBlockStoreV1(registry, packFuseConfig.getFileSystem(),
          packFuseConfig.getPath(), packFuseConfig.getConfig()));
    }
  }

  public static class V2BlockStoreFactory extends BlockStoreFactory {
    @Override
    public HdfsBlockStore getHdfsBlockStore(BlockPackAdmin blockPackAdmin, BlockPackFuseConfig packFuseConfig,
        UserGroupInformation ugi, MetricRegistry registry) throws IOException {
      blockPackAdmin.setStatus(Status.INITIALIZATION, "Opening Blockstore");
      File fsLocalCacheDir = new File(packFuseConfig.getFsLocalCache());
      fsLocalCacheDir.mkdirs();
      return UgiHdfsBlockStore.wrap(ugi, new HdfsBlockStoreV2(fsLocalCacheDir, packFuseConfig.getFileSystem(),
          packFuseConfig.getPath(), packFuseConfig.getConfig()));
    }
  }

  public static class V3BlockStoreFactory extends BlockStoreFactory {
    @Override
    public HdfsBlockStore getHdfsBlockStore(BlockPackAdmin blockPackAdmin, BlockPackFuseConfig packFuseConfig,
        UserGroupInformation ugi, MetricRegistry registry) throws IOException {
      blockPackAdmin.setStatus(Status.INITIALIZATION, "Opening Blockstore");
      return UgiHdfsBlockStore.wrap(ugi, new HdfsBlockStoreV3(registry, packFuseConfig.getFileSystem(),
          packFuseConfig.getPath(), packFuseConfig.getConfig()));
    }
  }

}
