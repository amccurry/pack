package pack.distributed.storage.wal;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.io.Closer;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.hdfs.PackHdfsWalFactory;
import pack.distributed.storage.zk.EmbeddedZookeeper;
import pack.distributed.storage.zk.PackZooKeeperServer;
import pack.distributed.storage.zk.PackZooKeeperServerConfig;
import pack.distributed.storage.zk.PackZooKeeperWalFactory;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;

public enum WalFactoryType {
  ZK {
    @Override
    public PackWalFactory create(Closer closer) throws IOException {
      String dataZkConnection;
      int sessionTimeout = PackConfig.getZooKeeperSessionTimeout();
      if (PackConfig.isDataZkEmbedded()) {
        EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
        zookeeper.startup();
        closer.register(() -> zookeeper.shutdown());
        dataZkConnection = zookeeper.getConnection();
      } else {
        File zkDir = PackConfig.getZkPath();
        zkDir.mkdirs();
        PackZooKeeperServerConfig myConfig = PackConfig.getPackZooKeeperServerConfig();
        PackZooKeeperServer zooKeeperServer = new PackZooKeeperServer(zkDir, myConfig,
            PackConfig.getAllPackZooKeeperServerConfig());
        closer.register(zooKeeperServer);
        dataZkConnection = zooKeeperServer.getLocalConnection();
      }
      ZooKeeperClient zkBroadCast = ZkUtils.addOnShutdownCloseTrigger(
          ZkUtils.newZooKeeper(dataZkConnection, sessionTimeout));
      return new PackZooKeeperWalFactory(zkBroadCast);
    }
  },
  KAFKA {
    @Override
    public PackWalFactory create(Closer closer) throws IOException {
      throw new IOException("Not implemented");
    }
  },
  HDFS {
    @Override
    public PackWalFactory create(Closer closer) throws IOException {
      Configuration configuration = PackConfig.getConfiguration();
      Path rootWal = PackConfig.getHdfsWalDir();
      return new PackHdfsWalFactory(configuration, rootWal);
    }
  };

  public abstract PackWalFactory create(Closer closer) throws IOException;
}