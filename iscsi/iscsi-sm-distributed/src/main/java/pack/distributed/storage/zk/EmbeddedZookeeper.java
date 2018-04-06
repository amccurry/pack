package pack.distributed.storage.zk;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import pack.iscsi.storage.utils.PackUtils;

public class EmbeddedZookeeper {
  private int port = -1;
  private int tickTime = 500;

  private ServerCnxnFactory factory;
  private File snapshotDir;
  private File logDir;

  public EmbeddedZookeeper() {
    this(-1);
  }

  public EmbeddedZookeeper(int port) {
    this(port, 500);
  }

  public EmbeddedZookeeper(int port, int tickTime) {
    this.port = resolvePort(port);
    this.tickTime = tickTime;
  }

  private int resolvePort(int port) {
    if (port == -1) {
      return PackUtils.getAvailablePort();
    }
    return port;
  }

  public void startup() throws IOException {
    if (this.port == -1) {
      this.port = PackUtils.getAvailablePort();
    }
    this.factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 1024);
    this.snapshotDir = PackUtils.constructTempDir("embeeded-zk/snapshot");
    this.logDir = PackUtils.constructTempDir("embeeded-zk/log");

    try {
      factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public void shutdown() {
    factory.shutdown();
    PackUtils.rmr(snapshotDir);
    PackUtils.rmr(logDir);
  }

  public String getConnection() {
    return "localhost:" + port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setTickTime(int tickTime) {
    this.tickTime = tickTime;
  }

  public int getPort() {
    return port;
  }

  public int getTickTime() {
    return tickTime;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("EmbeddedZookeeper{");
    sb.append("connection=")
      .append(getConnection());
    sb.append('}');
    return sb.toString();
  }
}