package pack.distributed.storage;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;

public class ZKTesting {

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    try (ZooKeeperClient zk = ZkUtils.newZooKeeper("c-sao-1.pdev.sigma.dsci/testing", 30000)) {
      zk.create("/atest", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      printStat(zk, "/atest");
      zk.create("/atest/d", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      printStat(zk, "/atest");
    }
  }

  private static void printStat(ZooKeeperClient zk, String path) throws KeeperException, InterruptedException {
    Stat stat = zk.exists(path, false);
    System.out.println(stat.getAversion());
    System.out.println(stat.getCversion());
    System.out.println(stat.getVersion());
    System.out.println(stat.getCzxid());
    System.out.println(stat.getMzxid());
    System.out.println(stat.getPzxid());
  }

}
