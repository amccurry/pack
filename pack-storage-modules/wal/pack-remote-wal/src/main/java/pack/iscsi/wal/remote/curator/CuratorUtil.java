package pack.iscsi.wal.remote.curator;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

public class CuratorUtil {

  private static final Splitter PORT_SPLITTER = Splitter.on(':');
  private static final String _0_0_0_0 = "0.0.0.0";
  private static final String WAL_HOSTS = "/wal/hosts";

  public static void registerServer(CuratorFramework curatorFramework, String zkPrefix, InetAddress inetAddress,
      int port) throws Exception {
    if (curatorFramework != null) {
      curatorFramework.create()
                      .creatingParentsIfNeeded()
                      .withMode(CreateMode.EPHEMERAL)
                      .forPath(getHostPath(zkPrefix, inetAddress, port));
    }
  }

  private static String getHostPath(String zkPrefix, InetAddress inetAddress, int port) throws Exception {
    return getHostsPath(zkPrefix) + "/" + getHostPlusPort(inetAddress, port);
  }

  private static String getHostsPath(String zkPrefix) {
    if (zkPrefix == null || zkPrefix.isEmpty()) {
      return WAL_HOSTS;
    } else {
      return zkPrefix + WAL_HOSTS;
    }
  }

  private static String getHostPlusPort(InetAddress inetAddress, int port) throws Exception {
    if (inetAddress.getHostAddress()
                   .equals(_0_0_0_0)) {
      inetAddress = InetAddress.getLocalHost();
    }
    return inetAddress.getHostAddress() + ":" + port;
  }

  public static void removeRegistration(CuratorFramework curatorFramework, String zkPrefix, InetAddress inetAddress,
      int port) throws Exception {
    if (curatorFramework != null) {
      curatorFramework.delete()
                      .forPath(getHostPath(zkPrefix, inetAddress, port));
    }
  }

  public static List<PackWalHostEntry> getRegisteredServers(CuratorFramework curatorFramework, String zkPrefix)
      throws Exception {
    if (curatorFramework == null) {
      return ImmutableList.of();
    }
    List<String> list = curatorFramework.getChildren()
                                        .forPath(getHostsPath(zkPrefix));

    List<PackWalHostEntry> entries = new ArrayList<>();
    for (String s : list) {
      List<String> parts = PORT_SPLITTER.splitToList(s);
      entries.add(PackWalHostEntry.builder()
                                  .hostname(parts.get(0))
                                  .port(Integer.parseInt(parts.get(1)))
                                  .build());
    }
    return entries;
  }
}
