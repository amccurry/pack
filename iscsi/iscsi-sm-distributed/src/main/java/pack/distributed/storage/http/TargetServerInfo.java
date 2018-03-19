package pack.distributed.storage.http;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;

@Value
@Builder
@AllArgsConstructor
public class TargetServerInfo {
  String hostname;
  String address;
  String bindAddress;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String TARGET_SERVERS = "/target-servers";

  public static void register(ZooKeeperClient zk, TargetServerInfo info) throws IOException {
    ZkUtils.mkNodesStr(zk, TARGET_SERVERS);
    try {
      zk.create(TARGET_SERVERS + "/" + info.getBindAddress(), toBytes(info), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  private static byte[] toBytes(TargetServerInfo info) throws IOException {
    return MAPPER.writeValueAsBytes(info);
  }

  private static TargetServerInfo fromBytes(byte[] data) throws IOException {
    return MAPPER.readValue(data, TargetServerInfo.class);
  }

  public static List<TargetServerInfo> list(ZooKeeperClient zk) {
    try {
      if (zk.exists(TARGET_SERVERS, false) == null) {
        return ImmutableList.of();
      }
      List<String> list = zk.getChildren(TARGET_SERVERS, false);
      ImmutableList.Builder<TargetServerInfo> builder = ImmutableList.builder();
      for (String s : list) {
        Stat stat = zk.exists(TARGET_SERVERS + "/" + s, false);
        if (stat != null) {
          byte[] data = zk.getData(TARGET_SERVERS + "/" + s, false, stat);
          builder.add(fromBytes(data));
        }
      }
      return builder.build();
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

}
