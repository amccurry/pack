package pack.distributed.storage.http;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import lombok.Builder;
import lombok.Value;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;

@Value
@Builder
public class CompactorServerInfo {
  String hostname;
  String address;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String COMPACTOR_SERVERS = "/compactor-servers";

  public static void register(ZooKeeperClient zk, CompactorServerInfo info) throws IOException {
    ZkUtils.mkNodesStr(zk, COMPACTOR_SERVERS);
    try {
      zk.create(COMPACTOR_SERVERS + "/" + info.getAddress(), toBytes(info), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  private static byte[] toBytes(CompactorServerInfo info) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsBytes(info);
  }
  
  private static CompactorServerInfo fromBytes(byte[] data) throws IOException {
    return MAPPER.readValue(data, CompactorServerInfo.class);
  }

  public static List<CompactorServerInfo> list(ZooKeeperClient zk) {
    try {
      if (zk.exists(COMPACTOR_SERVERS, false) == null) {
        return ImmutableList.of();
      }
      List<String> list = zk.getChildren(COMPACTOR_SERVERS, false);
      ImmutableList.Builder<CompactorServerInfo> builder = ImmutableList.builder();
      for (String s : list) {
        Stat stat = zk.exists(COMPACTOR_SERVERS + "/" + s, false);
        if (stat != null) {
          byte[] data = zk.getData(COMPACTOR_SERVERS + "/" + s, false, stat);
          builder.add(fromBytes(data));
        }
      }
      return builder.build();
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }
}
