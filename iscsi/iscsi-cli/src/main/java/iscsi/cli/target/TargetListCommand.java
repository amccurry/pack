package iscsi.cli.target;

import java.util.List;

import org.apache.commons.cli.CommandLine;

import com.fasterxml.jackson.databind.ObjectMapper;

import iscsi.cli.Command;
import iscsi.cli.PackCommandGroup;
import pack.distributed.storage.PackConfig;
import pack.distributed.storage.http.TargetServerInfo;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;

public class TargetListCommand extends Command {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public TargetListCommand() {
    super("ls");
  }

  @Override
  protected void handleCommand(CommandLine commandLine) throws Exception {
    String zkConn = PackConfig.getZooKeeperConnection();
    int zkTimeout = PackConfig.getZooKeeperSessionTimeout();
    try (ZooKeeperClient zk = ZkUtils.newZooKeeper(zkConn, zkTimeout)) {
      List<TargetServerInfo> list = TargetServerInfo.list(zk);
      if (!PackCommandGroup.outputJson()) {
        System.out.printf("%-20s %-20s %-20s%n", "Target Server", "Address", "Bind Address");
      } else {
        System.out.print("[");
      }
      boolean comma = false;
      for (TargetServerInfo targetServerInfo : list) {
        if (!PackCommandGroup.outputJson()) {
          System.out.printf("%-20s %-20s %-20s%n", targetServerInfo.getHostname(), targetServerInfo.getAddress(),
              targetServerInfo.getBindAddress());
        } else {
          if (comma) {
            System.out.print(',');
          }
          System.out.print(MAPPER.writeValueAsString(targetServerInfo));
          comma = true;
        }
      }
      if (PackCommandGroup.outputJson()) {
        System.out.print("]");
      }
    }
  }

  @Override
  public String getDescription() {
    return "List the target servers";
  }

}
