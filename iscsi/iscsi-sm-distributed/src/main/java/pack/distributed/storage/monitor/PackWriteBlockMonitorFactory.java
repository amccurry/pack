package pack.distributed.storage.monitor;

import java.io.IOException;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.monitor.rpc.MultiWriteBlockMonitor;

public class PackWriteBlockMonitorFactory {

  private final String _monitorAddress;
  private final int _monitorPort;
  private final String _serverName;

  public PackWriteBlockMonitorFactory(String serverName) {
    _serverName = serverName;
    _monitorAddress = PackConfig.getWriteBlockMonitorAddress();
    _monitorPort = PackConfig.getWriteBlockMonitorPort();
  }

  public WriteBlockMonitor create(String name) throws IOException {
    MultiWriteBlockMonitor client = MultiWriteBlockMonitor.createClientTcp(_monitorAddress, _monitorPort);
    int serverId = client.registerServer(_serverName);
    return new PackWriteBlockMonitorDistributed(name, client, serverId);
  }

}
