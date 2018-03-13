package pack.distributed.storage.monitor;

import java.io.IOException;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.monitor.rpc.WriteBlockMonitorRaw;

public class PackWriteBlockMonitorFactory {

  private final String _monitorAddress;
  private final int _monitorPort;

  public PackWriteBlockMonitorFactory() {
    _monitorAddress = PackConfig.getWriteBlockMonitorAddress();
    _monitorPort = PackConfig.getWriteBlockMonitorPort();
  }

  public WriteBlockMonitor create(String name) throws IOException {
    WriteBlockMonitorRaw client = WriteBlockMonitorRaw.createClientTcp(_monitorAddress, _monitorPort);
    return new PackWriteBlockMonitorDistributed(name, client);
  }

}
