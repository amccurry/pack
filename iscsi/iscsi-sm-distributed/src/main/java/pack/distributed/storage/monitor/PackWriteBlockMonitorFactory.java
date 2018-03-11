package pack.distributed.storage.monitor;

public class PackWriteBlockMonitorFactory {

  public WriteBlockMonitor create(String name) {
    return new PackWriteBlockMonitor();
  }

}
