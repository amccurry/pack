package pack.backstore.volume.client;

import java.io.Closeable;
import java.io.IOException;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.backstore.thrift.generated.BackstoreVolumeService;
import pack.iscsi.io.IOUtils;

public class VolumeServiceClient extends BackstoreVolumeService.Client implements BackstoreVolumeService.Iface, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(VolumeServiceClient.class);

  private final TSocket _transport;

  public VolumeServiceClient(TProtocol protocol, TSocket transport) {
    super(protocol);
    _transport = transport;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, _transport);
  }

}
