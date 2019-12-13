package pack.backstore.file.client;

import java.io.Closeable;
import java.io.IOException;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.backstore.thrift.generated.BackstoreFileService;
import pack.iscsi.io.IOUtils;

public class FileServiceClient extends BackstoreFileService.Client implements BackstoreFileService.Iface, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileServiceClient.class);

  private final TSocket _transport;

  public FileServiceClient(TProtocol protocol, TSocket transport) {
    super(protocol);
    _transport = transport;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, _transport);
  }

}
