package pack.backstore.volume.server;

import java.io.Closeable;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.backstore.thrift.common.BackstoreServiceExceptionHelper;
import pack.backstore.thrift.common.BaseServer;
import pack.backstore.thrift.generated.BackstoreServiceException;
import pack.backstore.thrift.generated.BackstoreVolumeService;
import pack.backstore.thrift.generated.CreateVolumeRequest;
import pack.backstore.thrift.generated.DestroyVolumeRequest;
import pack.backstore.thrift.generated.DiscardRequestBatch;
import pack.backstore.thrift.generated.ReadVolumeRequestBatch;
import pack.backstore.thrift.generated.ReadVolumeResponseBatch;
import pack.backstore.thrift.generated.WriteVolumeRequestBatch;

public class VolumeServer extends BaseServer
    implements BackstoreVolumeService.Iface, Closeable, BackstoreServiceExceptionHelper {

  private static Logger LOGGER = LoggerFactory.getLogger(VolumeServer.class);

  public VolumeServer(VolumeServerConfig config) throws TTransportException {
    super(LOGGER, config);

  }

  @Override
  protected TProcessor createTProcessor() {
    return new BackstoreVolumeService.Processor<>(this);
  }

  @Override
  public void createVolume(CreateVolumeRequest request) throws BackstoreServiceException, TException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void destroyVolume(DestroyVolumeRequest request) throws BackstoreServiceException, TException {
    throw new RuntimeException("not impl");
  }

  @Override
  public ReadVolumeResponseBatch readVolume(ReadVolumeRequestBatch request)
      throws BackstoreServiceException, TException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void writeVolume(WriteVolumeRequestBatch request) throws BackstoreServiceException, TException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void discardVolume(DiscardRequestBatch request) throws BackstoreServiceException, TException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void noop() throws BackstoreServiceException, TException {

  }

}
