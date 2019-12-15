package pack.backstore.volume.server;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.backstore.file.client.FileServiceClient;
import pack.backstore.thrift.common.BackstoreServiceExceptionHelper;
import pack.backstore.thrift.common.BaseServer;
import pack.backstore.thrift.generated.BackstoreServiceException;
import pack.backstore.thrift.generated.BackstoreVolumeService;
import pack.backstore.thrift.generated.CreateVolumeRequest;
import pack.backstore.thrift.generated.DestroyVolumeRequest;
import pack.backstore.thrift.generated.DiscardRequestBatch;
import pack.backstore.thrift.generated.ReadRequest;
import pack.backstore.thrift.generated.ReadResponse;
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
    try {
      long volumeId = request.getVolumeId();
      long volumeLength = getVolumeLength(volumeId);
      // get volume info here and pass it down...

      // check is volume exists
      checkVolumeExists(volumeId);
      List<ReadRequest> readRequests = request.getReadRequests();
      List<ReadResponse> readResponses = new ArrayList<>();
      for (ReadRequest readRequest : readRequests) {
        readResponses.add(doRead(volumeId, readRequest, volumeLength));
      }
      return new ReadVolumeResponseBatch(readResponses);
    } catch (Throwable t) {
      throw newException(t);
    }
  }

  private long getVolumeLength(long volumeId) {
    throw new RuntimeException("Not impl");
  }

  private void checkVolumeExists(long volumeId) {
    throw new RuntimeException("Not impl");
  }

  private ReadResponse doRead(long volumeId, ReadRequest readRequest, long volumeLength)
      throws BackstoreServiceException {
    long position = readRequest.getPosition();
    int length = readRequest.getLength();
    checkPositionAndLength(volumeId, volumeLength, position, length);
    
    throw new RuntimeException("Not impl");
  }

  private void checkPositionAndLength(long volumeId, long volumeLength, long position, int length)
      throws BackstoreServiceException {
    if (position < 0) {
      throw positionCannotBeLessThanZero(volumeId, position);
    }
    if (length < 0) {
      throw lengthCannotBeLessThanZero(volumeId, length);
    }
    if (length + position >= volumeLength) {
      throw readBeyondEndOfVolume(volumeId, volumeLength, length, position);
    }
    throw new RuntimeException("Not impl");
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
