package pack.volume.server;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import pack.volume.thrift.generated.CreateRequest;
import pack.volume.thrift.generated.CreateResponse;
import pack.volume.thrift.generated.DestroyRequest;
import pack.volume.thrift.generated.DestroyResponse;
import pack.volume.thrift.generated.DiscardRequest;
import pack.volume.thrift.generated.DiscardResponse;
import pack.volume.thrift.generated.ExistsRequest;
import pack.volume.thrift.generated.ExistsResponse;
import pack.volume.thrift.generated.ListRequest;
import pack.volume.thrift.generated.ListResponse;
import pack.volume.thrift.generated.PackVolumeException;
import pack.volume.thrift.generated.ReadRequest;
import pack.volume.thrift.generated.ReadResponse;
import pack.volume.thrift.generated.UpdateRequest;
import pack.volume.thrift.generated.UpdateResponse;
import pack.volume.thrift.generated.WriteRequest;
import pack.volume.thrift.generated.WriteResponse;

public class VolumeServer extends BaseVolumeServer {

  public VolumeServer(VolumeServerConfig config) throws TTransportException {
    super(config);
  }

  @Override
  public CreateResponse createVolume(CreateRequest request) throws PackVolumeException, TException {
    // create volume in s3
    // create brick entries?
    throw new RuntimeException("not impl");
  }

  @Override
  public UpdateResponse updateVolume(UpdateRequest request) throws PackVolumeException, TException {
    // update volume in s3, probably just size
    throw new RuntimeException("not impl");
  }

  @Override
  public ListResponse listVolume(ListRequest request) throws PackVolumeException, TException {
    // list volumes assigned to this cluster? or all? do handle assigned?
    throw new RuntimeException("not impl");
  }

  @Override
  public DestroyResponse destroyVolume(DestroyRequest request) throws PackVolumeException, TException {
    throw new RuntimeException("not impl");
  }

  @Override
  public ExistsResponse existsVolume(ExistsRequest request) throws PackVolumeException, TException {
    throw new RuntimeException("not impl");
  }

  @Override
  public ReadResponse readVolume(ReadRequest request) throws PackVolumeException, TException {
    throw new RuntimeException("not impl");
  }

  @Override
  public WriteResponse writeVolume(WriteRequest request) throws PackVolumeException, TException {
    throw new RuntimeException("not impl");
  }

  @Override
  public DiscardResponse discardVolume(DiscardRequest request) throws PackVolumeException, TException {
    throw new RuntimeException("not impl");
  }

}
