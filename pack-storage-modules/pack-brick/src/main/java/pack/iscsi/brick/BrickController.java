package pack.iscsi.brick;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import pack.iscsi.brick.remote.client.BrickClient;
import pack.iscsi.brick.remote.generated.WriteRequest;
import pack.rs.RS;
import pack.rs.RSProcessor;

public class BrickController {

  private final RSProcessor _processor;
  private final int _minStripeSize;
  private final int _blockSize;

  public BrickController(BrickControllerConfig config) throws IOException {
    _blockSize = config.getBlockSize();
    int dataPartCount = config.getDataPartCount();
    _minStripeSize = _blockSize / dataPartCount;
    checkDatapartCount(dataPartCount);
    checkBlockSize(_blockSize, dataPartCount);
    _processor = RS.create(config.getByteBufferSize(), dataPartCount, config.getParityPartCount(), _minStripeSize);
  }

  public void write(long position, byte[] buffer, int offset, int length) throws IOException {
    checkLength(length);
    checkPosition(position);
    // _processor.reset();
    // _processor.write(ByteBuffer.wrap(buffer, offset, length));
    // ByteBuffer[] byteBuffers = _processor.finish();
    // // perform writes...
    // List<BrickClient> clients = getClients(byteBuffers);
    // for (int i = 0; i < byteBuffers.length; i++) {
    // BrickClient client = clients.get(i);
    // WriteRequest request = new WriteRequest();
    // request.setBrickId(brickId);
    // request.set
    // client.write(request);
    // }
  }

  private List<BrickClient> getClients(ByteBuffer[] byteBuffers) {
    throw new RuntimeException("not impl");
  }

  public void read(long position, byte[] buffer, int offset, int length) throws IOException {
    checkLength(length);
    checkPosition(position);

  }

  public void delete(long position, int length) throws IOException {
    checkLength(length);
    checkPosition(position);

  }

  private void checkBlockSize(int blockSize, int dataPartCount) throws IOException {
    if (blockSize % 512 != 0) {
      throw new IOException("blocksize not a multiple of 512");
    }
    if (blockSize / dataPartCount < 512) {
      throw new IOException("blocksize " + blockSize + " not large enough with dataPartCount of = " + dataPartCount);
    }
  }

  private void checkDatapartCount(int dataPartCount) throws IOException {
    switch (dataPartCount) {
    case 1:
    case 2:
    case 4:
    case 8:
      return;
    default:
      throw new IOException("dataPartCount supported: 1, 2, 4, 8");
    }
  }

  private void checkLength(int length) throws IOException {
    if (length % _blockSize != 0) {
      throw new IOException("Length " + length + " not multiple of blocksize = " + _blockSize);
    }
  }

  private void checkPosition(long position) throws IOException {
    if (position % _blockSize != 0) {
      throw new IOException("Position " + position + " not positioned at block start, blocksize = " + _blockSize);
    }
  }

}
