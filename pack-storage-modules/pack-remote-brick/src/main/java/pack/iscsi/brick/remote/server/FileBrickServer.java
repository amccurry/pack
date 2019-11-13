package pack.iscsi.brick.remote.server;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.brick.remote.generated.CreateRequest;
import pack.iscsi.brick.remote.generated.CreateResponse;
import pack.iscsi.brick.remote.generated.ExistsRequest;
import pack.iscsi.brick.remote.generated.ExistsResponse;
import pack.iscsi.brick.remote.generated.PackBrickException;
import pack.iscsi.brick.remote.generated.ReadRequest;
import pack.iscsi.brick.remote.generated.ReadResponse;
import pack.iscsi.brick.remote.generated.WriteRequest;
import pack.iscsi.brick.remote.generated.WriteResponse;
import pack.iscsi.io.IOUtils;

public class FileBrickServer extends BaseBrickServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBrickServer.class);

  private final Map<String, FileBrickHandler> _handlers = new ConcurrentHashMap<>();

  public FileBrickServer(BrickServerConfig config) throws TTransportException {
    super(config);
  }

  @Override
  public CreateResponse create(CreateRequest request) throws PackBrickException, TException {
    try {
      String brickId = request.getBrickId();
      validateBrickId(brickId);
      long length = request.getLength();
      File file = getFile(brickId);
      if (file.exists()) {
        throw new PackBrickException("Brick already exists", null);
      }
      FileBrickHandler handler = _handlers.get(brickId);
      if (handler == null) {
        _handlers.put(brickId, handler = new FileBrickHandler(file, length));
      }
      return new CreateResponse();
    } catch (Exception e) {
      throw newPackBrickException(e);
    }
  }

  @Override
  public ReadResponse read(ReadRequest request) throws PackBrickException, TException {
    try {
      FileBrickHandler handler = getHandler(request.getBrickId());
      int length = request.getLength();

      ByteBuffer buffer = ByteBuffer.allocate(length);
      long generationId = handler.read(buffer, request.getPosition());
      buffer.flip();

      return new ReadResponse(generationId, buffer);
    } catch (Exception e) {
      throw newPackBrickException(e);
    }
  }

  @Override
  public WriteResponse write(WriteRequest request) throws PackBrickException, TException {
    try {
      FileBrickHandler handler = getHandler(request.getBrickId());
      ByteBuffer buffer = request.data.duplicate();
      if (request.isInitialize()) {
        return new WriteResponse(handler.write(buffer, request.getPosition(), request.getGenerationId()));
      } else {
        return new WriteResponse(handler.write(buffer, request.getPosition()));
      }
    } catch (Exception e) {
      throw newPackBrickException(e);
    }
  }

  @Override
  public ExistsResponse exists(ExistsRequest request) throws PackBrickException, TException {
    try {
      return new ExistsResponse(_handlers.containsKey(request.getBrickId()));
    } catch (Exception e) {
      throw newPackBrickException(e);
    }
  }

  @Override
  protected void closeFileHandle(String brickId) {
    IOUtils.close(LOGGER, _handlers.remove(brickId));
  }

  private FileBrickHandler getHandler(String brickId) throws IOException {
    FileBrickHandler handler = _handlers.get(brickId);
    if (handler == null) {
      throw new IOException("Missing brick " + brickId);
    }
    return handler;
  }
}
