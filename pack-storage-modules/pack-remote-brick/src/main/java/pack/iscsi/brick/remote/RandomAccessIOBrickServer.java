package pack.iscsi.brick.remote;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import pack.iscsi.brick.remote.generated.CreateRequest;
import pack.iscsi.brick.remote.generated.CreateResponse;
import pack.iscsi.brick.remote.generated.PackBrickException;
import pack.iscsi.brick.remote.generated.ReadRequest;
import pack.iscsi.brick.remote.generated.ReadResponse;
import pack.iscsi.brick.remote.generated.WriteRequest;
import pack.iscsi.brick.remote.generated.WriteResponse;
import pack.iscsi.io.FileIO;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.RandomAccessIO;
import pack.util.tracer.Tag;
import pack.util.tracer.TracerUtil;

public class RandomAccessIOBrickServer extends BaseBrickServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RandomAccessIOBrickServer.class);

  private final Map<Long, RandomAccessIO> _fileHandles = new ConcurrentHashMap<>();
  private final boolean _directIO;
  private final ExecutorService _service;
  private final boolean _async;

  public RandomAccessIOBrickServer(BrickServerConfig config) throws TTransportException {
    super(config);
    _async = config.isAsync();
    _directIO = config.isDirectIO();
    _service = Executors.newCachedThreadPool();
  }

  @Override
  public CreateResponse create(CreateRequest request) throws PackBrickException, TException {
    try {
      long brickId = request.getBrickId();
      long length = request.getLength();
      File file = getFile(brickId);
      if (file.exists()) {
        throw new PackBrickException("Brick already exists", null);
      }
      try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, -1, "rw")) {
        randomAccessIO.setLength(length);
      }
      return new CreateResponse();
    } catch (Exception e) {
      throw newPackBrickException(e);
    }
  }

  @Override
  public ReadResponse read(ReadRequest request) throws PackBrickException, TException {
    try (Scope s0 = TracerUtil.trace(getClass(), "read")) {
      RandomAccessIO randomAccessIO;
      try (Scope s1 = TracerUtil.trace(getClass(), "getRandomAccessIO")) {
        randomAccessIO = getRandomAccessIO(request.getBrickId());
      }
      byte[] data = new byte[request.getLength()];
      long position = request.getPosition();
      try (Scope s1 = TracerUtil.trace(getClass(), "read file", Tag.create("position", position),
          Tag.create("length", data.length))) {
        randomAccessIO.read(position, data);
      }
      return new ReadResponse(ByteBuffer.wrap(data));
    } catch (Exception e) {
      throw newPackBrickException(e);
    }
  }

  @Override
  public WriteResponse write(WriteRequest request) throws PackBrickException, TException {
    Callable<Void> callable = createCallableWrite(request);
    try {
      if (_async) {
        _service.submit(TracerUtil.traceCallable(getClass(), "write async", callable));
      } else {
        callable.call();
      }
    } catch (Exception e) {
      throw newPackBrickException(e);
    }
    return new WriteResponse();
  }

  private Callable<Void> createCallableWrite(WriteRequest request) {
    return () -> {
      try (Scope s0 = TracerUtil.trace(getClass(), "write")) {
        RandomAccessIO randomAccessIO;
        try (Scope s11 = TracerUtil.trace(getClass(), "getRandomAccessIO")) {
          randomAccessIO = getRandomAccessIO(request.getBrickId());
        }
        byte[] data = request.getData();
        long position = request.getPosition();
        try (Scope s12 = TracerUtil.trace(getClass(), "write file", Tag.create("position", position),
            Tag.create("length", data.length))) {
          randomAccessIO.write(position, data);
        }
      }
      return null;
    };
  }

  private RandomAccessIO getRandomAccessIO(long brickId) throws IOException {
    RandomAccessIO randomAccessIO = _fileHandles.get(brickId);
    if (randomAccessIO == null) {
      return createRandomAccessIO(brickId);
    }
    return randomAccessIO;
  }

  private synchronized RandomAccessIO createRandomAccessIO(long brickId) throws IOException {
    RandomAccessIO randomAccessIO = _fileHandles.get(brickId);
    if (randomAccessIO == null) {
      File file = getFile(brickId);
      _fileHandles.put(brickId, randomAccessIO = FileIO.openRandomAccess(file, -1, "rw", _directIO));
    }
    return randomAccessIO;
  }

  protected void closeFileHandle(long brickId) {
    IOUtils.close(LOGGER, _fileHandles.remove(brickId));
  }

}
