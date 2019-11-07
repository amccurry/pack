package pack.iscsi.brick.remote;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import pack.iscsi.brick.remote.generated.CreateRequest;
import pack.iscsi.brick.remote.generated.CreateResponse;
import pack.iscsi.brick.remote.generated.DestroyRequest;
import pack.iscsi.brick.remote.generated.DestroyResponse;
import pack.iscsi.brick.remote.generated.ListBricksRequest;
import pack.iscsi.brick.remote.generated.ListBricksResponse;
import pack.iscsi.brick.remote.generated.PackBrickService;
import pack.iscsi.brick.remote.generated.PackBrickService.AsyncClient;
import pack.iscsi.brick.remote.generated.ReadRequest;
import pack.iscsi.brick.remote.generated.ReadResponse;
import pack.iscsi.brick.remote.generated.WriteRequest;
import pack.iscsi.brick.remote.generated.WriteResponse;
import pack.iscsi.io.IOUtils;
import pack.util.tracer.TracerUtil;

public class BrickAsyncClient implements PackBrickService.AsyncIface, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BrickAsyncClient.class);

  private final BrickClientConfig _config;
  private final BlockingQueue<CloseableAsyncClient> _clients;
  private final AtomicInteger _clientCount = new AtomicInteger();
  private final int _maxClientCount;

  public BrickAsyncClient(BrickClientConfig config) {
    _config = config;
    _maxClientCount = config.getMaxClientCount();
    _clients = new ArrayBlockingQueue<>(_maxClientCount);
  }

  public static BrickAsyncClient create(BrickClientConfig config) throws IOException {
    return new BrickAsyncClient(config);
  }

  @Override
  public void close() throws IOException {
    List<CloseableAsyncClient> closeables = new ArrayList<>();
    _clients.drainTo(closeables);
    IOUtils.close(LOGGER, closeables.toArray(new Closeable[] {}));
  }

  @Override
  public void create(CreateRequest request, AsyncMethodCallback<CreateResponse> resultHandler) throws TException {
    CloseableAsyncClient client = getClient();
    client.create(request, createResultHandler(resultHandler, client));
  }

  @Override
  public void read(ReadRequest request, AsyncMethodCallback<ReadResponse> resultHandler) throws TException {
    CloseableAsyncClient client = getClient();
    client.read(request, createResultHandler(resultHandler, client));
  }

  @Override
  public void write(WriteRequest request, AsyncMethodCallback<WriteResponse> resultHandler) throws TException {
    CloseableAsyncClient client = getClient();
    client.write(request, createResultHandler(resultHandler, client));
  }

  @Override
  public void writeNonBlocking(WriteRequest request, AsyncMethodCallback<Void> resultHandler) throws TException {
    CloseableAsyncClient client = getClient();
    client.writeNonBlocking(request, createResultHandler(resultHandler, client));
  }

  @Override
  public void listBricks(ListBricksRequest request, AsyncMethodCallback<ListBricksResponse> resultHandler)
      throws TException {
    CloseableAsyncClient client = getClient();
    client.listBricks(request, createResultHandler(resultHandler, client));
  }

  @Override
  public void destroy(DestroyRequest request, AsyncMethodCallback<DestroyResponse> resultHandler) throws TException {
    CloseableAsyncClient client = getClient();
    client.destroy(request, createResultHandler(resultHandler, client));
  }

  @Override
  public void noop(AsyncMethodCallback<Void> resultHandler) throws TException {
    CloseableAsyncClient client = getClient();
    client.noop(createResultHandler(resultHandler, client));
  }

  private <T> AsyncMethodCallback<T> createResultHandler(AsyncMethodCallback<T> resultHandler,
      CloseableAsyncClient client) {
    return new AsyncMethodCallback<T>() {
      @Override
      public void onComplete(T response) {
        resultHandler.onComplete(response);
        release(client);
      }

      @Override
      public void onError(Exception exception) {
        resultHandler.onError(exception);
        destroy(client);
      }
    };
  }

  private static class CloseableAsyncClient extends AsyncClient implements Closeable {

    private final TNonblockingTransport _transport;
    private final TAsyncClientManager _clientManager;

    public CloseableAsyncClient(TProtocolFactory protocolFactory, TAsyncClientManager clientManager,
        TNonblockingTransport transport) {
      super(protocolFactory, clientManager, transport);
      _transport = transport;
      _clientManager = clientManager;
    }

    @Override
    public void close() throws IOException {
      _clientManager.stop();
      IOUtils.close(LOGGER, _transport);
    }
  }

  private void destroy(CloseableAsyncClient client) {
    _clientCount.decrementAndGet();
    IOUtils.close(LOGGER, client);
  }

  private void release(CloseableAsyncClient client) {
    if (!_clients.offer(client)) {
      destroy(client);
    }
  }

  private CloseableAsyncClient getClient() {
    CloseableAsyncClient client = _clients.poll();
    if (client != null) {
      return client;
    } else {
      return newClient();
    }
  }

  private CloseableAsyncClient newClient() {
    while (_clientCount.get() >= _maxClientCount) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    try (Scope scope = TracerUtil.trace(BrickClient.class, "create")) {
      LOGGER.debug("Creating a new client host {} port {}", _config.getHostname(), _config.getPort());
      TNonblockingTransport transport = new TNonblockingSocket(_config.getHostname(), _config.getPort(),
          _config.getClientTimeout());
      TAsyncClientManager clientManager = new TAsyncClientManager();
      TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
      CloseableAsyncClient client = new CloseableAsyncClient(factory, clientManager, transport);
      _clientCount.incrementAndGet();
      return client;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
