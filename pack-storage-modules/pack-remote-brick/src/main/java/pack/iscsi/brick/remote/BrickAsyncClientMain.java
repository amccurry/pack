package pack.iscsi.brick.remote;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.async.AsyncMethodCallback;

import io.opentracing.Scope;
import pack.iscsi.brick.remote.generated.CreateRequest;
import pack.iscsi.brick.remote.generated.DestroyRequest;
import pack.iscsi.brick.remote.generated.ListBricksRequest;
import pack.iscsi.brick.remote.generated.ListBricksResponse;
import pack.iscsi.brick.remote.generated.WriteRequest;
import pack.iscsi.brick.remote.generated.WriteResponse;
import pack.util.tracer.TracerUtil;

public class BrickAsyncClientMain {

  public static void main(String[] args) throws Exception {

    BrickClientConfig config = BrickClientConfig.builder()
                                                .hostname("localhost")
                                                .build();

    try (BrickAsyncClient asyncClient = BrickAsyncClient.create(config)) {
      try (BrickClient client = BrickClient.create(config)) {
        int length = 1_000_000_000;
        int brickId = 0;
        ListBricksResponse bricks = client.listBricks(new ListBricksRequest());
        for (Long id : bricks.getBrickIds()) {
          client.destroy(new DestroyRequest(id));
        }
        client.create(new CreateRequest(brickId, length));
        Random random = new Random();
        byte[] buffer = new byte[512 * 1024 / 8];
        random.nextBytes(buffer);
        long start = System.nanoTime();
        long total = 0;
        AtomicLong sent = new AtomicLong();
        AtomicLong confirmed = new AtomicLong();
        while (true) {
          long now = System.nanoTime();
          if (start + 5_000_000_000L < now) {
            double totalMB = total / 1024.0 / 1024.0;
            double seconds = (now - start) / 1_000_000_000.0;
            System.out.println(totalMB / seconds + " MiB/s " + sent + " " + confirmed);
            total = 0;
            start = System.nanoTime();
          }
          long position = (random.nextInt(length - buffer.length) / 4096) * 4096;
          ByteBuffer data = ByteBuffer.wrap(buffer);

          try (Scope scope = TracerUtil.trace(BrickAsyncClientMain.class, "write")) {
            AsyncMethodCallback<WriteResponse> callback = new AsyncMethodCallback<WriteResponse>() {
              @Override
              public void onError(Exception exception) {
                exception.printStackTrace();
              }

              @Override
              public void onComplete(WriteResponse response) {
                confirmed.incrementAndGet();
              }
            };
            asyncClient.write(new WriteRequest(brickId, position, data), callback);
            sent.incrementAndGet();
            total += buffer.length;
          }
        }
      }
    }
  }

}
