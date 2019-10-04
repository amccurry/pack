package pack.iscsi.spi.async;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import pack.util.TracerUtil;

public class AsyncCompletableFuture {

  private CompletableFuture<Void> _future;

  private AsyncCompletableFuture(CompletableFuture<Void> future) {
    _future = future;
  }

  public static AsyncCompletableFuture exec(Class<?> clazz, String name, Executor executor, AsyncExec exec) {
    return new AsyncCompletableFuture(
        CompletableFuture.supplyAsync(TracerUtil.traceSupplier(clazz, name, exec), executor));
  }

  public void get() throws IOException {
    try {
      _future.get();
    } catch (ExecutionException e) {
      Throwable throwable = e.getCause();
      if (throwable instanceof AsyncExecException) {
        AsyncExecException exception = (AsyncExecException) throwable;
        Throwable cause = exception.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          throw new IOException(cause);
        }
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public static AsyncCompletableFuture allOf(AsyncCompletableFuture... futures) {
    CompletableFuture<?>[] cfs = new CompletableFuture<?>[futures.length];
    for (int i = 0; i < futures.length; i++) {
      cfs[i] = futures[i]._future;
    }
    return new AsyncCompletableFuture(CompletableFuture.allOf(cfs));
  }

  public static AsyncCompletableFuture completedFuture() {
    return new AsyncCompletableFuture(CompletableFuture.completedFuture((Void) null));
  }

}
