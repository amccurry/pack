package pack.iscsi.spi.async;

import java.util.function.Supplier;

public interface AsyncExec extends Supplier<Void> {

  default Void get() {
    try {
      exec();
      return null;
    } catch (Throwable t) {
      throw new AsyncExecException(t);
    }
  }

  void exec() throws Exception;

}
