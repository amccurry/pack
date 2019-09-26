package pack.iscsi.spi.metric;

import java.io.Closeable;

public interface TimerContext  {
  
  Closeable time();
  
}
