package pack.iscsi.spi;

import java.io.Closeable;

public interface TimerContext  {
  
  Closeable time();
  
}
