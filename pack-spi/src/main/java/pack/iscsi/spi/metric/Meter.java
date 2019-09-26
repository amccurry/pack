package pack.iscsi.spi.metric;

public interface Meter {

  default void mark() {
    mark(1);
  }

  void mark(int count);

}
