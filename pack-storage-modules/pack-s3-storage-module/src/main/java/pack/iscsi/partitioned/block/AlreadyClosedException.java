package pack.iscsi.partitioned.block;

import java.io.IOException;

public class AlreadyClosedException extends IOException {

  private static final long serialVersionUID = 2696274690658631947L;

  public AlreadyClosedException(String message) {
    super(message);
  }
}
