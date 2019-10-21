package pack.iscsi.server.admin;

import java.io.IOException;

import swa.spi.Html;

public interface PackHtml extends Html {

  @Override
  default String getWindowTitle() throws IOException {
    return "Pack - " + getName();
  }

}
