package pack.iscsi.server.admin;

import java.io.IOException;

import swa.spi.Html;

public interface PackHtml extends Html {

  @Override
  default String getWindowName() throws IOException {
    return "Pack";
  }

  @Override
  default String getWindowTitle() throws IOException {
    return getWindowName() + " - " + getName();
  }

}
