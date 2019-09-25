package pack.iscsi.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface ActionTable {

  default String getName() throws IOException {
    return getLink();
  }

  String getLink() throws IOException;

  List<String> getHeaders() throws IOException;

  List<Row> getRows() throws IOException;

  default List<String> getActions() throws IOException {
    return new ArrayList<>();
  }

  default void execute(String action, String[] ids) throws IOException {

  }

}
