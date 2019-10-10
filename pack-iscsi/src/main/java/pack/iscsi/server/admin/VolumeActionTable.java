package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import pack.iscsi.admin.ActionTable;
import pack.iscsi.admin.Column;
import pack.iscsi.admin.Row;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;

public abstract class VolumeActionTable implements ActionTable {

  protected final PackVolumeStore _volumeStore;
  protected final String _name;
  protected final String _link;

  public VolumeActionTable(String name, String link, PackVolumeStore volumeStore) {
    _volumeStore = volumeStore;
    _name = name;
    _link = link;
  }

  @Override
  public String getName() throws IOException {
    return _name;
  }

  @Override
  public String getLink() throws IOException {
    return _link;
  }

  @Override
  public List<String> getHeaders() throws IOException {
    return Arrays.asList("Name", "Attached Host", "Read Only", "Length", "Id");
  }

  @Override
  public List<Row> getRows() throws IOException {
    List<String> volumes = getVolumeNames();
    List<Row> rows = new ArrayList<>();
    for (String attachedVolume : volumes) {
      PackVolumeMetadata metadata = _volumeStore.getVolumeMetadata(attachedVolume);
      if (metadata != null) {
        rows.add(Row.builder()
                    .columns(getColumns(metadata))
                    .id(Long.toString(metadata.getVolumeId()))
                    .build());
      }
    }
    return rows;
  }

  protected abstract List<String> getVolumeNames() throws IOException;

  private static List<Column> getColumns(PackVolumeMetadata metadata) {
    List<Column> columns = new ArrayList<>();
    columns.add(Column.builder()
                      .value(metadata.getName())
                      .build());

    columns.add(Column.builder()
                      .value(toString(metadata.getAttachedHostnames()))
                      .build());

    columns.add(Column.builder()
                      .value(Boolean.toString(metadata.isReadOnly()))
                      .build());

    columns.add(Column.builder()
                      .value(Long.toString(metadata.getLengthInBytes()))
                      .build());

    columns.add(Column.builder()
                      .value(Long.toString(metadata.getVolumeId()))
                      .build());
    return columns;
  }

  private static String toString(List<String> attachedHostnames) {
    if (attachedHostnames == null) {
      return "";
    }
    return attachedHostnames.toString();
  }
}
