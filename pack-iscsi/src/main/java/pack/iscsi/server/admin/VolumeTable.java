package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import swa.spi.Column;
import swa.spi.Link;
import swa.spi.Row;
import swa.spi.Table;

public abstract class VolumeTable implements PackHtml, Table {

  protected final PackVolumeStore _volumeStore;
  protected final String _name;
  protected final String _link;

  public VolumeTable(String name, String link, PackVolumeStore volumeStore) {
    _volumeStore = volumeStore;
    _name = name;
    _link = link;
  }

  @Override
  public String getName() throws IOException {
    return _name;
  }

  @Override
  public String getLinkName() throws IOException {
    return _link;
  }

  @Override
  public String getIcon() {
    return "layers";
  }

  @Override
  public List<String> getHeaders(Map<String, String[]> queryParams) throws IOException {
    return Arrays.asList("Name", "Attached Host", "Read Only", "Length", "Id");
  }

  @Override
  public List<Row> getRows(Map<String, String[]> queryParams) throws IOException {
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
                      .link(Link.create("volume/", metadata.getName()))
                      .build());

    columns.add(Column.builder()
                      .value(toString(metadata.getAttachedHostnames()))
                      .build());

    columns.add(Column.builder()
                      .value(Boolean.toString(metadata.isReadOnly()))
                      .build());

    columns.add(Column.builder()
                      .value(humanSize(metadata.getLengthInBytes()))
                      .build());

    columns.add(Column.builder()
                      .value(Long.toString(metadata.getVolumeId()))
                      .build());
    return columns;
  }

  private static String humanSize(long lengthInBytes) {
    return FileUtils.byteCountToDisplaySize(lengthInBytes);
  }

  private static String toString(List<String> attachedHostnames) {
    if (attachedHostnames == null) {
      return "";
    }
    if (attachedHostnames.size() == 1) {
      return attachedHostnames.get(0);
    } else {
      return attachedHostnames.get(0) + "(plus " + (attachedHostnames.size() - 1) + " more)";

    }
  }
}
