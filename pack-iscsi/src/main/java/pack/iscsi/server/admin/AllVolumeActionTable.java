package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;

public class AllVolumeActionTable extends VolumeActionTable {

  private static final String ALL = "all";
  private static final String ALL_VOLUMES = "All Volumes";
  private static final String DELETE = "Delete";
  private static final String ATTACH = "Attach";

  public AllVolumeActionTable(PackVolumeStore volumeStore) {
    super(ALL_VOLUMES, ALL, volumeStore);
  }

  @Override
  protected List<String> getVolumeNames() throws IOException {
    return _volumeStore.getAllVolumes();
  }

  @Override
  public List<String> getActions(Map<String, String[]> queryParams) throws IOException {
    return Arrays.asList(ATTACH, DELETE);
  }

  @Override
  public String execute(String action, String[] ids) throws IOException {
    for (String idStr : ids) {
      long id = Long.parseLong(idStr);
      PackVolumeMetadata metadata = _volumeStore.getVolumeMetadata(id);
      switch (action) {
      case ATTACH:
        _volumeStore.attachVolume(metadata.getName());
        break;
      case DELETE:
        _volumeStore.deleteVolume(metadata.getName());
        break;
      default:
        break;
      }
    }
    return getLink();
  }

}