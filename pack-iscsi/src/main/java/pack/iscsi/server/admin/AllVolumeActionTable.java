package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;

public class AllVolumeActionTable extends VolumeActionTable {

  private static final String ALL = "all";
  private static final String ALL_VOLUMES = "All Volumes";
  private static final String DELETE = "Delete";
  private static final String ASSIGN = "Assign";

  public AllVolumeActionTable(PackVolumeStore volumeStore) {
    super(ALL_VOLUMES, ALL, volumeStore);
  }

  @Override
  protected List<String> getVolumeNames() throws IOException {
    return _volumeStore.getAllVolumes();
  }

  @Override
  public List<String> getActions() throws IOException {
    return Arrays.asList(ASSIGN, DELETE);
  }

  @Override
  public void execute(String action, String[] ids) throws IOException {
    for (String idStr : ids) {
      long id = Long.parseLong(idStr);
      PackVolumeMetadata metadata = _volumeStore.getVolumeMetadata(id);
      switch (action) {
      case ASSIGN:
        _volumeStore.assignVolume(metadata.getName());
        break;
      case DELETE:
        _volumeStore.deleteVolume(metadata.getName());
        break;
      default:
        break;
      }
    }
  }

}