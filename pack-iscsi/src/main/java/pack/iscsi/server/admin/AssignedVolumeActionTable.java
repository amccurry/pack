package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;

public class AssignedVolumeActionTable extends VolumeActionTable {

  private static final String ASSIGNED = "assigned";
  private static final String ASSIGNED_VOLUMES = "Assigned Volumes";
  private static final String UNASSIGN = "Unassign";

  public AssignedVolumeActionTable(PackVolumeStore volumeStore) {
    super(ASSIGNED_VOLUMES, ASSIGNED, volumeStore);
  }

  @Override
  protected List<String> getVolumeNames() throws IOException {
    return _volumeStore.getAssignedVolumes();
  }

  @Override
  public List<String> getActions() throws IOException {
    return Arrays.asList(UNASSIGN);
  }

  @Override
  public void execute(String action, String[] ids) throws IOException {
    for (String idStr : ids) {
      long id = Long.parseLong(idStr);
      PackVolumeMetadata metadata = _volumeStore.getVolumeMetadata(id);
      switch (action) {
      case UNASSIGN:
        _volumeStore.unassignVolume(metadata.getName());
        break;
      default:
        break;
      }
    }
  }

}
