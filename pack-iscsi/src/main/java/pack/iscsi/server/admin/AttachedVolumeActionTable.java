package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;

public class AttachedVolumeActionTable extends VolumeActionTable {

  private static final String ATTACHED = "attached";
  private static final String ATTACHED_VOLUMES = "Attached Volumes";
  private static final String DETACH_ACTION = "Detach";

  public AttachedVolumeActionTable(PackVolumeStore volumeStore) {
    super(ATTACHED_VOLUMES, ATTACHED, volumeStore);
  }

  @Override
  protected List<String> getVolumeNames() throws IOException {
    return _volumeStore.getAttachedVolumes();
  }

  @Override
  public List<String> getActions() throws IOException {
    return Arrays.asList(DETACH_ACTION);
  }

  @Override
  public void execute(String action, String[] ids) throws IOException {
    for (String idStr : ids) {
      long id = Long.parseLong(idStr);
      PackVolumeMetadata metadata = _volumeStore.getVolumeMetadata(id);
      switch (action) {
      case DETACH_ACTION:
        _volumeStore.detachVolume(metadata.getName());
        break;
      default:
        break;
      }
    }
  }

}
