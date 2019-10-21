package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import swa.spi.Link;

public class AttachedVolumeTable extends VolumeTable {

  private static final String GROW = "Grow";
  private static final String ATTACHED = "attached";
  private static final String ATTACHED_VOLUMES = "Attached Volumes";
  private static final String DETACH_ACTION = "Detach";

  public AttachedVolumeTable(PackVolumeStore volumeStore) {
    super(ATTACHED_VOLUMES, ATTACHED, volumeStore);
  }

  @Override
  protected List<String> getVolumeNames() throws IOException {
    return _volumeStore.getAttachedVolumes();
  }

  @Override
  public List<String> getActions(Map<String, String[]> queryParams) throws IOException {
    return Arrays.asList(DETACH_ACTION, GROW);
  }

  @Override
  public Link execute(String action, String[] ids) throws IOException {
    for (String idStr : ids) {
      long id = Long.parseLong(idStr);
      PackVolumeMetadata metadata = _volumeStore.getVolumeMetadata(id);
      switch (action) {
      case DETACH_ACTION:
        _volumeStore.detachVolume(metadata.getName());
        break;
      case GROW:
        return Link.create(GrowVolume.LINK, "?volumename=", metadata.getName());
      default:
        break;
      }
    }
    return Link.create(getLinkName());
  }

}
