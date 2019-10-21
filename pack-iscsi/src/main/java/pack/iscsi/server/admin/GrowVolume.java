package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import swa.spi.Form;
import swa.spi.FormElement;
import swa.spi.Link;
import swa.spi.ReadOnlyFormElement;
import swa.spi.TextFormElement;

public class GrowVolume implements Form {

  public static final String LINK = "growvolume";
  private static final String GROW = "Grow";
  private static final long GIB = 1024L * 1024L * 1024L;

  private final PackVolumeStore _store;

  public GrowVolume(PackVolumeStore store) {
    _store = store;
  }

  @Override
  public String getIcon() {
    return "chevrons-up";
  }

  @Override
  public String getName() throws IOException {
    return "Grow Volume";
  }

  @Override
  public String getLinkName() throws IOException {
    return LINK;
  }

  @Override
  public List<FormElement> getElements(Map<String, String[]> queryParams, String[] splat) throws Exception {
    Builder<FormElement> builder = ImmutableList.builder();

    String volumeName = getValue("volumename", queryParams, "Volume Name Missing");

    builder.add(ReadOnlyFormElement.builder()
                                   .label("Volume Name")
                                   .name("volumename")
                                   .value(volumeName)
                                   .build());

    PackVolumeMetadata metadata = _store.getVolumeMetadata(volumeName);

    String sizeInGiBs = getSizeInGiBs(metadata.getLengthInBytes());

    builder.add(TextFormElement.builder()
                               .label("Volume Size (Min 10 Max 16000 in GiBs)")
                               .name("volumesize")
                               .defaultValue(sizeInGiBs)
                               .build());

    return builder.build();
  }

  private String getSizeInGiBs(long lengthInBytes) {
    long gibs = lengthInBytes / GIB;
    return Long.toString(gibs);
  }

  @Override
  public Link execute(Map<String, String[]> queryParams) throws Exception {
    String volumeName = getValue("volumename", queryParams, "Volume Name Missing");
    String volumeSizeStr = getValue("volumesize", queryParams, "Volume Size Missing");
    long lengthInBytes = Integer.parseInt(volumeSizeStr.trim()) * GIB;
    _store.growVolume(volumeName, lengthInBytes);
    return Link.create("attached");
  }

  @Override
  public String getSubmitButtonName() {
    return GROW;
  }

  @Override
  public String getSubmitButtonValue() {
    return GROW;
  }

}
