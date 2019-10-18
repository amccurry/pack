package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.iscsi.spi.PackVolumeStore;
import swa.spi.CheckBoxFormElement;
import swa.spi.Form;
import swa.spi.FormElement;
import swa.spi.TextFormElement;

public class CreateVolume implements Form {

  private static final String CREATE = "Create";
  private static final long GIB = 1024L * 1024L * 1024L;
  private static final int MIB = 1024 * 1024;

  private final PackVolumeStore _store;

  public CreateVolume(PackVolumeStore store) {
    _store = store;
  }

  @Override
  public String getIcon() {
    return "hard-drive";
  }

  @Override
  public String getName() throws IOException {
    return "Create Volume";
  }

  @Override
  public String getLink() throws IOException {
    return "createvolume";
  }

  @Override
  public List<FormElement> getElements(Map<String, String[]> queryParams) {
    Builder<FormElement> builder = ImmutableList.builder();

    builder.add(TextFormElement.builder()
                               .label("Volume Name")
                               .name("volumename")
                               .build());

    builder.add(TextFormElement.builder()
                               .label("Volume Size (Min 10 Max 16000 in GiBs)")
                               .name("volumesize")
                               .defaultValue("100")
                               .build());

    builder.add(TextFormElement.builder()
                               .label("Block Size (Min 1 Max 128 in MiBs)")
                               .name("blocksize")
                               .defaultValue("16")
                               .build());

    builder.add(CheckBoxFormElement.builder()
                                   .label("Auto Attach To Host")
                                   .name("attach")
                                   .defaultValue(true)
                                   .value("true")
                                   .build());

    return builder.build();
  }

  @Override
  public String execute(Map<String, String[]> queryParams) throws Exception {
    String volumeName = getValue("volumename", queryParams, "Volume Name Missing");
    String volumeSizeStr = getValue("volumesize", queryParams, "Volume Size Missing");
    String blockSizeStr = getValue("blocksize", queryParams, "Block Size Missing");

    boolean attach = hasValue("attach", queryParams);

    long lengthInBytes = Integer.parseInt(volumeSizeStr.trim()) * GIB;
    int blockSizeInBytes = Integer.parseInt(blockSizeStr.trim()) * MIB;
    _store.createVolume(volumeName, lengthInBytes, blockSizeInBytes);
    if (attach) {
      _store.attachVolume(volumeName);
      return "attached";
    }
    return "all";
  }

  @Override
  public String getSubmitButtonName() {
    return CREATE;
  }

  @Override
  public String getSubmitButtonValue() {
    return CREATE;
  }

}
