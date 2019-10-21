package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import swa.spi.ChartElement;
import swa.spi.Page;
import swa.spi.PageElement;

public class VolumePage implements PackHtml, Page {

  private final PackVolumeStore _packVolumeStore;

  public VolumePage(PackVolumeStore packVolumeStore) {
    _packVolumeStore = packVolumeStore;
  }

  @Override
  public String getIcon() {
    return "database";
  }

  @Override
  public String getName() throws IOException {
    return "Volume";
  }

  @Override
  public String getLinkName() throws IOException {
    return "volume";
  }

  @Override
  public List<PageElement> getElements(Map<String, String[]> queryParams, String[] splat) throws Exception {
    if (splat.length != 1) {
      throw new IOException("Volume name not specified.");
    }
    String name = splat[0];

    PackVolumeMetadata volumeMetadata = _packVolumeStore.getVolumeMetadata(name);

    Builder<PageElement> builder = ImmutableList.builder();

    builder.add(PageElement.builder()
                           .label("Name")
                           .value(name)
                           .build());

    builder.add(PageElement.builder()
                           .label("Read Only")
                           .value(Boolean.toString(volumeMetadata.isReadOnly()))
                           .build());

    long volumeId = volumeMetadata.getVolumeId();
    builder.add(PageElement.builder()
                           .label("Volume Id")
                           .value(Long.toString(volumeId))
                           .build());

    long lengthInBytes = volumeMetadata.getLengthInBytes();
    builder.add(PageElement.builder()
                           .label("Length")
                           .value(FileUtils.byteCountToDisplaySize(lengthInBytes))
                           .build());

    int blockSizeInBytes = volumeMetadata.getBlockSizeInBytes();
    builder.add(PageElement.builder()
                           .label("BlockSize")
                           .value(FileUtils.byteCountToDisplaySize(blockSizeInBytes))
                           .build());

    List<String> attachedHostnames = volumeMetadata.getAttachedHostnames();
    builder.add(PageElement.builder()
                           .label("Attached Hosts")
                           .value(attachedHostnames == null ? ""
                               : Joiner.on(',')
                                       .join(attachedHostnames))
                           .build());

    Integer readAheadBlockLimit = volumeMetadata.getReadAheadBlockLimit();
    builder.add(PageElement.builder()
                           .label("Readahead Block Limit")
                           .value(readAheadBlockLimit == null ? "" : readAheadBlockLimit.toString())
                           .build());

    Integer readAheadExecutorThreadCount = volumeMetadata.getReadAheadExecutorThreadCount();
    builder.add(PageElement.builder()
                           .label("Readahead Thread Count")
                           .value(readAheadExecutorThreadCount == null ? "" : readAheadExecutorThreadCount.toString())
                           .build());

    Integer syncExecutorThreadCount = volumeMetadata.getSyncExecutorThreadCount();
    builder.add(PageElement.builder()
                           .label("Sync Thread Count")
                           .value(syncExecutorThreadCount == null ? "" : syncExecutorThreadCount.toString())
                           .build());

    Long syncTimeAfterIdle = volumeMetadata.getSyncTimeAfterIdle();
    builder.add(PageElement.builder()
                           .label("Sync Time After Idle")
                           .value(syncTimeAfterIdle == null ? "" : syncTimeAfterIdle.toString())
                           .build());

    TimeUnit syncTimeAfterIdleTimeUnit = volumeMetadata.getSyncTimeAfterIdleTimeUnit();
    builder.add(PageElement.builder()
                           .label("Sync Time After Idle Unit")
                           .value(syncTimeAfterIdleTimeUnit == null ? "" : syncTimeAfterIdleTimeUnit.toString())
                           .build());

    return builder.build();
  }

  @Override
  public List<ChartElement> getCharts(Map<String, String[]> queryParams, String[] splat) throws Exception {
    return Arrays.asList();
  }

}
