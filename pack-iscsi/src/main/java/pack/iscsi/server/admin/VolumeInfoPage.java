package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import swa.spi.ChartDataset;
import swa.spi.ChartElement;
import swa.spi.Page;
import swa.spi.PageAction;
import swa.spi.PageButton;
import swa.spi.PageElement;
import swa.spi.PageSection;

public class VolumeInfoPage implements PackHtml, Page {

  private final PackVolumeStore _packVolumeStore;

  public VolumeInfoPage(PackVolumeStore packVolumeStore) {
    _packVolumeStore = packVolumeStore;
  }

  @Override
  public String getIcon() {
    return "database";
  }

  @Override
  public String getName() throws IOException {
    return "Volume Info";
  }

  @Override
  public String getLinkName() throws IOException {
    return "volume";
  }

  @Override
  public List<PageSection> getPagesSections(Map<String, String[]> queryParams, String[] splat) throws Exception {
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

    PageButton growButton = PageButton.builder()
                                      .link("/growvolume?volumename=" + name)
                                      .name("Grow")
                                      .build();

    List<PageAction> pageActions = Arrays.asList(growButton);

    ChartElement bytes = getBytesChart();
    List<ChartElement> chartElements = Arrays.asList(bytes);
    return Arrays.asList(PageSection.builder()
                                    .pageElements(builder.build())
                                    .pageActions(pageActions)
                                    .chartElements(chartElements)
                                    .build());
  }

  private ChartElement getBytesChart() {
    
    //mock up for now

    List<String> chartLabels = Arrays.asList("4:00", "4:01", "4:02", "4:03", "4:04", "4:05", "4:06", "4:07", "4:08",
        "4:09", "4:10", "4:11", "4:12", "4:13", "4:14", "4:15", "4:16", "4:17", "4:18", "4:19", "4:20", "4:21", "4:22",
        "4:23", "4:24", "4:25", "4:26", "4:27", "4:28", "4:29");

    ChartDataset written;
    ChartDataset read;
    {
      List<Number> values = new ArrayList<>();
      Random random = new Random();
      for (int i = 0; i < chartLabels.size(); i++) {
        values.add((long) random.nextInt(40_000_000) + 80_000_000);
      }

      written = ChartDataset.builder()
                            .color("red")
                            .label("Bytes Written")
                            .values(values)
                            .build();
    }
    {
      List<Number> values = new ArrayList<>();
      Random random = new Random();
      for (int i = 0; i < chartLabels.size(); i++) {
        values.add((long) random.nextInt(40_000_000) + 80_000_000);
      }

      read = ChartDataset.builder()
                         .color("blue")
                         .label("Bytes Read")
                         .values(values)
                         .build();
    }

    List<ChartDataset> datasets = Arrays.asList(written, read);
    ChartElement bytes = ChartElement.builder()
                                     .chartLabels(chartLabels)
                                     .datasets(datasets)
                                     .build();
    return bytes;
  }

}
