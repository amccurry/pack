package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.ArrayList;
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
import swa.spi.Column;
import swa.spi.Page;
import swa.spi.PageAction;
import swa.spi.PageButton;
import swa.spi.PageElement;
import swa.spi.PageSection;
import swa.spi.Row;
import swa.spi.TableElement;

public class VolumeInfoPage implements PackHtml, Page {

  private final PackVolumeStore _packVolumeStore;
  private final MetricsTable _metricsTable;

  public VolumeInfoPage(PackVolumeStore packVolumeStore, MetricsTable metricsTable) {
    _packVolumeStore = packVolumeStore;
    _metricsTable = metricsTable;
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

    List<PageElement> pageElements = getPageElements(name, volumeMetadata);

    PageButton growButton = PageButton.builder()
                                      .link("/growvolume?volumename=" + name)
                                      .name("Grow Size")
                                      .build();

    List<PageAction> pageActions = Arrays.asList(growButton);

    List<ChartElement> chartElements = _metricsTable.getChartElements(name);

    List<TableElement> tableElements = Arrays.asList(getSnapshotTable(name));
    PageSection section = PageSection.builder()
                                     .pageElements(pageElements)
                                     .pageActions(pageActions)
                                     .chartElements(chartElements)
                                     .tableElements(tableElements)
                                     .build();
    return Arrays.asList(section);
  }

  private TableElement getSnapshotTable(String name) throws IOException {
    List<String> headers = Arrays.asList("Snapshot");
    List<Row> rows = new ArrayList<>();
    List<String> listSnapshots = _packVolumeStore.listSnapshots(name);

    for (String snapshot : listSnapshots) {
      rows.add(Row.builder()
                  .columns(Arrays.asList(Column.builder()
                                               .value(snapshot)
                                               .build()))
                  .build());
    }

    return TableElement.builder()
                       .headers(headers)
                       .rows(rows)
                       .build();
  }

  private List<PageElement> getPageElements(String name, PackVolumeMetadata volumeMetadata) {
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

    List<PageElement> pageElements = builder.build();
    return pageElements;
  }

}
