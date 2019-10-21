package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import pack.iscsi.server.metrics.PackScheduledReporter;
import swa.spi.Row;
import swa.spi.Table;

public class MeterMetricsTable implements PackHtml, Table {

  private final PackScheduledReporter _reporter;

  public MeterMetricsTable(PackScheduledReporter reporter) {
    _reporter = reporter;
  }

  @Override
  public String getName() throws IOException {
    return "Volume Metrics";
  }

  @Override
  public String getLinkName() throws IOException {
    return "metermetrics";
  }

  @Override
  public String getIcon() {
    return "bar-chart-2";
  }

  @Override
  public List<Row> getRows(Map<String, String[]> queryParams) throws IOException {
    return _reporter.getRows();
  }

  @Override
  public List<String> getHeaders(Map<String, String[]> queryParams) throws IOException {
    return Arrays.asList("Name", "Count", "Mean", "1 Minute", "5 Minute", "15 Minute");
  }

}
