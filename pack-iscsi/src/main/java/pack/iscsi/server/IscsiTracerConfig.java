package pack.iscsi.server;

import java.io.IOException;
import java.net.InetAddress;

import io.opencensus.exporter.trace.jaeger.JaegerExporterConfiguration;
import io.opencensus.exporter.trace.jaeger.JaegerTraceExporter;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.config.TraceParams;
import io.opencensus.trace.samplers.Samplers;

public class IscsiTracerConfig {

  public static void setupTracer(String thriftEndpoint) throws IOException {
    if (thriftEndpoint == null) {
      return;
    }
    String serviceName = "iscsi-" + InetAddress.getLocalHost()
                                               .getHostName();
    JaegerExporterConfiguration configuration = JaegerExporterConfiguration.builder()
                                                                           .setThriftEndpoint(thriftEndpoint)
                                                                           .setServiceName(serviceName)
                                                                           .build();
    JaegerTraceExporter.createAndRegister(configuration);

    TraceConfig traceConfig = Tracing.getTraceConfig();
    TraceParams activeTraceParams = traceConfig.getActiveTraceParams();
    traceConfig.updateActiveTraceParams(activeTraceParams.toBuilder()
                                                         .setSampler(Samplers.alwaysSample())
                                                         .build());
  }

}
