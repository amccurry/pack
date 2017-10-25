package pack.block.server.admin;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.methods.GetMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.server.admin.client.UnixDomainSocketClient;

public class DockerMonitor extends UnixDomainSocketClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(DockerMonitor.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    DockerMonitor monitor = new DockerMonitor(new File("/var/run/docker.sock"));
    while (true) {
      try {
        System.out.println("======================================");
        int count = monitor.getContainerCount("testingstuff");
        System.out.println(count);
      } catch (Exception e) {
        System.out.println(e.getClass());
      }
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
  }

  public DockerMonitor(File sockFile) {
    super(sockFile);
  }

  public int getContainerCount(String volume) throws IOException {
    LOGGER.debug("getStatus {}", _sockFile);
    String filter = URLEncoder.encode("{\"volume\":[\"" + volume + "\"]}", UTF_8);
    GetMethod get = new GetMethod(HTTP_LOCALHOST + "/containers/json?all=1&filters=" + filter);
    int executeMethod = getClient().executeMethod(get);
    String body = getBodyAsString(get);
    if (executeMethod == 200) {
      List<?> list = OBJECT_MAPPER.readValue(body, List.class);
      return list.size();
    } else {
      throw new IOException(body);
    }
  }

}
