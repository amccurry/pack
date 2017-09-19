package pack.block.server.admin.client;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import pack.block.server.admin.BlockPackAdminServer;
import pack.block.server.admin.CounterAction;
import pack.block.server.admin.CounterRequest;
import pack.block.server.admin.CounterResponse;
import pack.block.server.admin.PidResponse;
import pack.block.server.admin.Status;
import pack.block.server.admin.StatusResponse;
import spark.Service;
import spark.SparkJava;
import spark.SparkJavaIdentifier;

public class BlockPackAdminClient extends UnixDomainSocketClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackAdminClient.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    File file = new File("test.sock");
    file.delete();
    BlockPackAdminClient client = new BlockPackAdminClient(file);
    printPid(client);
    startTestServer(file);
    for (int i = 0; i < 10; i++) {
      printPid(client);
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
    System.exit(0);
  }

  private static void printPid(BlockPackAdminClient client) throws IOException {
    try {
      System.out.println(client.getPid());
    } catch (NoFileException e) {
      System.out.println("Server doesn't exist");
    } catch (ConnectionRefusedException e) {
      System.out.println("Server offline");
    }
  }

  private final ObjectMapper _mapper = new ObjectMapper();

  public static BlockPackAdminClient create(File sockFile) {
    return new BlockPackAdminClient(sockFile);
  }

  public BlockPackAdminClient(File sockFile) {
    super(sockFile);
  }

  public Status getStatus() throws IOException {
    LOGGER.info("getStatus {}", _sockFile);
    GetMethod get = new GetMethod(HTTP_LOCALHOST + BlockPackAdminServer.STATUS);
    int executeMethod = getClient().executeMethod(get);
    String body = getBodyAsString(get);
    if (executeMethod == 200) {
      StatusResponse statusResponse = _mapper.readValue(body, StatusResponse.class);
      return statusResponse.getStatus();
    } else {
      throw new IOException(body);
    }
  }

  public String getPid() throws IOException {
    LOGGER.info("getPid {}", _sockFile);
    GetMethod get = new GetMethod(HTTP_LOCALHOST + BlockPackAdminServer.PID);
    int executeMethod = getClient().executeMethod(get);
    String body = getBodyAsString(get);
    if (executeMethod == 200) {
      return body;
    } else {
      throw new IOException(body);
    }
  }

  public long incrementCounter(String name) throws IOException {
    LOGGER.info("incrementCounter {} {}", name, _sockFile);
    CounterAction action = CounterAction.INCREMENT;
    long value = 1L;
    return execCounter(name, action, value);
  }

  private long execCounter(String name, CounterAction action, long value)
      throws JsonProcessingException, IOException, HttpException, JsonParseException, JsonMappingException {
    PostMethod post = new PostMethod(HTTP_LOCALHOST + BlockPackAdminServer.COUNTER);
    CounterRequest request = CounterRequest.builder()
                                           .name(name)
                                           .action(action)
                                           .value(value)
                                           .build();
    RequestEntity requestEntity = new ByteArrayRequestEntity(OBJECT_MAPPER.writeValueAsBytes(request));
    post.setRequestEntity(requestEntity);
    int executeMethod = getClient().executeMethod(post);
    String bodyAsString = getBodyAsString(post);
    if (executeMethod != 200) {
      throw new IOException(bodyAsString);
    } else {
      CounterResponse response = OBJECT_MAPPER.readValue(bodyAsString, CounterResponse.class);
      return response.getValue();
    }
  }

  public long decrementCounter(String name) throws IOException {
    LOGGER.info("decrementCounter {} {}", name, _sockFile);
    CounterAction action = CounterAction.DECREMENT;
    long value = -1L;
    return execCounter(name, action, value);
  }

  public void umount() throws IOException {
    LOGGER.info("umount {}", _sockFile);
    PostMethod post = new PostMethod(HTTP_LOCALHOST + BlockPackAdminServer.UMOUNT);
    int executeMethod = getClient().executeMethod(post);
    if (executeMethod != 200) {
      throw new IOException(getBodyAsString(post));
    }
  }

  public void shutdown() throws IOException {
    LOGGER.info("shutdown {}", _sockFile);
    PostMethod post = new PostMethod(HTTP_LOCALHOST + BlockPackAdminServer.SHUTDOWN);
    int executeMethod = getClient().executeMethod(post);
    if (executeMethod != 200) {
      throw new IOException(getBodyAsString(post));
    }
  }

  private static Service startTestServer(File sockFile) {
    SparkJava.init();
    Service service = Service.ignite();
    SparkJava.configureService(SparkJavaIdentifier.UNIX_SOCKET, service);
    String pid = BlockPackAdminServer.getPid();
    service.ipAddress(sockFile.getAbsolutePath());
    service.get(BlockPackAdminServer.PID, (request, response) -> PidResponse.builder()
                                                                      .pid(pid)
                                                                      .build(),
        BlockPackAdminServer.TRANSFORMER);
    return service;
  }

}
