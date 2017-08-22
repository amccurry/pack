package pack;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.json.CreateRequest;
import pack.json.Err;
import pack.json.GetRequest;
import pack.json.GetResponse;
import pack.json.Implements;
import pack.json.ListResponse;
import pack.json.MountUnmountRequest;
import pack.json.PathRequest;
import pack.json.PathResponse;
import pack.json.RemoveRequest;
import pack.json.Volume;
import spark.Request;
import spark.Response;
import spark.ResponseTransformer;
import spark.Route;
import spark.Service;
import spark.SparkJava;
import spark.SparkJavaIdentifier;

public abstract class PackServer {

  private static final Logger LOG = LoggerFactory.getLogger(PackServer.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final boolean global;
  private final String sockFile;

  public PackServer(boolean global, String sockFile) {
    this.global = global;
    this.sockFile = sockFile;
  }

  protected abstract PackStorage getPackStorage() throws Exception;

  public void runServer() throws Exception {
    PackStorage packStorage = getPackStorage();
    
    SparkJava.init();
    Service service = Service.ignite();
    SparkJava.configureService(SparkJavaIdentifier.UNIX_SOCKET, service);
    File file = new File(sockFile);
    if (file.exists()) {
      file.delete();
    }
    service.ipAddress(sockFile);

    ResponseTransformer trans = model -> {
      if (model instanceof String) {
        return (String) model;
      } else {
        return objectMapper.writeValueAsString(model);
      }
    };

    Implements impls = Implements.builder()
                                 .impls(Arrays.asList("VolumeDriver"))
                                 .build();
    service.post("/VolumeDriver.Capabilities",
        (request, response) -> "{ \"Capabilities\": { \"Scope\": \"" + (global ? "global" : "local") + "\" } }");

    service.post("/Plugin.Activate", (request, response) -> impls, trans);

    service.post("/VolumeDriver.Create", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        CreateRequest createRequest = read(request, CreateRequest.class);
        try {
          packStorage.create(createRequest.getVolumeName(), createRequest.getOptions());
          return Err.builder()
                    .build();
        } catch (Throwable t) {
          LOG.error("error", t);
          return Err.builder()
                    .error(t.getMessage())
                    .build();
        }
      }
    }, trans);

    service.post("/VolumeDriver.Remove", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        RemoveRequest removeRequest = read(request, RemoveRequest.class);
        try {
          packStorage.remove(removeRequest.getVolumeName());
          return Err.builder()
                    .build();
        } catch (Throwable t) {
          LOG.error("error", t);
          return Err.builder()
                    .error(t.getMessage())
                    .build();
        }
      }
    }, trans);

    service.post("/VolumeDriver.Mount", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        MountUnmountRequest mountUnmountRequest = read(request, MountUnmountRequest.class);
        try {
          String mountPoint = packStorage.mount(mountUnmountRequest.getVolumeName(), mountUnmountRequest.getId());
          return PathResponse.builder()
                             .mountpoint(mountPoint)
                             .build();
        } catch (Throwable t) {
          LOG.error("error", t);
          return PathResponse.builder()
                             .mountpoint("<unknown>")
                             .error(t.getMessage())
                             .build();
        }
      }
    }, trans);

    service.post("/VolumeDriver.Path", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        PathRequest pathRequest = read(request, PathRequest.class);
        try {
          String mountPoint = packStorage.getMountPoint(pathRequest.getVolumeName());
          return PathResponse.builder()
                             .mountpoint(mountPoint)
                             .build();
        } catch (Throwable t) {
          LOG.error("error", t);
          return PathResponse.builder()
                             .error(t.getMessage())
                             .build();
        }
      }
    }, trans);

    service.post("/VolumeDriver.Unmount", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        MountUnmountRequest mountUnmountRequest = read(request, MountUnmountRequest.class);
        try {
          packStorage.unmount(mountUnmountRequest.getVolumeName(), mountUnmountRequest.getId());
          return Err.builder()
                    .build();
        } catch (Throwable t) {
          LOG.error("error", t);
          return Err.builder()
                    .error(t.getMessage())
                    .build();
        }
      }
    }, trans);

    service.post("/VolumeDriver.Get", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        GetRequest getRequest = read(request, GetRequest.class);
        try {
          if (packStorage.exists(getRequest.getVolumeName())) {
            String mountPoint = packStorage.getMountPoint(getRequest.getVolumeName());
            Volume volume = Volume.builder()
                                  .volumeName(getRequest.getVolumeName())
                                  .mountpoint(mountPoint)
                                  .build();
            return GetResponse.builder()
                              .volume(volume)
                              .build();
          }
          return GetResponse.builder()
                            .build();
        } catch (Throwable t) {
          LOG.error("error", t);
          return GetResponse.builder()
                            .error(t.getMessage())
                            .build();
        }
      }
    }, trans);

    service.post("/VolumeDriver.List", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        try {
          List<String> volumeNames = packStorage.listVolumes();
          Builder<Volume> volumes = ImmutableList.builder();
          for (String volumeName : volumeNames) {
            String mountPoint = packStorage.getMountPoint(volumeName);
            Volume volume = Volume.builder()
                                  .volumeName(volumeName)
                                  .mountpoint(mountPoint)
                                  .build();
            volumes.add(volume);
          }
          return ListResponse.builder()
                             .volumes(volumes.build())
                             .build();
        } catch (Throwable t) {
          LOG.error("error", t);
          return ListResponse.builder()
                             .error(t.getMessage())
                             .build();
        }
      }
    }, trans);
  }

  private static <T> T read(Request request, Class<T> clazz) throws IOException {
    return objectMapper.readValue(request.bodyAsBytes(), clazz);
  }

  public static Result exec(List<String> command) throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder(command);
    Process process = builder.start();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
    Thread t1 = captureOutput(process.getInputStream(), outputStream);
    Thread t2 = captureOutput(process.getErrorStream(), errorStream);
    t1.start();
    t2.start();
    int exitCode = process.waitFor();
    t1.join();
    t2.join();
    return new Result(exitCode, new ByteArrayInputStream(outputStream.toByteArray()),
        new ByteArrayInputStream(errorStream.toByteArray()));
  }

  public static class Result {
    public final int exitCode;
    public final InputStream output;
    public final InputStream error;

    public Result(int exitCode, InputStream output, InputStream error) {
      this.exitCode = exitCode;
      this.output = output;
      this.error = error;
    }
  }

  private static Thread captureOutput(InputStream inputStream, OutputStream outputStream) {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          IOUtils.copy(inputStream, outputStream);
        } catch (IOException e) {
          LOG.error("Unknown error", e);
        } finally {
          IOUtils.closeQuietly(inputStream);
          IOUtils.closeQuietly(outputStream);
        }
      }
    });
  }
}
