package pack;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.json.Capabilities;
import pack.json.CapabilitiesResponse;
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
import spark.ResponseTransformer;
import spark.Route;
import spark.Service;
import spark.SparkJava;
import spark.SparkJavaIdentifier;

public abstract class PackServer {

  private static final String GLOBAL = "global";
  private static final String LOCAL = "local";
  private static final String VOLUME_DRIVER_CAPABILITIES = "/VolumeDriver.Capabilities";
  private static final String PLUGIN_ACTIVATE = "/Plugin.Activate";
  private static final String VOLUME_DRIVER = "VolumeDriver";
  private static final String VOLUME_DRIVER_CREATE = "/VolumeDriver.Create";
  private static final String VOLUME_DRIVER_REMOVE = "/VolumeDriver.Remove";
  private static final String VOLUME_DRIVER_MOUNT = "/VolumeDriver.Mount";
  private static final String VOLUME_DRIVER_PATH = "/VolumeDriver.Path";
  private static final String VOLUME_DRIVER_UNMOUNT = "/VolumeDriver.Unmount";
  private static final String VOLUME_DRIVER_GET = "/VolumeDriver.Get";
  private static final String VOLUME_DRIVER_LIST = "/VolumeDriver.List";

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
      LOG.info("response {}", model);
      if (model instanceof String) {
        return (String) model;
      } else {
        return objectMapper.writeValueAsString(model);
      }
    };

    Implements impls = Implements.builder()
                                 .impls(Arrays.asList(VOLUME_DRIVER))
                                 .build();

    String scope = global ? GLOBAL : LOCAL;
    Capabilities capabilities = Capabilities.builder()
                                            .scope(scope)
                                            .build();
    CapabilitiesResponse capabilitiesResponse = CapabilitiesResponse.builder()
                                                                    .capabilities(capabilities)
                                                                    .build();

    service.post(VOLUME_DRIVER_CAPABILITIES, (request, response) -> capabilitiesResponse);
    service.post(PLUGIN_ACTIVATE, (request, response) -> impls, trans);
    service.post(VOLUME_DRIVER_CREATE, (Route) (request, response) -> {
      debugInfo(request);
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
    }, trans);

    service.post(VOLUME_DRIVER_REMOVE, (Route) (request, response) -> {
      debugInfo(request);
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
    }, trans);

    service.post(VOLUME_DRIVER_MOUNT, (Route) (request, response) -> {
      debugInfo(request);
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
    }, trans);

    service.post(VOLUME_DRIVER_PATH, (Route) (request, response) -> {
      debugInfo(request);
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
    }, trans);

    service.post(VOLUME_DRIVER_UNMOUNT, (Route) (request, response) -> {
      debugInfo(request);
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
    }, trans);

    service.post(VOLUME_DRIVER_GET, (Route) (request, response) -> {
      debugInfo(request);
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
    }, trans);

    service.post(VOLUME_DRIVER_LIST, (Route) (request, response) -> {
      debugInfo(request);
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
    }, trans);
  }

  private static <T> T read(Request request, Class<T> clazz) throws IOException {
    return objectMapper.readValue(request.bodyAsBytes(), clazz);
  }

  public static Result exec(String cmdId, List<String> command, Logger logger)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder(command);
    Process process = builder.start();
    StringWriter stdout = new StringWriter();
    StringWriter stderr = new StringWriter();
    Thread t1 = captureOutput(cmdId, "stdout", toBuffer(process.getInputStream()), logger, stdout);
    Thread t2 = captureOutput(cmdId, "stderr", toBuffer(process.getErrorStream()), logger, stderr);
    t1.start();
    t2.start();
    int exitCode = process.waitFor();
    t1.join();
    t2.join();
    return new Result(exitCode, stdout.toString(), stderr.toString());
  }

  private static BufferedReader toBuffer(InputStream inputStream) {
    return new BufferedReader(new InputStreamReader(inputStream));
  }

  public static class Result {
    public final int exitCode;
    public final String stdout;
    public final String stderr;

    public Result(int exitCode, String stdout, String stderr) {
      this.exitCode = exitCode;
      this.stdout = stdout;
      this.stderr = stderr;
    }
  }

  private static Thread captureOutput(String cmdId, String type, BufferedReader reader, Logger logger, Writer writer) {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          String s;
          while ((s = reader.readLine()) != null) {
            logger.info("Command {} Type {} Message {}", cmdId, type, s.trim());
            writer.write(s);
          }
        } catch (IOException e) {
          LOG.error("Unknown error", e);
        } finally {
          try {
            writer.close();
          } catch (IOException e) {
            LOG.error("Error trying to close output writer", e);
          }
        }
      }
    });
  }

  private void debugInfo(Request request) {
    LOG.info("pathInfo {} contextPath {}", request.pathInfo(), request.contextPath());
    Set<String> attributes = new TreeSet<>(request.attributes());
    for (String attribute : attributes) {
      LOG.info("attribute {} {}", attribute, request.attribute(attribute));
    }
    Set<String> headers = new TreeSet<>(request.headers());
    for (String header : headers) {
      LOG.info("header {} {}", header, request.headers(header));
    }
    LOG.info("params {}", request.params());
  }
}
