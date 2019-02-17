package pack.docker;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.PackStorage;
import pack.docker.json.Capabilities;
import pack.docker.json.CapabilitiesResponse;
import pack.docker.json.CreateRequest;
import pack.docker.json.Err;
import pack.docker.json.GetRequest;
import pack.docker.json.GetResponse;
import pack.docker.json.Implements;
import pack.docker.json.ListResponse;
import pack.docker.json.MountUnmountRequest;
import pack.docker.json.PathRequest;
import pack.docker.json.PathResponse;
import pack.docker.json.RemoveRequest;
import pack.docker.json.Volume;
import spark.Request;
import spark.ResponseTransformer;
import spark.Route;
import spark.Service;
import spark.SparkJava;
import spark.SparkJavaIdentifier;

public abstract class PackServer {

  private static final Logger LOG = LoggerFactory.getLogger(PackServer.class);

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

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final ResponseTransformer TRANSFORMER = model -> {
    LOG.info("response {}", model);
    if (model instanceof String) {
      return (String) model;
    } else {
      return OBJECT_MAPPER.writeValueAsString(model);
    }
  };

  private final boolean global;
  private final String sockFile;

  public PackServer(boolean global, String sockFile) {
    this.global = global;
    this.sockFile = sockFile;
  }

  protected abstract PackStorage getPackStorage(Service service) throws Exception;

  public void runServer() throws Exception {

    SparkJava.init();
    Service service = Service.ignite();
    SparkJava.configureService(SparkJavaIdentifier.UNIX_SOCKET, service);
    File file = new File(sockFile);
    if (file.exists()) {
      file.delete();
    }
    LOG.info("Using sock {}", sockFile);
    service.ipAddress(sockFile);

    PackStorage packStorage = getPackStorage(service);

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

    service.post(PLUGIN_ACTIVATE, (request, response) -> impls, TRANSFORMER);
    service.post(VOLUME_DRIVER_CAPABILITIES, (request, response) -> capabilitiesResponse, TRANSFORMER);
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
    }, TRANSFORMER);

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
    }, TRANSFORMER);

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
    }, TRANSFORMER);

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
    }, TRANSFORMER);

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
    }, TRANSFORMER);

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
    }, TRANSFORMER);

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
    }, TRANSFORMER);
  }

  public static <T> T read(Request request, Class<T> clazz) throws IOException {
    return OBJECT_MAPPER.readValue(request.bodyAsBytes(), clazz);
  }

  public static void debugInfo(Request request) {
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
