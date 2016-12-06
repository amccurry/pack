package pack;

import static spark.Spark.port;
import static spark.Spark.post;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

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

public class Pack {

  private static final String LOOP_BACK_ADDR = "127.23.24.25";
  private static final String LOOP_BACK_ADDR_CIDR = LOOP_BACK_ADDR + "/32";
  private static final int PORT = 8732;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    // try (PrintWriter printWriter = new PrintWriter(new
    // FileOutputStream("/etc/docker/plugins/pack.json"))) {
    // printWriter.println("{\"Name\": \"pack\",\"Addr\": \"http://" +
    // LOOP_BACK_ADDR + ":" + PORT + "\"}");
    // }

    // ipAddress(LOOP_BACK_ADDR);

    PackStorage packStorage = getPackStorage();

    port(PORT);

    ResponseTransformer trans = model -> {
      if (model instanceof String) {
        return (String) model;
      } else {
        return objectMapper.writeValueAsString(model);
      }
    };

    // post("/VolumeDriver.Capabilities", (request, response) -> "{
    // \"Capabilities\": { \"Scope\": \"global\" } }");
    post("/VolumeDriver.Capabilities", (request, response) -> "{ \"Capabilities\": { \"Scope\": \"local\" } }");
    post("/Plugin.Activate", (request, response) -> Implements.builder().impls(Arrays.asList("VolumeDriver")).build(),
        trans);

    post("/VolumeDriver.Create", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        CreateRequest createRequest = read(request, CreateRequest.class);
        try {
          packStorage.create(createRequest.getVolumeName(), createRequest.getOptions());
          return Err.builder().build();
        } catch (Throwable t) {
          return Err.builder().error(t.getMessage()).build();
        }
      }
    }, trans);

    post("/VolumeDriver.Remove", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        RemoveRequest removeRequest = read(request, RemoveRequest.class);
        try {
          packStorage.remove(removeRequest.getVolumeName());
          return Err.builder().build();
        } catch (Throwable t) {
          return Err.builder().error(t.getMessage()).build();
        }
      }
    }, trans);

    post("/VolumeDriver.Mount", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        MountUnmountRequest mountUnmountRequest = read(request, MountUnmountRequest.class);
        try {
          String mountPoint = packStorage.mount(mountUnmountRequest.getVolumeName(), mountUnmountRequest.getId());
          return PathResponse.builder().mountpoint(mountPoint).build();
        } catch (Throwable t) {
          return PathResponse.builder().mountpoint("<unknown>").error(t.getMessage()).build();
        }
      }
    }, trans);

    post("/VolumeDriver.Path", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        PathRequest pathRequest = read(request, PathRequest.class);
        try {
          String mountPoint = packStorage.getMountPoint(pathRequest.getVolumeName());
          return PathResponse.builder().mountpoint(mountPoint).build();
        } catch (Throwable t) {
          return PathResponse.builder().error(t.getMessage()).build();
        }
      }
    }, trans);

    post("/VolumeDriver.Unmount", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        MountUnmountRequest mountUnmountRequest = read(request, MountUnmountRequest.class);
        try {
          packStorage.unmount(mountUnmountRequest.getVolumeName(), mountUnmountRequest.getId());
          return Err.builder().build();
        } catch (Throwable t) {
          return Err.builder().error(t.getMessage()).build();
        }
      }
    }, trans);

    post("/VolumeDriver.Get", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        GetRequest getRequest = read(request, GetRequest.class);
        try {
          String mountPoint = packStorage.getMountPoint(getRequest.getVolumeName());
          Volume volume = Volume.builder().mountpoint(mountPoint).build();
          return GetResponse.builder().volume(volume).build();
        } catch (Throwable t) {
          return GetResponse.builder().error(t.getMessage()).build();
        }
      }
    }, trans);

    post("/VolumeDriver.List", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        try {
          List<String> volumeNames = packStorage.listVolumes();
          Builder<Volume> volumes = ImmutableList.builder();
          for (String volumeName : volumeNames) {
            String mountPoint = packStorage.getMountPoint(volumeName);
            Volume volume = Volume.builder().mountpoint(mountPoint).build();
            volumes.add(volume);
          }
          return ListResponse.builder().volumes(volumes.build()).build();
        } catch (Throwable t) {
          return ListResponse.builder().error(t.getMessage()).build();
        }
      }
    }, trans);
  }

  private static PackStorage getPackStorage() throws Exception {
    File localFile = new File("/volume-mounts");
    Configuration configuration = new Configuration();
    Path remotePath = new Path("file:///volume-data");
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return new TarPackStorage(localFile, configuration, remotePath, ugi);
  }

  private static <T> T read(Request request, Class<T> clazz) throws IOException {
    return objectMapper.readValue(request.bodyAsBytes(), clazz);
  }
}
