package pack.admin;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.PackVolumeMetadata;
import spark.ResponseTransformer;
import spark.Route;
import spark.Service;

public class PackVolumeAdminServer {

  private static final String VOLUME_NAME_PARAM = ":volumeName";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static void main(String[] args) throws UnknownHostException {
    Service service = Service.ignite();

    String hostname = InetAddress.getLocalHost()
                                 .getHostName();

    Map<String, PackVolumeMetadata> allVolumes = new ConcurrentHashMap<>();
    List<String> assignedVolumes = new ArrayList<String>();

    PackVolumeStore packAdmin = new PackVolumeStore() {

      @Override
      public List<String> getAllVolumes() throws IOException {
        Set<String> set = allVolumes.keySet();
        List<String> list = new ArrayList<>(set);
        Collections.sort(list);
        return list;
      }

      @Override
      public List<String> getAssignedVolumes() throws IOException {
        return assignedVolumes;
      }

      @Override
      public void createVolume(String name, long lengthInBytes, int blockSizeInBytes) throws IOException {
        checkNonExistence(name);
        PackVolumeMetadata metadata = PackVolumeMetadata.builder()
                                                        .blockSizeInBytes(blockSizeInBytes)
                                                        .lengthInBytes(lengthInBytes)
                                                        .name(name)
                                                        .volumeId(new Random().nextLong())
                                                        .build();
        allVolumes.put(name, metadata);
      }

      @Override
      public void deleteVolume(String name) throws IOException {
        checkExistence(name);
        checkNotAssigned(name);
        allVolumes.remove(name);
      }

      @Override
      public void growVolume(String name, long newLengthInBytes) throws IOException {
        checkExistence(name);
        checkNewSizeInBytes(name, newLengthInBytes);
        PackVolumeMetadata metadata = getVolumeMetadata(name);
        allVolumes.put(name, metadata.toBuilder()
                                     .lengthInBytes(newLengthInBytes)
                                     .build());
      }

      @Override
      public void assignVolume(String name) throws IOException {
        checkExistence(name);
        checkNotAssigned(name);
        assignedVolumes.add(name);
        PackVolumeMetadata metadata = getVolumeMetadata(name);
        allVolumes.put(name, metadata.toBuilder()
                                     .assignedHostname(hostname)
                                     .build());
      }

      @Override
      public void unassignVolume(String name) throws IOException {
        checkExistence(name);
        checkAssigned(name);
        assignedVolumes.remove(name);
      }

      @Override
      public PackVolumeMetadata getVolumeMetadata(String name) throws IOException {
        checkExistence(name);
        return allVolumes.get(name);
      }

      @Override
      public PackVolumeMetadata getVolumeMetadata(long volumeId) throws IOException {
        for (PackVolumeMetadata metadata : allVolumes.values()) {
          if (volumeId == metadata.getVolumeId()) {
            return metadata;
          }
        }
        throw new IOException("Volume id " + volumeId + " does not exist");
      }

      @Override
      public void renameVolume(String name, String newName) throws IOException {
        checkExistence(name);
        checkNonExistence(newName);
        checkNotAssigned(name);
        PackVolumeMetadata metadata = allVolumes.remove(name);
        allVolumes.put(newName, metadata.toBuilder()
                                        .name(newName)
                                        .build());
      }

    };
    PackVolumeAdminServer server = new PackVolumeAdminServer(service, packAdmin);
    server.setup();
  }

  private final Service _service;
  private final PackVolumeStore _packAdmin;

  public PackVolumeAdminServer(Service service, PackVolumeStore packAdmin) {
    _service = service;
    _packAdmin = packAdmin;
  }

  public void setup() {
    ResponseTransformer transformer = model -> OBJECT_MAPPER.writeValueAsString(model);
    _service.get("/all-volumes", getAllVolumes(), transformer);
    _service.get("/assigned-volumes", getAssignedVolumes(), transformer);
    _service.post("/create/" + VOLUME_NAME_PARAM, createVolume(), transformer);
    _service.post("/grow/" + VOLUME_NAME_PARAM, growVolume(), transformer);
    _service.post("/delete/" + VOLUME_NAME_PARAM, deleteVolume(), transformer);
    _service.post("/assign/" + VOLUME_NAME_PARAM, assignVolume(), transformer);
    _service.post("/unassign/" + VOLUME_NAME_PARAM, unassignVolume(), transformer);
    _service.get("/volume/" + VOLUME_NAME_PARAM, getVolumeInfo(), transformer);
    _service.exception(Exception.class, (exception, request, response) -> {
      ErrorResponse errorResponse = ErrorResponse.builder()
                                                 .error(exception.getMessage())
                                                 .build();
      try {
        response.status(500);
        response.body(OBJECT_MAPPER.writeValueAsString(errorResponse));
      } catch (JsonProcessingException e) {
        response.body(errorResponse.getError());
      }
    });
  }

  private Route growVolume() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      GrowVolumeRequest growVolumeRequest = OBJECT_MAPPER.readValue(request.body(), GrowVolumeRequest.class);
      _packAdmin.growVolume(volumeName, growVolumeRequest.getSizeInBytes());
      return OKResponse.builder()
                       .message("Volume " + volumeName + " was grown to " + growVolumeRequest.getSizeInBytes())
                       .build();
    };
  }

  private Route getVolumeInfo() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      PackVolumeMetadata volumeMetadata = _packAdmin.getVolumeMetadata(volumeName);
      return volumeMetadata;
    };
  }

  private Route unassignVolume() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      _packAdmin.unassignVolume(volumeName);
      return OKResponse.builder()
                       .message("Volume " + volumeName + " unassigned")
                       .build();
    };
  }

  private Route assignVolume() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      _packAdmin.assignVolume(volumeName);
      return OKResponse.builder()
                       .message("Volume " + volumeName + " assigned")
                       .build();
    };
  }

  private Route deleteVolume() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      _packAdmin.deleteVolume(volumeName);
      return OKResponse.builder()
                       .message("Volume " + volumeName + " deleted")
                       .build();
    };
  }

  private Route createVolume() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      CreateVolumeRequest createVolumeRequest = OBJECT_MAPPER.readValue(request.body(), CreateVolumeRequest.class);
      _packAdmin.createVolume(volumeName, createVolumeRequest.getSizeInBytes(), createVolumeRequest.getBlockSize());
      return OKResponse.builder()
                       .message("Volume " + volumeName + " created")
                       .build();
    };
  }

  private Route getAssignedVolumes() {
    return (request, response) -> _packAdmin.getAssignedVolumes();
  }

  private Route getAllVolumes() {
    return (request, response) -> _packAdmin.getAllVolumes();
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder(toBuilder = true)
  public static class CreateVolumeRequest {

    @Builder.Default
    long sizeInBytes = 107374182400L;// 100 GiB

    @Builder.Default
    int blockSize = 1048576;// 1 MiB
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder(toBuilder = true)
  public static class GrowVolumeRequest {
    @Builder.Default
    long sizeInBytes = 0;
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder(toBuilder = true)
  public static class OKResponse {

    String message;

  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder(toBuilder = true)
  public static class VolumeInfoResponse {

    String volumeName;
    long sizeInBytes;
    int blockSizeInBytes;
    String assignedHostname;

  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder(toBuilder = true)
  public static class ErrorResponse {

    String error;

  }

}
