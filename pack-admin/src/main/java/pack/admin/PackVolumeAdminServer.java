package pack.admin;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import spark.ResponseTransformer;
import spark.Route;
import spark.Service;

public class PackVolumeAdminServer {

  private static final String SYNC = "sync";
  private static final Logger LOGGER = LoggerFactory.getLogger(PackVolumeAdminServer.class);
  private static final String GC = "gc";
  private static final String READ_ONLY = "readOnly";
  private static final String API_PREFIX = "/api/v1.0/volume";
  private static final String CLONE_VOLUME_NAME_PARAM = ":cloneVolumeName";
  private static final String SNAPSHOT_ID_PARAM = ":snapshotId";
  private static final String VOLUME_NAME_PARAM = ":volumeName";
  private static final String ATTACHED = "attached";
  private static final String CREATE = "create";
  private static final String CLONE = "clone";
  private static final String GROW = "grow";
  private static final String DELETE = "delete";
  private static final String ATTACH = "attach";
  private static final String DETACH = "detach";
  private static final String SNAPSHOT = "snapshot";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Service _service;
  private final PackVolumeStore _packAdmin;

  public PackVolumeAdminServer(Service service, PackVolumeStore packAdmin) throws IOException {
    _service = service;
    _packAdmin = packAdmin;
  }

  public void setup() {
    ResponseTransformer transformer = model -> OBJECT_MAPPER.writeValueAsString(model);
    _service.get(toPath(API_PREFIX), getAllVolumes(), transformer);
    _service.get(toPath(API_PREFIX, ATTACHED), getAttachedVolumes(), transformer);
    _service.post(toPath(API_PREFIX, CREATE, VOLUME_NAME_PARAM), createVolume(), transformer);
    _service.post(toPath(API_PREFIX, CLONE, VOLUME_NAME_PARAM, SNAPSHOT_ID_PARAM, CLONE_VOLUME_NAME_PARAM),
        cloneVolume(), transformer);
    _service.post(toPath(API_PREFIX, GC, VOLUME_NAME_PARAM), gcVolume(), transformer);
    _service.post(toPath(API_PREFIX, GROW, VOLUME_NAME_PARAM), growVolume(), transformer);
    _service.post(toPath(API_PREFIX, DELETE, VOLUME_NAME_PARAM), deleteVolume(), transformer);
    _service.post(toPath(API_PREFIX, ATTACH, VOLUME_NAME_PARAM), attachVolume(), transformer);
    _service.post(toPath(API_PREFIX, DETACH, VOLUME_NAME_PARAM), detachVolume(), transformer);
    _service.get(toPath(API_PREFIX, VOLUME_NAME_PARAM), getVolumeInfo(), transformer);
    _service.get(toPath(API_PREFIX, SNAPSHOT, VOLUME_NAME_PARAM), getSnapshots(), transformer);
    _service.post(toPath(API_PREFIX, SNAPSHOT, CREATE, VOLUME_NAME_PARAM, SNAPSHOT_ID_PARAM), createSnapshot(),
        transformer);
    _service.post(toPath(API_PREFIX, SNAPSHOT, DELETE, VOLUME_NAME_PARAM, SNAPSHOT_ID_PARAM), deleteSnapshot(),
        transformer);
    _service.post(toPath(API_PREFIX, SYNC, VOLUME_NAME_PARAM), sync(), transformer);

    _service.exception(Exception.class, (exception, request, response) -> {
      LOGGER.error("Unknown error", exception);
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

  private String toPath(String... path) {
    return Joiner.on('/')
                 .join(path);
  }

  private Route gcVolume() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      _packAdmin.gc(volumeName);
      return OKResponse.builder()
                       .message("Volume " + volumeName + " gc complete")
                       .build();
    };
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

  private Route getSnapshots() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      return _packAdmin.listSnapshots(volumeName);
    };
  }

  private Route detachVolume() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      _packAdmin.detachVolume(volumeName);
      return OKResponse.builder()
                       .message("Volume " + volumeName + " detached")
                       .build();
    };
  }

  private Route attachVolume() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      _packAdmin.attachVolume(volumeName);
      return OKResponse.builder()
                       .message("Volume " + volumeName + " attached")
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

  private Route cloneVolume() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      String snapshotId = request.params(SNAPSHOT_ID_PARAM);
      String cloneVolumeName = request.params(CLONE_VOLUME_NAME_PARAM);
      String roValue = request.queryParams(READ_ONLY);
      boolean readOnly = roValue != null && Boolean.parseBoolean(roValue.toLowerCase());
      _packAdmin.cloneVolume(cloneVolumeName, volumeName, snapshotId, readOnly);
      return OKResponse.builder()
                       .message("Volume " + cloneVolumeName + " cloned from " + volumeName + "/" + snapshotId)
                       .build();
    };
  }

  private Route createSnapshot() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      String snapshotId = request.params(SNAPSHOT_ID_PARAM);
      _packAdmin.createSnapshot(volumeName, snapshotId);
      return OKResponse.builder()
                       .message("Snapshot " + snapshotId + " for Volume " + volumeName + " created")
                       .build();
    };
  }

  private Route deleteSnapshot() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      String snapshotId = request.params(SNAPSHOT_ID_PARAM);
      _packAdmin.deleteSnapshot(volumeName, snapshotId);
      return OKResponse.builder()
                       .message("Snapshot " + snapshotId + " for Volume " + volumeName + " deleted")
                       .build();
    };
  }

  private Route getAttachedVolumes() {
    return (request, response) -> _packAdmin.getAttachedVolumes();
  }

  private Route getAllVolumes() {
    return (request, response) -> _packAdmin.getAllVolumes();
  }

  private Route sync() {
    return (request, response) -> {
      String volumeName = request.params(VOLUME_NAME_PARAM);
      _packAdmin.sync(volumeName);
      return OKResponse.builder()
                       .message("Volume " + volumeName + " was synced")
                       .build();
    };
  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder(toBuilder = true)
  public static class CreateVolumeRequest {

    @Builder.Default
    long sizeInBytes = 107374182400L;// 100 GiB

    @Builder.Default
    int blockSize = 16 * 1048576;// 16 MiB
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
    String attachedHostname;

  }

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder(toBuilder = true)
  public static class ErrorResponse {

    String error;

  }

}
