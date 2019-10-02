package pack.admin;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
import pack.iscsi.admin.ActionTable;
import pack.iscsi.admin.Column;
import pack.iscsi.admin.Menu;
import pack.iscsi.admin.MenuTable;
import pack.iscsi.admin.Row;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import spark.ModelAndView;
import spark.Request;
import spark.Response;
import spark.ResponseTransformer;
import spark.Route;
import spark.Service;
import spark.TemplateViewRoute;
import spark.template.freemarker.FreeMarkerEngine;

public class PackVolumeAdminServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackVolumeAdminServer.class);

  private static final String VOLUME_PREFIX = "/api/v1.0/volume";
  private static final String SNAPSHOT_ID_PARAM = ":snapshotId";
  private static final String VOLUME_NAME_PARAM = ":volumeName";
  private static final String ASSIGNED = "assigned";
  private static final String CREATE = "create";
  private static final String GROW = "grow";
  private static final String DELETE = "delete";
  private static final String ASSIGN = "assign";
  private static final String UNASSIGN = "unassign";
  private static final String SNAPSHOT = "snapshot";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static void main(String[] args) throws IOException {
    Service service = Service.ignite();

    String hostname = InetAddress.getLocalHost()
                                 .getHostName();

    Map<String, PackVolumeMetadata> allVolumes = new ConcurrentHashMap<>();
    List<String> assignedVolumes = new ArrayList<String>();

    ActionTable actionTable1 = new ActionTable() {

      @Override
      public String getLink() {
        return "test1";
      }

      @Override
      public List<Row> getRows() {
        return Arrays.asList(createRow("1"), createRow("2"), createRow("3"));
      }

      private Row createRow(String id) {
        return Row.builder()
                  .id(id)
                  .columns(Arrays.asList(Column.builder()
                                               .value("test1")
                                               .build(),
                      Column.builder()
                            .value("test2")
                            .build()))
                  .build();
      }

      @Override
      public List<String> getActions() {
        return Arrays.asList("action1", "action2");
      }

      @Override
      public List<String> getHeaders() {
        return Arrays.asList("col1", "col2");
      }
    };

    ActionTable actionTable2 = new ActionTable() {

      @Override
      public String getLink() {
        return "test2";
      }

      @Override
      public List<Row> getRows() {
        return Arrays.asList(createRow("1"), createRow("2"), createRow("3"));
      }

      private Row createRow(String id) {
        return Row.builder()
                  .id(id)
                  .columns(Arrays.asList(Column.builder()
                                               .value("test1")
                                               .build(),
                      Column.builder()
                            .value("test2")
                            .build()))
                  .build();
      }

      @Override
      public List<String> getHeaders() {
        return Arrays.asList("col1", "col2");
      }
    };

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

      @Override
      public void createSnapshot(String name, String snapshotName) throws IOException {
        LOGGER.info("createSnapshot {} {}", name, snapshotName);
      }

      @Override
      public List<String> listSnapshots(String name) throws IOException {
        LOGGER.info("listSnapshots {} {}", name);
        return Arrays.asList();
      }

      @Override
      public void deleteSnapshot(String name, String snapshotName) throws IOException {
        LOGGER.info("deleteSnapshot {} {}", name, snapshotName);
      }

      @Override
      public void sync(String name) throws IOException {
        LOGGER.info("sync {}", name);
      }

      @Override
      public void cloneVolume(String name, String existingVolume, String snapshotId) throws IOException {
        checkNonExistence(name);
        checkExistence(existingVolume);
        checkExistence(existingVolume, snapshotId);
        PackVolumeMetadata metadata = getVolumeMetadata(existingVolume, snapshotId);
        allVolumes.put(name, metadata.toBuilder()
                                     .name(name)
                                     .volumeId(new Random().nextLong())
                                     .build());
      }

      @Override
      public PackVolumeMetadata getVolumeMetadata(String name, String snapshotId) throws IOException {
        return getVolumeMetadata(name);
      }

      @Override
      public PackVolumeMetadata getVolumeMetadata(long volumeId, String snapshotId) throws IOException {
        return getVolumeMetadata(volumeId);
      }

    };
    PackVolumeAdminServer server = new PackVolumeAdminServer(service, packAdmin, actionTable1.getLink(), actionTable1,
        actionTable2);
    server.setup();
  }

  private final Service _service;
  private final PackVolumeStore _packAdmin;
  private final FreeMarkerEngine _engine = new FreeMarkerEngine();
  private final Map<String, ActionTable> _tables = new ConcurrentHashMap<>();
  private final MenuTable _menuTable;
  private final String _defaultActionTable;

  public PackVolumeAdminServer(Service service, PackVolumeStore packAdmin, String defaultActionTable,
      ActionTable... actionTables) throws IOException {
    _service = service;
    _service.staticFileLocation("/public");
    _packAdmin = packAdmin;
    _menuTable = new MenuTable() {

      @Override
      public List<Menu> getMenus() throws IOException {
        List<Menu> menus = new ArrayList<>();
        for (ActionTable actionTable : actionTables) {
          menus.add(Menu.builder()
                        .name(actionTable.getName())
                        .link("/" + actionTable.getLink())
                        .build());
        }
        Collections.sort(menus, new Comparator<Menu>() {
          @Override
          public int compare(Menu o1, Menu o2) {
            return o1.getName()
                     .compareTo(o2.getName());
          }
        });
        return menus;
      }
    };
    _defaultActionTable = defaultActionTable;
    for (ActionTable actionTable : actionTables) {
      addActionTable(actionTable);
    }
  }

  public void addActionTable(ActionTable actionTable) throws IOException {
    String name = actionTable.getLink();
    if (_tables.containsKey(name)) {
      throw new RuntimeException("Already contains table " + name);
    }
    _tables.put(name, actionTable);
    _service.get("/" + name, getActionTable(actionTable), _engine);
    _service.post("/" + name, portActionTable(actionTable), _engine);
  }

  public void setup() {
    ResponseTransformer transformer = model -> OBJECT_MAPPER.writeValueAsString(model);
    _service.get(toPath(VOLUME_PREFIX), getAllVolumes(), transformer);
    _service.get(toPath(VOLUME_PREFIX, ASSIGNED), getAssignedVolumes(), transformer);
    _service.post(toPath(VOLUME_PREFIX, CREATE, VOLUME_NAME_PARAM), createVolume(), transformer);
    _service.post(toPath(VOLUME_PREFIX, GROW, VOLUME_NAME_PARAM), growVolume(), transformer);
    _service.post(toPath(VOLUME_PREFIX, DELETE, VOLUME_NAME_PARAM), deleteVolume(), transformer);
    _service.post(toPath(VOLUME_PREFIX, ASSIGN, VOLUME_NAME_PARAM), assignVolume(), transformer);
    _service.post(toPath(VOLUME_PREFIX, UNASSIGN, VOLUME_NAME_PARAM), unassignVolume(), transformer);
    _service.get(toPath(VOLUME_PREFIX, VOLUME_NAME_PARAM), getVolumeInfo(), transformer);
    _service.get(toPath(VOLUME_PREFIX, SNAPSHOT, VOLUME_NAME_PARAM), getSnapshots(), transformer);
    _service.post(toPath(VOLUME_PREFIX, SNAPSHOT, CREATE, VOLUME_NAME_PARAM, SNAPSHOT_ID_PARAM), createSnapshot(),
        transformer);
    _service.post(toPath(VOLUME_PREFIX, SNAPSHOT, DELETE, VOLUME_NAME_PARAM, SNAPSHOT_ID_PARAM), deleteSnapshot(),
        transformer);

    ActionTable defaultActionTable = getActionTable(_defaultActionTable);
    _service.get("/", getActionTable(defaultActionTable), _engine);
    _service.post("/", portActionTable(defaultActionTable), _engine);

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

  private String toPath(String... path) {
    return Joiner.on('/')
                 .join(path);
  }

  private ActionTable getActionTable(String name) {
    return _tables.get(name);
  }

  private TemplateViewRoute portActionTable(ActionTable actionTable) {
    return new TemplateViewRoute() {
      @Override
      public ModelAndView handle(Request request, Response response) throws Exception {
        String action = request.queryParams("action");
        actionTable.execute(action, request.queryParamsValues("id"));
        response.redirect("/" + actionTable.getLink());
        return null;
      }
    };
  }

  private TemplateViewRoute getActionTable(ActionTable actionTable) {
    return (request, response) -> {
      Map<String, Object> model = new HashMap<>();
      List<Menu> menus = new ArrayList<>();
      menus.add(Menu.builder()
                    .link("/")
                    .name("Pack")
                    .build());
      menus.addAll(_menuTable.getMenus());
      model.put("menus", menus);
      model.put("name", actionTable.getName());
      model.put("link", actionTable.getLink());
      model.put("headers", actionTable.getHeaders());
      model.put("rows", actionTable.getRows());
      model.put("actions", actionTable.getActions());
      return new ModelAndView(model, "table.ftl");
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
