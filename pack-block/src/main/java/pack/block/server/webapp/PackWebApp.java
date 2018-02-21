package pack.block.server.webapp;

import com.fasterxml.jackson.databind.ObjectMapper;

import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.server.webapp.request.CreateOrUpdateVolumeRequest;
import pack.block.server.webapp.request.ListVolumeRequest;
import pack.block.server.webapp.response.ErrorResponse;
import pack.block.server.webapp.response.InfoResponse;
import pack.block.server.webapp.response.ListVolumeResponse;
import spark.Route;
import spark.Service;

public class PackWebApp {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String NAME = "name";

  public static void main(String[] args) {
    Service service = Service.ignite();
    PackDao packDao = new MockPackDao();

    Route listVolumes = (request, response) -> {
      ListVolumeRequest listVolumeRequest = MAPPER.readValue(request.body(), ListVolumeRequest.class);
      return ListVolumeResponse.builder()
                               .volumes(packDao.listVolumes(listVolumeRequest.getFilter()))
                               .build();
    };

    Route createOrUpdateVolume = (request, response) -> {
      CreateOrUpdateVolumeRequest createVolumeRequest = MAPPER.readValue(request.body(),
          CreateOrUpdateVolumeRequest.class);
      try {
        packDao.createOrUpdate(createVolumeRequest.getName(), createVolumeRequest.getOptions());
        return ErrorResponse.builder()
                            .build();
      } catch (Exception e) {
        return ErrorResponse.builder()
                            .error(e.getMessage())
                            .build();
      }
    };

    Route deleteVolume = (request, response) -> {
      String name = request.params(NAME);
      try {
        packDao.delete(name);
        return ErrorResponse.builder()
                            .build();
      } catch (Exception e) {
        return ErrorResponse.builder()
                            .error(e.getMessage())
                            .build();
      }
    };

    Route infoRoute = (request, response) -> {
      String name = request.params(NAME);
      try {
        HdfsMetaData metaData = packDao.get(name);
        if (metaData == null) {
          service.halt(404);
        }
        return InfoResponse.builder()
                           .name(name)
                           .options(metaData)
                           .build();
      } catch (Exception e) {
        return InfoResponse.builder()
                           .error(e.getMessage())
                           .build();
      }
    };

    service.post("/volume/:name", createOrUpdateVolume);
    service.put("/volume/:name", createOrUpdateVolume);
    service.get("/volume/:name", infoRoute);
    service.delete("/volume/:name", deleteVolume);

    service.get("/volumes", listVolumes);

    // service.get("/list.compactions", null);
    // service.get("/get.compaction/:id", null);
    // service.get("/get.mount/:name", null);
    // service.get("/get.mount.metrics/:name", null);

    service.init();
  }

}
