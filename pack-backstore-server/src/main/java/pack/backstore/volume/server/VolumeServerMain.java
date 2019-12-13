package pack.backstore.volume.server;

public class VolumeServerMain {

  public static void main(String[] args) throws Exception {
    VolumeServerConfig config = VolumeServerConfigArgs.create(args);
    try (VolumeServer server = new VolumeServer(config)) {
      server.start(true);
    }
  }

}
