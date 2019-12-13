package pack.backstore.coordinator.server;

public class CoordinatorServerMain {

  public static void main(String[] args) throws Exception {
    CoordinatorServerConfig config = CoordinatorServerConfigArgs.create(args);
    try (CoordinatorServer server = new CoordinatorServer(config)) {
      server.start(true);
    }
  }

}
