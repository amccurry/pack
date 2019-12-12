package pack.backstore.file.server;

public class BackstoreFileServerMain {

  public static void main(String[] args) throws Exception {
    BackstoreFileServerConfig config = BackstoreFileServerConfigArgs.create(args);
    try (BackstoreFileServerAdmin server = new BackstoreFileServerRW(config)) {
      server.start(true);
    }
  }

}
