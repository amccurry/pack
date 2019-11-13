package pack.iscsi.brick.remote.server;

public class BrickServerMain {

  public static void main(String[] args) throws Exception {
    BrickServerConfig config = BrickServerConfigArgs.create(args);
    try (BaseBrickServer server = new FileBrickServer(config)) {
      server.start(true);
    }
  }

}
