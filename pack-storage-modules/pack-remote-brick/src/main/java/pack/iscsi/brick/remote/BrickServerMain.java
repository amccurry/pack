package pack.iscsi.brick.remote;

import java.io.File;

public class BrickServerMain {

  public static void main(String[] args) throws Exception {
    BrickServerConfig config = BrickServerConfig.builder()
                                                .brickDir(new File("./brick"))
                                                .build();
    try (BaseBrickServer server = new RandomAccessIOBrickServer(config)) {
      server.start(true);
    }
  }

}
