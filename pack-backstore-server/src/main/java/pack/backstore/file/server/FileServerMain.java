package pack.backstore.file.server;

public class FileServerMain {

  public static void main(String[] args) throws Exception {
    FileServerConfig config = FileServerConfigArgs.create(args);
    try (FileServerAdmin server = new FileServerReadWrite(config)) {
      server.start(true);
    }
  }

}
