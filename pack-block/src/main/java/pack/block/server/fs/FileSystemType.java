package pack.block.server.fs;

public enum FileSystemType {

  EXT4(Ext4LinuxFileSystem.INSTANCE), XFS(XfsLinuxFileSystem.INSTANCE);

  private LinuxFileSystem linuxFileSystem;

  private FileSystemType(LinuxFileSystem linuxFileSystem) {
    this.linuxFileSystem = linuxFileSystem;
  }

  public LinuxFileSystem getLinuxFileSystem() {
    return linuxFileSystem;
  }

  public static LinuxFileSystem getType(FileSystemType type) {
    return type.getLinuxFileSystem();
  }

  public static LinuxFileSystem getType(String type) {
    return FileSystemType.valueOf(type.toUpperCase())
                         .getLinuxFileSystem();
  }

}
