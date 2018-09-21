package pack.nativehdfs;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;

import jnrfuse.struct.FileStat;

public class TestFile {

  public static void main(String[] args) throws Exception {

    // File file = new File("./src", "/");
    // System.out.println(file.getCanonicalPath());

    // File f1 = new File("/mnt/testing/test1");
    // File f2 = new File("/mnt/testing/test2");
    //
    // System.out.println(f1.renameTo(f2));

    Path p = Paths.get("/");

    PosixFileAttributeView posixView = Files.getFileAttributeView(p, PosixFileAttributeView.class);
    PosixFileAttributes attributes = posixView.readAttributes();
    UserPrincipal owner = attributes.owner();
    GroupPrincipal group = attributes.group();
    Set<PosixFilePermission> permissionsSet = attributes.permissions();

    int permission = getPermission(permissionsSet);
    System.out.println(permission);
    System.out.println(getUid(owner));
    System.out.println(getGid(group));

  }

  private static int getPermission(Set<PosixFilePermission> permissionsSet) {
    int permission = 0;
    for (PosixFilePermission filePermission : permissionsSet) {
      switch (filePermission) {
      case OWNER_READ:
        permission = (permission | FileStat.S_IRUSR);
        break;
      case OWNER_WRITE:
        permission = (permission | FileStat.S_IWUSR);
        break;
      case OWNER_EXECUTE:
        permission = (permission | FileStat.S_IXUSR);
        break;
      case GROUP_READ:
        permission = (permission | FileStat.S_IRGRP);
        break;
      case GROUP_WRITE:
        permission = (permission | FileStat.S_IWGRP);
        break;
      case GROUP_EXECUTE:
        permission = (permission | FileStat.S_IXGRP);
        break;
      case OTHERS_READ:
        permission = (permission | FileStat.S_IROTH);
        break;
      case OTHERS_WRITE:
        permission = (permission | FileStat.S_IWOTH);
        break;
      case OTHERS_EXECUTE:
        permission = (permission | FileStat.S_IXOTH);
        break;
      default:
        break;
      }
    }
    return permission;
  }

  private static Number getUid(UserPrincipal owner) throws Exception {
    Field field = owner.getClass()
                       .getDeclaredField("id");
    field.setAccessible(true);
    return (Number) field.get(owner);
  }

  private static Number getGid(GroupPrincipal group) throws Exception {
    Field field = group.getClass()
                       .getSuperclass()
                       .getDeclaredField("id");
    field.setAccessible(true);
    return (Number) field.get(group);
  }

}
