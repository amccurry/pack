package iscsi.cli;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.security.UserGroupInformation;

import pack.distributed.storage.PackConfig;

public class Main {

  public static void main(String[] args) throws ParseException, IOException, InterruptedException {
    PackCommandGroup packCommandGroup = new PackCommandGroup();
    UserGroupInformation ugi = PackConfig.getUgi();
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      packCommandGroup.process(Arrays.asList(args));
      return null;
    });
  }
}
