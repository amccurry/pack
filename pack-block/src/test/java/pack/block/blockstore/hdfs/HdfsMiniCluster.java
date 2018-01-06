package pack.block.blockstore.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class HdfsMiniCluster {

  private static MiniDFSCluster cluster;
  private static FileSystem fileSystem;

  public static void main(String[] args) throws IOException, InterruptedException {
    File file = new File(args[0]);
    file.mkdirs();

    File conf = new File(file, "conf");
    conf.mkdirs();

    File hdfs = new File(file, "hdfs");
    hdfs.mkdirs();

    Configuration configuration = new Configuration();
    String storePath = hdfs.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    cluster = new MiniDFSCluster.Builder(configuration).build();
    fileSystem = cluster.getFileSystem();

    Configuration config = new Configuration(fileSystem.getConf());
    config.set("fs.default.name", fileSystem.getUri()
                                            .toString());
    config.set("fs.defaultFS", fileSystem.getUri()
                                         .toString());

    writeConfiguration(config, new File(conf, "hdfs-site.xml"));
    writeConfiguration(config, new File(conf, "core-site.xml"));

    while (true) {
      Thread.sleep(100000000);
    }
  }

  private static void writeConfiguration(Configuration conf, File file) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      conf.writeXml(outputStream);
    }
  }

}
