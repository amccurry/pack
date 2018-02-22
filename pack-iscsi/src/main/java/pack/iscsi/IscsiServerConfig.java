package pack.iscsi;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.codahale.metrics.MetricRegistry;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class IscsiServerConfig {

  String address;
  int port;
  TargetManager iscsiTargetManager;
  MetricRegistry registry;
  Configuration configuration;
  Path root;
  File cacheDir;
  UserGroupInformation ugi;

}
