package pack.iscsi.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;

import pack.iscsi.spi.StorageModuleFactory;
import pack.iscsi.spi.StorageModuleLoader;

public class IscsiConfig {

  public static List<StorageModuleFactory> getStorageModuleFactories(String configPath) throws IOException {
    Properties properties = new Properties();
    load(properties, new File(configPath));
    List<StorageModuleFactory> results = new ArrayList<>();
    ServiceLoader<StorageModuleLoader> serviceLoader = ServiceLoader.load(StorageModuleLoader.class);
    for (StorageModuleLoader moduleLoader : serviceLoader) {
      String type = moduleLoader.getType();
      Collection<Entry<String, Properties>> props = getPropertiesForType(properties, type);
      for (Entry<String, Properties> e : props) {
        String instanceName = e.getKey();
        Properties subProperties = e.getValue();
        results.add(moduleLoader.create(instanceName, subProperties));
      }
    }
    return results;
  }

  private static void load(Properties properties, File file) throws IOException {
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        load(properties, f);
      }
    } else {
      try (FileInputStream input = new FileInputStream(file)) {
        properties.load(input);
      }
    }
  }

  private static Collection<Entry<String, Properties>> getPropertiesForType(Properties properties, String type) {
    Map<String, Properties> results = new HashMap<>();

    Set<String> names = properties.stringPropertyNames();
    for (String name : names) {
      List<String> list = Splitter.on('.')
                                  .splitToList(name);
      if (list.size() < 3) {
        continue;
      }
      if (list.get(0)
              .equals(type)) {
        String instanceName = list.get(1);
        Properties subProperties = results.get(instanceName);
        if (subProperties == null) {
          results.put(instanceName, subProperties = new Properties());
        }
        String newName = Joiner.on('.')
                               .join(list.subList(2, list.size()));
        subProperties.put(newName, properties.get(name));
      }
    }
    return results.entrySet();
  }

}
