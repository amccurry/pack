package pack.backstore.config;

public interface ServerConfig {

  String getHostname();

  int getPort();

  int getClientTimeout();

  int getMinThreads();

  int getMaxThreads();

}
