package pack.block.server.admin;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;

import pack.block.server.BlockPackFuse;

public interface BlockPackAdmin extends Closeable {

  public static BlockPackAdmin getLoggerInstance(Logger logger) {
    return new BlockPackAdmin() {
      @Override
      public void setStatus(Status status) {
        logger.info("status {}", status);
      }

      @Override
      public void setStatus(Status status, String message) {
        logger.info("status {} message {}", status, message);
      }
    };
  }

  default void setStatus(Status status) {
  }

  default void setStatus(Status status, String message) {
  }

  default BlockPackFuse register(BlockPackFuse blockPackFuse) {
    return blockPackFuse;
  }

  @Override
  default void close() throws IOException {

  }

}