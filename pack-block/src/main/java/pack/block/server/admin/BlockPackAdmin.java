package pack.block.server.admin;

import java.io.Closeable;
import java.io.IOException;

import pack.block.server.BlockPackFuse;

public interface BlockPackAdmin extends Closeable {

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