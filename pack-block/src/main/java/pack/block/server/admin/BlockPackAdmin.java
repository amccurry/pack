package pack.block.server.admin;

import pack.block.server.BlockPackFuse;

public interface BlockPackAdmin {

  default void setStatus(Status status) {
  }

  default void setStatus(Status status, String message) {
  }

  default BlockPackFuse register(BlockPackFuse blockPackFuse) {
    return blockPackFuse;
  }

}