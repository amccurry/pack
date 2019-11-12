package pack.rs;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface RSProcessor {

  void reset();

  void read(ByteBuffer[] inputs, ByteBuffer output) throws IOException;

  void write(ByteBuffer buffer) throws IOException;

  ByteBuffer[] finish() throws IOException;

}
