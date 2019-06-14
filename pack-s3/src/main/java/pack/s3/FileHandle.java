package pack.s3;

import java.io.Closeable;

import jnr.ffi.Pointer;

public interface FileHandle extends Closeable {

  int read(Pointer buf, int size, long offset) throws Exception;

  int write(Pointer buf, int size, long offset) throws Exception;

}