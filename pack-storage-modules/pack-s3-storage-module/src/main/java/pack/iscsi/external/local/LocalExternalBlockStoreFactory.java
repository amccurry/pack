package pack.iscsi.external.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.external.ExternalBlockIOFactory;
import pack.iscsi.partitioned.block.Block;
import pack.iscsi.partitioned.block.BlockIOExecutor;
import pack.iscsi.partitioned.block.BlockIORequest;
import pack.iscsi.partitioned.block.BlockIOResponse;
import pack.iscsi.partitioned.block.BlockState;

public class LocalExternalBlockStoreFactory implements ExternalBlockIOFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalExternalBlockStoreFactory.class);

  private final File _storeDir;

  public LocalExternalBlockStoreFactory(File storeDir) {
    _storeDir = storeDir;
  }

  @Override
  public BlockIOExecutor getBlockWriter() throws IOException {
    return new BlockIOExecutor() {
      @Override
      public BlockIOResponse exec(BlockIORequest request) throws IOException {
        LOGGER.info("write request {} {}", request.getBlockId(), request.getOnDiskGeneration());

        File dstVolDir = new File(_storeDir, Long.toString(request.getVolumeId()));
        File dstBlockDir = new File(dstVolDir, Long.toString(request.getBlockId()));
        File dst = new File(dstBlockDir, Long.toString(request.getOnDiskGeneration()));
        dst.getParentFile()
           .mkdirs();

        try (FileOutputStream output = new FileOutputStream(dst)) {
          int len = request.getBlockSize();
          FileChannel channel = request.getChannel();
          byte[] buffer = new byte[4096];
          long position = 0;
          while (len > 0) {
            int read = channel.read(ByteBuffer.wrap(buffer), position);
            len -= read;
            position += read;
            output.write(buffer, 0, read);
          }
        }
        return BlockIOResponse.builder()
                              .lastStoredGeneration(request.getOnDiskGeneration())
                              .onDiskBlockState(request.getOnDiskState())
                              .onDiskGeneration(request.getOnDiskGeneration())
                              .build();
      }
    };
  }

  @Override
  public BlockIOExecutor getBlockReader() throws IOException {
    return new BlockIOExecutor() {

      @Override
      public BlockIOResponse exec(BlockIORequest request) throws IOException {
        long generation = request.getLastStoredGeneration();
        if (generation == Block.MISSING_BLOCK_GENERATION) {
          return BlockIOResponse.builder()
                                .lastStoredGeneration(generation)
                                .onDiskGeneration(generation)
                                .onDiskBlockState(BlockState.CLEAN)
                                .build();
        }
        LOGGER.info("read request {} {}", request.getBlockId(), generation);
        File srcVolDir = new File(_storeDir, Long.toString(request.getVolumeId()));
        File srcBlockDir = new File(srcVolDir, Long.toString(request.getBlockId()));
        File src = new File(srcBlockDir, Long.toString(generation));
        if (src.exists()) {
          try (FileInputStream input = new FileInputStream(src)) {
            FileChannel channel = request.getChannel();
            byte[] buffer = new byte[4096];
            int read;
            long position = 0;
            while ((read = input.read(buffer)) != -1) {
              int len = read;
              int offset = 0;
              while (len > 0) {
                int write = channel.write(ByteBuffer.wrap(buffer, offset, len), position);
                position += write;
                len -= write;
                offset += write;
              }
            }
          }
        } else {
          throw new FileNotFoundException(src.toString());
        }
        return BlockIOResponse.builder()
                              .lastStoredGeneration(generation)
                              .onDiskGeneration(generation)
                              .onDiskBlockState(BlockState.CLEAN)
                              .build();
      }
    };
  }
}
