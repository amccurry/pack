package pack.iscsi.file.block.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockIOExecutor;
import pack.iscsi.spi.block.BlockIOFactory;
import pack.iscsi.spi.block.BlockIORequest;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;

public class LocalExternalBlockStoreFactory implements BlockIOFactory {

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
        long onDiskGeneration = request.getOnDiskGeneration();
        BlockState onDiskState = request.getOnDiskState();
        long lastStoredGeneration = request.getLastStoredGeneration();
        if (onDiskState == BlockState.CLEAN) {
          return BlockIOResponse.newBlockIOResult(onDiskGeneration, onDiskState, lastStoredGeneration);
        }
        long volumeId = request.getVolumeId();
        long blockId = request.getBlockId();
        LOGGER.info("write request {} onDiskGeneration {}", blockId, onDiskGeneration);

        File dstVolDir = new File(_storeDir, Long.toString(volumeId));
        File dstBlockDir = new File(dstVolDir, Long.toString(blockId));
        File dst = new File(dstBlockDir, Long.toString(onDiskGeneration));
        dst.getParentFile()
           .mkdirs();

        try (FileOutputStream output = new FileOutputStream(dst)) {
          RandomAccessIO randomAccessIO = request.getRandomAccessIO();
          byte[] buffer = new byte[4096];
          long position = request.getStartingPositionOfBlock();
          int length = request.getBlockSize();
          while (length > 0) {
            int len = Math.min(buffer.length, length);
            LOGGER.info("readFully position {} buffer {} len {}", position, buffer.length, len);
            try {
              randomAccessIO.readFully(position, buffer, 0, len);
            } catch (Throwable t) {
              LOGGER.error("Unknown error", t);
              throw t;
            }
            output.write(buffer, 0, len);
            position += len;
            length -= len;
          }
        } catch (IOException e) {
          LOGGER.error("Unknown error", e);
          throw e;
        } catch (Exception e) {
          LOGGER.error("Unknown error", e);
          throw e;
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
        LOGGER.info("read request {} generation {}", request.getBlockId(), generation);
        File srcVolDir = new File(_storeDir, Long.toString(request.getVolumeId()));
        File srcBlockDir = new File(srcVolDir, Long.toString(request.getBlockId()));
        File src = new File(srcBlockDir, Long.toString(generation));
        if (src.exists()) {
          try (FileInputStream input = new FileInputStream(src)) {
            RandomAccessIO randomAccessIO = request.getRandomAccessIO();
            byte[] buffer = new byte[4096];

            long position = request.getStartingPositionOfBlock();
            int length = request.getBlockSize();
            while (length > 0) {
              int read = input.read(buffer);
              randomAccessIO.writeFully(position, buffer, 0, read);
              position += read;
              length -= read;
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
