package pack.block.blockstore.hdfs.v4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.file.WalKeyWritable;

public class WalFileFactorySequenceFile extends WalFileFactory {

  private final static Logger LOGGER = LoggerFactory.getLogger(WalFileFactorySequenceFile.class);

  private final CompressionCodecFactory _compressionCodecFactory;
  private final CompressionType _compressionType;
  private final CompressionCodec _compressCodec;

  public WalFileFactorySequenceFile(FileSystem fileSystem, HdfsMetaData metaData) {
    super(fileSystem, metaData);
    _compressionCodecFactory = new CompressionCodecFactory(fileSystem.getConf());
    _compressionType = getCompressType(metaData.getWalCompressionType());
    LOGGER.info("WAL compression type {}", _compressionType);
    _compressCodec = getCompressCodec(metaData.getWalCompressionCodec());
    LOGGER.info("WAL compression codec {}", _compressCodec);
  }

  public WalFile.Writer create(Path path) throws IOException {
    Writer writer = createWriter(path);
    return new WalFile.Writer() {

      @Override
      public void close() throws IOException {
        writer.close();
      }

      @Override
      public long getLength() throws IOException {
        return writer.getLength();
      }

      @Override
      public void flush() throws IOException {
        writer.hflush();
      }

      @Override
      public void append(WalKeyWritable key, BytesWritable value) throws IOException {
        writer.append(key, value);
      }
    };
  }

  private Writer createWriter(Path path) throws IOException {
    List<Option> options = new ArrayList<>();
    options.add(SequenceFile.Writer.file(path));
    options.add(SequenceFile.Writer.keyClass(WalKeyWritable.class));
    options.add(SequenceFile.Writer.valueClass(BytesWritable.class));
    if (_compressionType != null) {
      if (_compressCodec != null) {
        options.add(SequenceFile.Writer.compression(_compressionType, _compressCodec));
      } else {
        options.add(SequenceFile.Writer.compression(_compressionType));
      }
    }
    return SequenceFile.createWriter(_fileSystem.getConf(), options.toArray(new Option[options.size()]));
  }

  public WalFile.Reader open(Path path) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(_fileSystem.getConf(), SequenceFile.Reader.file(path));
    return new WalFile.Reader() {

      @Override
      public void close() throws IOException {
        reader.close();
      }

      @Override
      public boolean next(WalKeyWritable key, BytesWritable value) throws IOException {
        return reader.next(key, value);
      }
    };
  }

  private CompressionCodec getCompressCodec(String walCompressionCodec) {
    if (walCompressionCodec == null) {
      return null;
    }
    CompressionCodec codecByName = _compressionCodecFactory.getCodecByName(walCompressionCodec);
    if (codecByName != null) {
      return codecByName;
    }
    return _compressionCodecFactory.getCodecByClassName(walCompressionCodec);
  }

  private CompressionType getCompressType(String walCompressionType) {
    if (walCompressionType == null) {
      return null;
    }
    return CompressionType.valueOf(walCompressionType.toUpperCase());
  }

  @Override
  public void recover(Path src, Path dst) throws IOException {
    WalKeyWritable key = new WalKeyWritable();
    BytesWritable value = new BytesWritable();
    try (SequenceFile.Reader reader = new SequenceFile.Reader(_fileSystem.getConf(), SequenceFile.Reader.file(src))) {
      try (Writer writer = createWriter(dst)) {
        while (reader.next(key, value)) {
          writer.append(key, value);
        }
      }
    } catch (IOException e) {
      LOGGER.info("Reached error, assuming end of file");
    }
  }
}
