package pack.rs;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

public class RSManager {

  private static final String RS = "rs";

  static {
    RSUtil.loadNativeLibrary();
  }

  private final ByteBuffer _dataBuffer;
  private final ByteBuffer _shuffleBuffer;
  private final ByteBuffer _parityBuffer;
  private final int _dataPartCount;
  private final int _parityPartCount;
  private final int _blockSize;
  private final Configuration _conf = new Configuration();
  private final RawErasureEncoder _encoder;
  private final RawErasureDecoder _decoder;

  private RSManager(int byteBufferSize, int dataPartCount, int parityPartCount, int blockSize) {
    _dataBuffer = ByteBuffer.allocateDirect(byteBufferSize);
    _shuffleBuffer = ByteBuffer.allocateDirect(byteBufferSize);
    _parityBuffer = ByteBuffer.allocateDirect(byteBufferSize);
    _dataPartCount = dataPartCount;
    _parityPartCount = parityPartCount;
    _blockSize = blockSize;
    ErasureCoderOptions options = new ErasureCoderOptions(dataPartCount, parityPartCount);
    _encoder = CodecUtil.createRawEncoder(_conf, RS, options);
    _decoder = CodecUtil.createRawDecoder(_conf, RS, options);
  }

  public static RSManager create(int byteBufferSize, int dataPartCount, int parityPartCount, int blockSize) {
    return new RSManager(byteBufferSize, dataPartCount, parityPartCount, blockSize);
  }

  public static void reset(ByteBuffer... buffer) {
    for (ByteBuffer byteBuffer : buffer) {
      if (byteBuffer != null) {
        byteBuffer.position(0);
      }
    }
  }

  public void reset() {
    _dataBuffer.clear();
    _shuffleBuffer.clear();
    _parityBuffer.clear();
  }

  public void read(ByteBuffer[] inputs, ByteBuffer output) throws IOException {
    checkInputs(inputs);
    checkOutput(inputs, output);
    createMissingInputsIfNeeded(inputs);
    copyToOutput(inputs, output);
    output.flip();
  }

  public void write(ByteBuffer buffer) {
    _dataBuffer.put(buffer);
  }

  public ByteBuffer[] finish() throws IOException {
    _dataBuffer.flip();
    checkBufferSize();
    int byteBufferSize = _dataBuffer.limit() / _dataPartCount;
    ByteBuffer[] dataParts = toDataParts(_dataBuffer, _shuffleBuffer, _dataPartCount, _blockSize, byteBufferSize);
    ByteBuffer[] parityParts = setupParityBuffers(byteBufferSize);
    _encoder.encode(dataParts, parityParts);
    return getResults(dataParts, parityParts);
  }

  private ByteBuffer[] setupParityBuffers(int byteBufferSize) {
    ByteBuffer[] parityParts = new ByteBuffer[_parityPartCount];
    int pos = 0;
    for (int i = 0; i < _parityPartCount; i++) {
      _parityBuffer.position(pos);
      _parityBuffer.limit(pos + byteBufferSize);
      parityParts[i] = _parityBuffer.slice();
      pos += byteBufferSize;
    }
    return parityParts;
  }

  private ByteBuffer[] getResults(ByteBuffer[] dataParts, ByteBuffer[] parityParts) {
    ByteBuffer[] result = new ByteBuffer[dataParts.length + parityParts.length];
    int index = 0;
    for (int j = 0; j < dataParts.length; j++) {
      ByteBuffer byteBuffer = dataParts[j];
      byteBuffer.flip();
      result[index] = byteBuffer;
      index++;
    }
    for (int j = 0; j < parityParts.length; j++) {
      result[index] = parityParts[j];
      index++;
    }
    return result;
  }

  private void checkBufferSize() throws IOException {
    if (_dataBuffer.limit() % _blockSize != 0) {
      throw new IOException(
          "Data written " + _dataBuffer.limit() + " is not multiple of min stripe size " + _blockSize);
    }
  }

  private static ByteBuffer[] toDataParts(ByteBuffer data, ByteBuffer buffer, int parts, int blockSize,
      int byteBufferSize) {
    int sizeLength = data.limit();
    int partsPerBuffer = sizeLength / blockSize;
    int partSize = blockSize / parts;
    ByteBuffer[] buffers = new ByteBuffer[parts];
    int pos = 0;
    for (int i = 0; i < parts; i++) {
      buffer.position(pos);
      buffer.limit(pos + byteBufferSize);
      buffers[i] = buffer.slice();
      copyToBuffer(buffers[i], data, i, partsPerBuffer, blockSize, partSize);
      buffers[i].flip();
      pos += byteBufferSize;
    }
    return buffers;
  }

  private static void copyToBuffer(ByteBuffer buffer, ByteBuffer data, int bufferId, int partsPerBuffer, int blockSize,
      int partSize) {
    int dataPos = bufferId * partSize;
    for (int part = 0; part < partsPerBuffer; part++) {
      // System.out.println("Fill pos " + dataPos + " " + bufferId + " " +
      // part);
      ByteBuffer duplicate = data.duplicate();
      duplicate.position(dataPos);
      duplicate.limit(dataPos + partSize);
      buffer.put(duplicate.slice());
      dataPos += blockSize;
    }
  }

  private void copyToOutput(ByteBuffer[] inputs, ByteBuffer output) throws IOException {
    int byteBufferSize = getLimitOfOne(inputs);
    ByteBuffer[] buffers = new ByteBuffer[_dataPartCount];
    System.arraycopy(inputs, 0, buffers, 0, _dataPartCount);

    int length = byteBufferSize * _dataPartCount;
    int partSize = _blockSize / buffers.length;
    while (length > 0) {
      for (int b = 0; b < buffers.length; b++) {
        ByteBuffer buffer = buffers[b];
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.limit(duplicate.position() + partSize);
        ByteBuffer slice = duplicate.slice();
        output.put(slice);
        buffer.position(buffer.position() + partSize);
        length -= partSize;
      }
    }
  }

  private void checkOutput(ByteBuffer[] inputs, ByteBuffer output) throws IOException {
    int limit = getLimitOfOne(inputs);
    int length = limit * _dataPartCount;
    if (output.remaining() < length) {
      throw new IOException("Output buffer too small " + output.remaining() + " needed " + length);
    }
  }

  private int getLimitOfOne(ByteBuffer[] inputs) throws IOException {
    for (ByteBuffer buffer : inputs) {
      if (buffer != null) {
        return buffer.limit();
      }
    }
    throw new IOException("No non-null inputs");
  }

  private void checkInputs(ByteBuffer[] inputs) throws IOException {
    ByteBuffer buf = null;
    for (ByteBuffer buffer : inputs) {
      if (buffer != null) {
        if (buffer.position() != 0) {
          throw new IOException("Buffer position not 0 " + buffer.position());
        }
        if (buf == null) {
          buf = buffer;
        } else if (buf.limit() != buffer.limit()) {
          throw new IOException("Buffer limits do not match " + buf.limit() + " " + buffer.limit());
        }
      }
    }
  }

  private void createMissingInputsIfNeeded(ByteBuffer[] inputs) throws IOException {
    int[] erasedIndexes = new int[0];
    for (int i = 0; i < _dataPartCount; i++) {
      if (inputs[i] == null) {
        int newLength = erasedIndexes.length + 1;
        int[] newErasedIndexes = new int[newLength];
        System.arraycopy(erasedIndexes, 0, newErasedIndexes, 0, erasedIndexes.length);
        newErasedIndexes[newLength - 1] = i;
        erasedIndexes = newErasedIndexes;
      }
    }
    if (erasedIndexes.length == 0) {
      return;
    }

    int length = 0;
    for (int i = 0; i < inputs.length; i++) {
      ByteBuffer byteBuffer = inputs[i];
      if (byteBuffer == null) {
        continue;
      }
      length = byteBuffer.limit();
      if (!byteBuffer.isDirect()) {
        int start = _dataBuffer.position();
        _dataBuffer.put(byteBuffer);
        ByteBuffer duplicate = _dataBuffer.duplicate();
        int end = duplicate.position();
        duplicate.position(start);
        duplicate.limit(end);
        inputs[i] = duplicate.slice();
      }
    }

    ByteBuffer[] outputs = new ByteBuffer[erasedIndexes.length];
    for (int i = 0; i < erasedIndexes.length; i++) {
      int start = _dataBuffer.position();
      ByteBuffer duplicate = _dataBuffer.duplicate();
      duplicate.position(start);
      duplicate.limit(length + start);
      outputs[i] = duplicate.slice();
    }

    _decoder.decode(inputs, erasedIndexes, outputs);

    for (int i = 0; i < erasedIndexes.length; i++) {
      int index = erasedIndexes[i];
      inputs[index] = outputs[i];
    }

    for (ByteBuffer buffer : inputs) {
      if (buffer != null) {
        buffer.position(0);
      }
    }
  }

}
