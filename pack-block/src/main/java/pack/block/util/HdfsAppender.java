package pack.block.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.log4j.Layout;
import org.apache.log4j.WriterAppender;

import com.google.common.io.Closer;

public class HdfsAppender extends WriterAppender {

  private static final int PRIORITY = 20000;
  private final FSDataOutputStream _output;
  private final OutputStreamWriter _writer;
  private final static AtomicBoolean CLOSED = new AtomicBoolean();
  private final static Closer CLOSER = Closer.create();
  static {
    ShutdownHookManager.get()
                       .addShutdownHook(() -> {
                         CLOSED.set(true);
                         Utils.closeQuietly(CLOSER);
                       }, PRIORITY);
  }

  public HdfsAppender(Layout layout, FSDataOutputStream output) {
    _output = output;
    CLOSER.register((Closeable) () -> IOUtils.closeQuietly(_output));
    setLayout(layout);
    activateOptions();
    _writer = new OutputStreamWriter(_output) {
      @Override
      public void flush() throws IOException {
        super.flush();
        _output.hflush();
      }
    };
  }

  public void activateOptions() {
    setWriter(removalableWriter(_writer));
    super.activateOptions();
  }

  private Writer removalableWriter(OutputStreamWriter writer) {
    return new Writer() {

      @Override
      public void write(char[] cbuf, int off, int len) throws IOException {
        if (!CLOSED.get()) {
          _writer.write(cbuf, off, len);
        }
      }

      @Override
      public void flush() throws IOException {
        if (!CLOSED.get()) {
          _writer.flush();
        }
      }

      @Override
      public void close() throws IOException {
        if (!CLOSED.get()) {
          _writer.close();
        }
      }
    };
  }
}
