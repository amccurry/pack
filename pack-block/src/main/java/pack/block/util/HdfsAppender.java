package pack.block.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.log4j.Layout;
import org.apache.log4j.WriterAppender;

public class HdfsAppender extends WriterAppender {

  private static final int PRIORITY = 20000;
  private final FSDataOutputStream _output;
  private final OutputStreamWriter _writer;
  private final AtomicBoolean _closed = new AtomicBoolean();

  public HdfsAppender(Layout layout, FSDataOutputStream output) {
    _output = output;
    setLayout(layout);
    activateOptions();
    _writer = new OutputStreamWriter(_output) {
      @Override
      public void flush() throws IOException {
        super.flush();
        _output.hflush();
      }
    };
    ShutdownHookManager.get()
                       .addShutdownHook(() -> {
                         _closed.set(true);
                         Utils.closeQuietly(_writer);
                       }, PRIORITY);
  }

  public void activateOptions() {
    setWriter(removalableWriter(_writer));
    super.activateOptions();
  }

  private Writer removalableWriter(OutputStreamWriter writer) {
    return new Writer() {

      @Override
      public void write(char[] cbuf, int off, int len) throws IOException {
        if (!_closed.get()) {
          _writer.write(cbuf, off, len);
        }
      }

      @Override
      public void flush() throws IOException {
        if (!_closed.get()) {
          _writer.flush();
        }
      }

      @Override
      public void close() throws IOException {
        if (!_closed.get()) {
          _writer.close();
        }
      }
    };
  }

  protected final void closeWriter() {
    super.closeWriter();
  }

}
