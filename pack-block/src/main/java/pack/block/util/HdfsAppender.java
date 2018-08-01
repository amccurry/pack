package pack.block.util;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Layout;
import org.apache.log4j.WriterAppender;

public class HdfsAppender extends WriterAppender {

  private final FSDataOutputStream _output;

  public HdfsAppender(Layout layout, FSDataOutputStream output) {
    _output = output;
    setLayout(layout);
    activateOptions();
  }

  public void activateOptions() {
    setWriter(new OutputStreamWriter(_output) {
      @Override
      public void flush() throws IOException {
        super.flush();
        _output.hflush();
      }
    });
    super.activateOptions();
  }

  protected final void closeWriter() {
    super.closeWriter();
  }

}
