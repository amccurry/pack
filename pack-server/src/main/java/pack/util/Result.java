package pack.util;

public class Result {

  public final int exitCode;
  public final String stdout;
  public final String stderr;

  public Result(int exitCode, String stdout, String stderr) {
    this.exitCode = exitCode;
    this.stdout = stdout;
    this.stderr = stderr;
  }
}
