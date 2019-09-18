package pack.iscsi.wal.remote.thrift;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import pack.iscsi.wal.remote.generated.FetchJournalEntriesRequest;
import pack.iscsi.wal.remote.generated.FetchJournalEntriesResponse;
import pack.iscsi.wal.remote.generated.JournalRangeRequest;
import pack.iscsi.wal.remote.generated.JournalRangeResponse;
import pack.iscsi.wal.remote.generated.PackException;
import pack.iscsi.wal.remote.generated.PackWalService;
import pack.iscsi.wal.remote.generated.ReleaseRequest;
import pack.iscsi.wal.remote.generated.WriteRequest;

public class PackWalServiceClientImpl extends PackWalService.Client implements PackWalServiceClient {

  private final AtomicLong _lastSuccessfulUse = new AtomicLong(System.currentTimeMillis());
  private final String _str;
  private final String _hostname;
  private final int _port;

  public PackWalServiceClientImpl(String hostname, int port, TProtocol prot, String str) {
    super(prot);
    _str = str;
    _hostname = hostname;
    _port = port;
  }

  public String getHostname() {
    return _hostname;
  }

  public int getPort() {
    return _port;
  }

  public long getLastSuccessfulUse() {
    return _lastSuccessfulUse.get();
  }

  @Override
  public void ping() throws PackException, TException {
    super.ping();
    successfulUse();
  }

  @Override
  public void write(WriteRequest writeRequest) throws PackException, TException {
    super.write(writeRequest);
    successfulUse();
  }

  @Override
  public void releaseJournals(ReleaseRequest releaseRequest) throws PackException, TException {
    super.releaseJournals(releaseRequest);
    successfulUse();
  }

  @Override
  public JournalRangeResponse journalRanges(JournalRangeRequest journalRangeRequest) throws PackException, TException {
    JournalRangeResponse journalRanges = super.journalRanges(journalRangeRequest);
    successfulUse();
    return journalRanges;
  }

  @Override
  public FetchJournalEntriesResponse fetchJournalEntries(FetchJournalEntriesRequest fetchJournalEntriesRequest)
      throws PackException, TException {
    FetchJournalEntriesResponse fetchJournalEntries = super.fetchJournalEntries(fetchJournalEntriesRequest);
    successfulUse();
    return fetchJournalEntries;
  }

  @Override
  public String toString() {
    return _str;
  }

  private void successfulUse() {
    _lastSuccessfulUse.set(System.currentTimeMillis());
  }

  public void closeTransport() {
    getInputProtocol().getTransport();
  }

  @Override
  public void close() throws IOException {

  }

}
