package pack.iscsi.bk.wal.status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.LedgerMetadata.State;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Versioned;

import io.opencensus.common.Scope;
import lombok.Builder;
import lombok.Value;
import pack.iscsi.bk.wal.BookKeeperWriteAheadLog;
import pack.util.TracerUtil;
import spark.ModelAndView;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Service;
import spark.template.freemarker.FreeMarkerEngine;

public class BookKeeperStatus {

  private static final FreeMarkerEngine ENGINE = new FreeMarkerEngine();

  @Value
  @Builder(toBuilder = true)
  public static class BookKeeperStatusConfig {

    BookKeeperWriteAheadLog bookKeeperWriteAheadLog;
    Service service;

  }

  private final BookKeeperWriteAheadLog _bookKeeperWriteAheadLog;
  private final Service _service;

  public BookKeeperStatus(BookKeeperStatusConfig config) {
    _bookKeeperWriteAheadLog = config.getBookKeeperWriteAheadLog();
    _service = config.getService();
  }

  @Value
  @Builder(toBuilder = true)
  public static class BookieInfoEntry {
    String hostName;
    int port;
    long freeDiskSpace;
    long totalDiskSpace;
    long weight;
  }

  @Value
  @Builder(toBuilder = true)
  public static class LedgerMetadataEntry {
    long ledgerId;
    int ackQuorumSize;
    String allEnsembles;
    long ctime;
    String customMetadata;
    String digestType;
    int ensembleSize;
    long lastEntryId;
    long length;
    int metadataFormatVersion;
    State state;
    int writeQuorumSize;
  }

  public void setup() {
    BookKeeper bookKeeper = _bookKeeperWriteAheadLog.getBookKeeper();
    _service.get("/bk/bookies", (request, response) -> {
      Map<BookieSocketAddress, BookieInfo> bookieInfoMap = bookKeeper.getBookieInfo();
      List<BookieInfoEntry> entries = new ArrayList<>();
      for (Entry<BookieSocketAddress, BookieInfo> e : bookieInfoMap.entrySet()) {
        BookieSocketAddress socketAddress = e.getKey();
        BookieInfo bookieInfo = e.getValue();
        entries.add(BookieInfoEntry.builder()
                                   .freeDiskSpace(bookieInfo.getFreeDiskSpace())
                                   .hostName(socketAddress.getHostName())
                                   .port(socketAddress.getPort())
                                   .totalDiskSpace(bookieInfo.getTotalDiskSpace())
                                   .weight(bookieInfo.getWeight())
                                   .build());
      }

      Map<String, Object> attributes = new HashMap<>();
      attributes.put("bookieInfoList", entries);
      return new ModelAndView(attributes, "bookies.ftl");
    }, ENGINE);

    _service.get("/bk/ledgers", (request, response) -> {
      List<LedgerMetadataEntry> entries = getEntries(bookKeeper);
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("ledgerMetadataList", entries);
      return new ModelAndView(attributes, "ledgers.ftl");
    }, ENGINE);

    _service.delete("/bk/ledgers/:id", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        String id = request.params(":id");
        bookKeeper.deleteLedger(Long.parseLong(id));
        response.redirect("/bk/ledgers");
        return null;
      }
    });

    _service.post("/bk/ledgers/delete/:id", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        String id = request.params(":id");
        bookKeeper.deleteLedger(Long.parseLong(id));
        response.redirect("/bk/ledgers");
        return null;
      }
    });

  }

  private List<LedgerMetadataEntry> getEntries(BookKeeper bookKeeper)
      throws IOException, InterruptedException, ExecutionException {
    try (Scope ranges = TracerUtil.trace("get ledger ranges")) {
      LedgerRangeIterator ledgerRanges = bookKeeper.getLedgerManager()
                                                   .getLedgerRanges();
      Map<Long, CompletableFuture<Versioned<LedgerMetadata>>> futures = new HashMap<>();
      while (ledgerRanges.hasNext()) {
        LedgerRange range = ledgerRanges.next();
        try (Scope ledgerids = TracerUtil.trace("get ledgerids")) {
          Set<Long> ledgerIds = range.getLedgers();
          for (Long ledgerId : ledgerIds) {
            try (Scope entryTracer = TracerUtil.trace("get entry future")) {
              futures.put(ledgerId, bookKeeper.getLedgerManager()
                                              .readLedgerMetadata(ledgerId));
            }
          }
        }
      }

      List<LedgerMetadataEntry> entries = new ArrayList<>();
      for (Entry<Long, CompletableFuture<Versioned<LedgerMetadata>>> future : futures.entrySet()) {
        try {
          CompletableFuture<Versioned<LedgerMetadata>> completableFuture = future.getValue();

          Versioned<LedgerMetadata> versioned = completableFuture.get();
          LedgerMetadata metadata = versioned.getValue();
          if (metadata != null) {
            entries.add(getEntry(future.getKey(), metadata));
          }
        } catch (ExecutionException e) {
          if (e.getCause() instanceof BKNoSuchLedgerExistsException) {
            continue;
          }
          throw e;
        }
      }
      return entries;
    }
  }

  private LedgerMetadataEntry getEntry(Long ledgerId, LedgerMetadata metadata) {
    int ackQuorumSize = metadata.getAckQuorumSize();
    NavigableMap<Long, ? extends List<BookieSocketAddress>> allEnsembles = metadata.getAllEnsembles();
    long ctime = metadata.getCtime();
    Map<String, byte[]> customMetadata = metadata.getCustomMetadata();
    DigestType digestType = metadata.getDigestType();
    int ensembleSize = metadata.getEnsembleSize();
    long lastEntryId = metadata.getLastEntryId();
    long length = metadata.getLength();
    int metadataFormatVersion = metadata.getMetadataFormatVersion();
    State state = metadata.getState();
    int writeQuorumSize = metadata.getWriteQuorumSize();

    LedgerMetadataEntry entry = LedgerMetadataEntry.builder()
                                                   .ledgerId(ledgerId)
                                                   .ackQuorumSize(ackQuorumSize)
                                                   .allEnsembles(allEnsembles.toString())
                                                   .ctime(ctime)
                                                   .customMetadata(customMetadata.toString())
                                                   .digestType(digestType.name())
                                                   .ensembleSize(ensembleSize)
                                                   .lastEntryId(lastEntryId)
                                                   .length(length)
                                                   .metadataFormatVersion(metadataFormatVersion)
                                                   .state(state)
                                                   .writeQuorumSize(writeQuorumSize)
                                                   .build();
    return entry;
  }

}
