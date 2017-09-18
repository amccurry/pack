package pack.block.server.admin;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;

import pack.block.server.BlockPackFuse;
import pack.block.util.Utils;
import spark.ResponseTransformer;
import spark.Service;
import spark.SparkJava;
import spark.SparkJavaIdentifier;

public class BlockPackAdmin {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackAdmin.class);

  public static final String PID = "/pid";
  public static final String STATUS = "/status";
  public static final String UMOUNT = "/umount";
  public static final String COUNTER = "/counter";
  public static final String SHUTDOWN = "/shutdown";
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final ResponseTransformer TRANSFORMER = model -> {
    if (model == null) {
      return "";
    } else if (model instanceof String) {
      return model.toString();
    }
    return OBJECT_MAPPER.writeValueAsString(model);
  };

  public static BlockPackAdmin startAdminServer(String sockFile) {
    return new BlockPackAdmin(sockFile);
  }

  private final AtomicReference<Status> _currentStatus = new AtomicReference<Status>(Status.UNKNOWN);
  private final AtomicReference<BlockPackFuse> _blockPackFuse = new AtomicReference<BlockPackFuse>();
  private final AtomicReference<String> _currentStatusMessage = new AtomicReference<String>();
  private final String _pid;
  private final AtomicBoolean _shutDown = new AtomicBoolean(false);
  private final Map<String, AtomicLong> _counter = new ConcurrentHashMap<>();

  private BlockPackAdmin(String sockFile) {
    _pid = getPid();

    SparkJava.init();
    Service service = Service.ignite();
    SparkJava.configureService(SparkJavaIdentifier.UNIX_SOCKET, service);
    service.ipAddress(sockFile);

    service.get(PID, (request, response) -> PidResponse.builder()
                                                       .pid(_pid)
                                                       .build(),
        TRANSFORMER);

    service.get(STATUS, (request, response) -> StatusResponse.builder()
                                                             .status(_currentStatus.get())
                                                             .build(),
        TRANSFORMER);

    service.post(UMOUNT, (request, response) -> {
      LOGGER.info("umount request");
      BlockPackFuse blockPackFuse = _blockPackFuse.get();
      if (blockPackFuse != null) {
        new Thread(() -> {
          Utils.close(LOGGER, blockPackFuse);
        }).start();
      }
      waitForStatusToChange(Status.FS_UMOUNT_COMPLETE, Status.FS_UMOUNT_STARTED);
      return UnmountResponse.builder()
                            .build();
    }, TRANSFORMER);

    service.post(COUNTER, (request, response) -> {
      LOGGER.info("counter request");
      LOGGER.info("current counters {}", _counter);
      CounterRequest counterRequest = OBJECT_MAPPER.readValue(request.bodyAsBytes(), CounterRequest.class);
      String name = counterRequest.getName();
      if (!_counter.containsKey(name)) {
        LOGGER.info("counter {} not found adding new entry", name);
        _counter.put(name, new AtomicLong());
      }
      AtomicLong counter = _counter.get(name);
      LOGGER.info("current counter {} value {}", name, counter);
      LOGGER.info("counterRequest {} ", counterRequest);
      CounterAction action = counterRequest.getAction();
      switch (action) {
      case PUT:
        counter.set(counterRequest.getValue());
        break;
      case INCREMENT:
        counter.incrementAndGet();
        break;
      case DECREMENT:
        counter.decrementAndGet();
        break;
      default:
        break;
      }

      CounterResponse counterResponse = CounterResponse.builder()
                                                       .name(name)
                                                       .value(counter.get())
                                                       .build();
      LOGGER.info("counterResponse {} ", counterResponse);
      return counterResponse;
    }, TRANSFORMER);

    service.post(SHUTDOWN, (request, response) -> {
      LOGGER.info("shutdown request");
      synchronized (_shutDown) {
        if (_shutDown.get()) {
          return ShutdownResponse.builder()
                                 .build();
        }
        _shutDown.set(true);
      }
      LOGGER.info("shutdown being processed");
      new Thread(() -> {
        BlockPackFuse blockPackFuse = _blockPackFuse.get();
        if (blockPackFuse != null) {
          Utils.close(LOGGER, blockPackFuse);
        }
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
          LOGGER.info("Unknown error", e);
        }
        System.exit(0);
      }).start();
      return ShutdownResponse.builder()
                             .build();
    }, TRANSFORMER);
  }

  private void waitForStatusToChange(Status... statusList) throws InterruptedException {
    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
    while (true) {
      Status status = _currentStatus.get();
      for (Status s : statusList) {
        if (s == status) {
          return;
        }
      }
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
  }

  public static String getPid() {
    List<String> list = Splitter.on('@')
                                .splitToList(ManagementFactory.getRuntimeMXBean()
                                                              .getName());

    return list.get(0);
  }

  public void setStatus(Status status) {
    setStatus(status, null);
  }

  public void setStatus(Status status, String message) {
    _currentStatus.set(status);
    _currentStatusMessage.set(message);
  }

  public BlockPackFuse register(BlockPackFuse blockPackFuse) {
    _blockPackFuse.set(blockPackFuse);
    return blockPackFuse;
  }

}
