package com.spotify.folsom;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.thimbleware.jmemcached.protocol.text.MemcachedResponseEncoder;
import io.netty.channel.nio.NioEventLoop;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.LoggerFactory;

public class ReconnectStressTest {

  public static final int NUM_THREADS = 1;

  private static final Logger log = (Logger) LoggerFactory.getLogger(ReconnectStressTest.class);
  private static final Random random = new Random();

  public static void main(String[] args) throws TimeoutException, InterruptedException {
    long t1 = System.currentTimeMillis();
    setLogLevel(NioEventLoop.class, Level.INFO);
    setLogLevel(MemcachedResponseEncoder.class, Level.OFF);

    int port = EmbeddedServer.findFreePort();
    EmbeddedServer server = new EmbeddedServer(false, port);

    AsciiMemcacheClient<String> client =
        MemcacheClientBuilder.newStringClient()
            .withAddress("localhost", port)
            .withConnections(16)
            .withRetry(true)
            .withMaxOutstandingRequests(1000)
            .connectAscii();

    client.awaitConnected(30, TimeUnit.SECONDS);

    AtomicInteger success = new AtomicInteger(0);
    ConcurrentMap<Class<?>, AtomicInteger> failures = new ConcurrentHashMap<>();
    ExecutorService executorService =
        Executors.newFixedThreadPool(
            NUM_THREADS, new ThreadFactoryBuilder().setDaemon(true).build());

    AtomicLong lastSuccess = new AtomicLong(System.currentTimeMillis());

    for (int i = 0; i < NUM_THREADS; i++) {
      executorService.submit(
          () -> {
            while (true) {
              client
                  .get("key")
                  .handle(
                      (s, throwable) -> {
                        if (throwable == null) {
                          lastSuccess.set(System.currentTimeMillis());
                          success.incrementAndGet();
                        } else {
                          if (throwable instanceof CompletionException) {
                            throwable = throwable.getCause();
                          }
                          AtomicInteger failure = failures.get(throwable.getClass());
                          if (failure == null) {
                            failures.putIfAbsent(throwable.getClass(), new AtomicInteger());
                            failure = failures.get(throwable.getClass());
                          }
                          failure.incrementAndGet();
                        }
                        try {
                          Thread.sleep(1);
                        } catch (InterruptedException e) {
                          e.printStackTrace();
                        }
                        return null;
                      });
            }
          });
    }

    while (true) {
      int numSuccesses = success.get();
      success.addAndGet(-numSuccesses);
      StringBuilder sb = new StringBuilder();
      for (Class<?> key : failures.keySet()) {
        int numFailures = failures.get(key).get();
        failures.get(key).addAndGet(-numFailures);
        sb.append(", " + key.getSimpleName() + ": " + numFailures + "");
      }
      log.info("Success: " + numSuccesses + sb.toString());
      log.info("Stopping server");
      server.stop();
      long timestamp = lastSuccess.get();
      long t2 = System.currentTimeMillis();
      log.info("Total test time: " + Duration.ofMillis(t2 - t1).toString());
      if (t2 - timestamp > 20000) {
        throw new RuntimeException("Could not manage to reconnect");
      }
      doSleep(100);
      log.info("Starting server");
      server.start();
      doSleep(100);
    }
  }

  private static void doSleep(int maxTime) throws InterruptedException {
    int time = random.nextInt(maxTime);
    log.info("Sleeping for " + time + " ms");
    Thread.sleep(time);
  }

  private static void setLogLevel(Class<?> clazz, Level level) {
    Logger logger = (Logger) LoggerFactory.getLogger(clazz);
    logger.setLevel(level);
  }
}
