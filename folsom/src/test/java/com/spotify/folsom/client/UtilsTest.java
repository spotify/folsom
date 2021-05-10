package com.spotify.folsom.client;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Test;

public class UtilsTest {
  @Test
  public void testTtlToExpiration() {
    assertEquals(0, Utils.ttlToExpiration(-123123));
    assertEquals(0, Utils.ttlToExpiration(0));
    assertEquals(123, Utils.ttlToExpiration(123));

    assertEquals(2591999, Utils.ttlToExpiration(2591999));

    // Converted to timestamp
    assertTimestamp(2592000);
    assertTimestamp(3000000);

    // Overflows the timestamp
    assertEquals(Integer.MAX_VALUE - 1, Utils.ttlToExpiration(Integer.MAX_VALUE - 1234));
  }

  private void assertTimestamp(int ttl) {
    double expectedTimestamp = (System.currentTimeMillis() / 1000.0) + ttl;
    assertEquals(expectedTimestamp, Utils.ttlToExpiration(ttl), 2.0);
  }

  @Test
  public void testOnExecutorOk() throws ExecutionException, InterruptedException {
    ExecutorService executorA =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("thread-A-%d").setDaemon(true).build());
    ExecutorService executorB =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("thread-B-%d").setDaemon(true).build());
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<String> future2 =
        Utils.onExecutor(future, executorB)
            .toCompletableFuture()
            .thenApply(s -> Thread.currentThread().getName());
    executorA.submit(
        () -> {
          future.complete(Thread.currentThread().getName());
        });
    assertEquals("thread-B-0", future2.get());
  }

  @Test
  public void testOnExecutorException() throws ExecutionException, InterruptedException {
    ExecutorService executorA =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("thread-A-%d").setDaemon(true).build());
    ExecutorService executorB =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("thread-B-%d").setDaemon(true).build());
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<String> future2 =
        Utils.onExecutor(future, executorB)
            .toCompletableFuture()
            .exceptionally(s -> Thread.currentThread().getName());
    executorA.submit(
        () -> {
          future.completeExceptionally(new RuntimeException());
        });
    assertEquals("thread-B-0", future2.get());
  }

  @Test
  public void testZipToMap() {
    assertEquals(
        ImmutableMap.of("a", 1, "b", 2),
        Utils.zipToMap(Arrays.asList("a", "b"), Arrays.asList(1, 2)));
    assertEquals(
        ImmutableMap.of("a", 1, "b", 2),
        Utils.zipToMap(Arrays.asList("a", "b", "c"), Arrays.asList(1, 2, null)));
    assertEquals(
        ImmutableMap.of("a", 1, "b", 2),
        Utils.zipToMap(Arrays.asList("a", "b", null), Arrays.asList(1, 2, 3)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testZipToMapThrows() {
    Utils.zipToMap(Arrays.asList("a", "b"), Arrays.asList(1));
  }
}
