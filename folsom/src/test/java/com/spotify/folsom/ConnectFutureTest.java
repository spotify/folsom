package com.spotify.folsom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.spotify.folsom.client.test.FakeRawMemcacheClient;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

public class ConnectFutureTest {
  @Test
  public void testUnregister() {
    FakeRawMemcacheClient client = new FakeRawMemcacheClient();
    client.shutdown();
    assertEquals(0, client.numListeners());

    final CompletableFuture<Void> future =
        ConnectFuture.connectFuture(client).toCompletableFuture();

    assertEquals(1, client.numListeners());
    assertFalse(future.isDone());

    client.notifyConnectionChange();
    assertFalse(future.isDone());

    client.setFailure(new RuntimeException("Error"));
    client.notifyConnectionChange();

    assertTrue(future.isDone());

    assertEquals(0, client.numListeners());
  }
}
