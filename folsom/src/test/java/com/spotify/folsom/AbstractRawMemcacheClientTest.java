package com.spotify.folsom;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.spotify.folsom.client.Request;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

public class AbstractRawMemcacheClientTest {

  @Test
  public void testRegisterAndUnregister() {
    verifyClientNotifications(new TestRawMemcacheClient());
  }

  public static void verifyClientNotifications(ObservableClient client) {
    final ConnectionChangeListener listener = mock(ConnectionChangeListener.class);

    client.registerForConnectionChanges(listener);
    verify(listener, times(1)).connectionChanged(any());

    client.notifyConnectionChange();
    verify(listener, times(2)).connectionChanged(any());

    client.unregisterForConnectionChanges(listener);
    client.notifyConnectionChange();
    verify(listener, times(2)).connectionChanged(any());
  }

  private static class TestRawMemcacheClient extends AbstractRawMemcacheClient {
    @Override
    public <T> CompletionStage<T> send(Request<T> request) {
      throw new RuntimeException("Not implemented");
    }

    @Override
    public void shutdown() {}

    @Override
    public boolean isConnected() {
      return false;
    }

    @Override
    public Throwable getConnectionFailure() {
      return null;
    }

    @Override
    public int numTotalConnections() {
      return 0;
    }

    @Override
    public int numActiveConnections() {
      return 0;
    }

    @Override
    public int numPendingRequests() {
      return 0;
    }
  }
}
