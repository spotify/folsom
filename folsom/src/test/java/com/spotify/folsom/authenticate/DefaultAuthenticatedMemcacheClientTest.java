package com.spotify.folsom.authenticate;

import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheClientBuilder;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Transcoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class DefaultAuthenticatedMemcacheClientTest {

  @Test
  public void testConnect() throws InterruptedException, ExecutionException {
    final String hostname = "127.0.0.1";
    final int port = 3000;
    final String username = "memcached";
    final String password = "foobar";

    Authenticator authenticator = new PlaintextAuthenticator(username, password);
    MemcacheClient<String> client = new MemcacheClientBuilder<>(createProtobufTranscoder())
        .withAddress(hostname, port)
        .connectAuthenticated(username, password);

    Thread.sleep(2000);

    MemcacheStatus status = client.add("foo", "bar", 1)
        .toCompletableFuture()
        .get();

    System.out.println(status);
  }

  private Transcoder<String> createProtobufTranscoder() {
    return new Transcoder<String>() {
      @Override
      public String decode(final byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
      }

      @Override
      public byte[] encode(final String entity) {
        return entity.getBytes(StandardCharsets.UTF_8);
      }
    };
  }
}
