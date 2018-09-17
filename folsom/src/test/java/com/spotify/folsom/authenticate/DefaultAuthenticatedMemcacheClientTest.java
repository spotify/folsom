package com.spotify.folsom.authenticate;

import static com.spotify.folsom.MemcacheStatus.OK;
import static com.spotify.hamcrest.future.CompletableFutureMatchers.stageWillCompleteWithExceptionThat;
import static com.spotify.hamcrest.future.CompletableFutureMatchers.stageWillCompleteWithValueThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheClientBuilder;
import com.spotify.folsom.MemcachedServer;
import com.spotify.folsom.Transcoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DefaultAuthenticatedMemcacheClientTest {
  @Rule
  public ExpectedException thrown= ExpectedException.none();

  private static final String USERNAME = "a_nice_password";
  private static final String PASSWORD = "a_nice_password";

  private static MemcachedServer server;

  @BeforeClass
  public static void setUpClass() {
    server = new MemcachedServer(USERNAME, PASSWORD);
  }

  @AfterClass
  public static void tearDownClass() {
    server.stop();
  }

  @Test
  public void testAuthenticateAndSet() throws InterruptedException {
    Authenticator authenticator = new PlaintextAuthenticator(USERNAME, PASSWORD);
    MemcacheClient<String> client = new MemcacheClientBuilder<>(createProtobufTranscoder())
        .withAddress(server.getHost(), server.getPort())
        .connectAuthenticated(authenticator);

    // client needs a a roundtrip to get an answer to our auth request
    Thread.sleep(2000);

    assertThat(client.set("some_key", "some_val", 1).toCompletableFuture(),
        stageWillCompleteWithValueThat(is(OK)));
  }

  @Test
  public void unAuthorizedClientFails() throws InterruptedException, ExecutionException {
    MemcacheClient<String> client = new MemcacheClientBuilder<>(createProtobufTranscoder())
        .withAddress(server.getHost(), server.getPort())
        .connectBinary();

    Thread.sleep(2000);

    thrown.expectMessage("Unexpected response: UNAUTHORIZED");
    client.get("someKey").toCompletableFuture().get();
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
