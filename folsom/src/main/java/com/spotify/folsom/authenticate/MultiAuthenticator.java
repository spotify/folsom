package com.spotify.folsom.authenticate;

import com.spotify.folsom.MemcacheAuthenticationException;
import com.spotify.folsom.RawMemcacheClient;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class MultiAuthenticator implements Authenticator {
  private final List<? extends Authenticator> authenticators;

  public MultiAuthenticator(List<? extends Authenticator> authenticators) {
    this.authenticators = authenticators;
    if (authenticators.isEmpty()) {
      throw new IllegalStateException("Must not have an empty list of authenticators");
    }
  }

  @Override
  public CompletionStage<RawMemcacheClient> authenticate(RawMemcacheClient client) {
    final Iterator<? extends Authenticator> iterator = authenticators.iterator();
    CompletionStage<RawMemcacheClient> current = iterator.next().authenticate(client);

    while (iterator.hasNext()) {
      final Authenticator authenticator = iterator.next();
      current =
          current
              .handle(
                  (ignore, throwable) -> {
                    if (throwable == null) {
                      return CompletableFuture.completedFuture(client);
                    }
                    if (throwable.getCause() instanceof MemcacheAuthenticationException) {
                      return authenticator.authenticate(client);
                    }
                    CompletableFuture<RawMemcacheClient> result = new CompletableFuture<>();
                    result.completeExceptionally(throwable);
                    return result;
                  })
              .thenCompose(stage -> stage);
    }
    return current;
  }

  @Override
  public void validate(boolean binary) {
    authenticators.forEach(authenticator -> authenticator.validate(binary));
  }
}
