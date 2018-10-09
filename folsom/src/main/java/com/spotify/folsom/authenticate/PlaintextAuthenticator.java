/*
 * Copyright (c) 2018 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.folsom.authenticate;

import static com.google.common.base.Preconditions.checkNotNull;

import com.spotify.folsom.MemcacheAuthenticationException;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.binary.PlaintextAuthenticateRequest;
import java.util.concurrent.CompletionStage;

public class PlaintextAuthenticator implements Authenticator {

  private final String username;
  private final String password;

  public PlaintextAuthenticator(final String username, final String password) {
    this.username = checkNotNull(username);
    this.password = checkNotNull(password);
  }

  public CompletionStage<RawMemcacheClient> authenticate(
      final CompletionStage<RawMemcacheClient> clientFuture) {

    final PlaintextAuthenticateRequest
        authenticateRequest = new PlaintextAuthenticateRequest(username, password);

    return clientFuture.thenCompose(
        client -> client.connectFuture().thenCompose(ignored ->
            client.send(authenticateRequest)
                .thenApply(status -> {
                  if (status == MemcacheStatus.OK) {
                    return client;
                  } else if (status == MemcacheStatus.UNAUTHORIZED) {
                    throw new MemcacheAuthenticationException("Authentication failed");
                  } else {
                    throw new RuntimeException("Unexpected status: " + status.name());
                  }
                })
        )
    );
  }

  @Override
  public void validate(final boolean binary) {
    if (!binary) {
      throw new IllegalArgumentException("Authentication can only be used for binary clients.");
    }
  }
}
