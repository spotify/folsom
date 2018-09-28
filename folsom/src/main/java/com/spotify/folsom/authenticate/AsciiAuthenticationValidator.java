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

import com.google.common.base.Charsets;
import com.spotify.folsom.MemcacheAuthenticationException;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.ascii.GetRequest;
import java.util.concurrent.CompletionStage;

public class AsciiAuthenticationValidator implements Authenticator {

  private static final byte[] EXAMPLE_KEY = "a".getBytes(Charsets.UTF_8);

  @Override
  public CompletionStage<RawMemcacheClient> authenticate(
      CompletionStage<RawMemcacheClient> clientFuture) {

    final GetRequest request = new GetRequest(EXAMPLE_KEY, false);

    return clientFuture.thenCompose(
        client -> client.connectFuture().thenCompose(ignored ->
        client.send(request)
            .handle((status, throwable) -> {
              if (throwable == null) {
                return client;
              } else {
                throw new MemcacheAuthenticationException("Authentication failed");
              }
            }))
    );
  }

  @Override
  public void validate(final boolean binary) {
    if (binary) {
      throw new RuntimeException("Programmer error: wrong validator used");
    }
  }
}
