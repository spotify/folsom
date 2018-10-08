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

import static com.spotify.folsom.client.Utils.unwrap;

import com.google.common.base.Charsets;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.ascii.GetRequest;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * If SASL is enabled then ascii protocol won't be allowed at all, so we simply expect a connection closed.
 */
public class AsciiAuthenticationValidator implements Authenticator {

  private static final AsciiAuthenticationValidator INSTANCE
      = new AsciiAuthenticationValidator();

  public static AsciiAuthenticationValidator getInstance() {
    return INSTANCE;
  }

  private static final byte[] EXAMPLE_KEY = "folsom_authentication_validation".getBytes(Charsets.US_ASCII);

  private AsciiAuthenticationValidator() {
  }

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
                throw new CompletionException(unwrap(throwable));
              }
            }))
    );
  }

  @Override
  public void validate(final boolean binary) {
    if (binary) {
      throw new IllegalStateException("Programmer error: wrong validator used");
    }
  }
}
