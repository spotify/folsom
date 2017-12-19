/*
 * Copyright (c) 2015 Spotify AB
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
package com.spotify.folsom;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ConnectFuture
    extends CompletableFuture<Void>
    implements ConnectionChangeListener {

  private final boolean awaitedState;

  /**
   * Create a future that completes once the client reaches the awaited state
   * @param client
   * @param awaitedState
   */
  private ConnectFuture(ObservableClient client, boolean awaitedState) {
    this.awaitedState = awaitedState;
    client.registerForConnectionChanges(this);
    check(client);
  }

  public static CompletionStage<Void> disconnectFuture(ObservableClient client) {
    return new ConnectFuture(client, false);
  }

  public static CompletionStage<Void> connectFuture(ObservableClient client) {
    return new ConnectFuture(client, true);
  }

  @Override
  public void connectionChanged(ObservableClient client) {
    check(client);
  }

  private void check(ObservableClient client) {
    if (awaitedState == client.isConnected()) {
      if (complete(null)) {
        client.unregisterForConnectionChanges(this);
      }
    }
  }
}
