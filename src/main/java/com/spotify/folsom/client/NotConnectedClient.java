/*
 * Copyright (c) 2014-2015 Spotify AB
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
package com.spotify.folsom.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.AbstractRawMemcacheClient;
import com.spotify.folsom.MemcacheClosedException;

public class NotConnectedClient extends AbstractRawMemcacheClient {

  public static final NotConnectedClient INSTANCE = new NotConnectedClient();

  private NotConnectedClient() {
  }

  @Override
  public <T> ListenableFuture<T> send(final Request<T> request) {
    return fail();
  }

  @Override
  public void shutdown() {
    notifyConnectionChange();
  }

  @Override
  public boolean isConnected() {
    return false;
  }

  @Override
  public int numTotalConnections() {
    return 1;
  }

  @Override
  public int numActiveConnections() {
    return 0;
  }

  private <T> ListenableFuture<T> fail() {
    return Futures.immediateFailedFuture(new MemcacheClosedException("Not connected"));
  }
}
