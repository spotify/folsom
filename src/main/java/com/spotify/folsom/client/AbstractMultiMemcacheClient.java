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

import com.google.common.base.Preconditions;
import com.spotify.folsom.AbstractRawMemcacheClient;
import com.spotify.folsom.ConnectionChangeListener;
import com.spotify.folsom.RawMemcacheClient;

import java.util.Collection;

public abstract class AbstractMultiMemcacheClient
        extends AbstractRawMemcacheClient
        implements ConnectionChangeListener {

  protected final Collection<RawMemcacheClient> clients;

  public AbstractMultiMemcacheClient(final Collection<RawMemcacheClient> clients) {
    Preconditions.checkArgument(!clients.isEmpty(), "clients must not be empty");
    this.clients = clients;
    for (RawMemcacheClient client : clients) {
      client.registerForConnectionChanges(this);
    }
  }


  @Override
  public void shutdown() {
    for (final RawMemcacheClient client : clients) {
      client.shutdown();
    }
  }

  @Override
  public boolean isConnected() {
    for (final RawMemcacheClient client : clients) {
      if (client.isConnected()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int numTotalConnections() {
    int sum = 0;
    for (RawMemcacheClient client : clients) {
      sum += client.numTotalConnections();
    }
    return sum;
  }

  @Override
  public int numActiveConnections() {
    int sum = 0;
    for (RawMemcacheClient client : clients) {
      sum += client.numActiveConnections();
    }
    return sum;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + clients + ")";
  }

  @Override
  public void connectionChanged(RawMemcacheClient client) {
    notifyConnectionChange();
  }
}
