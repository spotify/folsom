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

import com.google.common.annotations.VisibleForTesting;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractRawMemcacheClient implements RawMemcacheClient {
  private final Set<ConnectionChangeListener> listeners = ConcurrentHashMap.newKeySet();

  @Override
  public void registerForConnectionChanges(final ConnectionChangeListener listener) {
    listeners.add(listener);
    listener.connectionChanged(this);
  }

  @Override
  public void unregisterForConnectionChanges(final ConnectionChangeListener listener) {
    listeners.remove(listener);
  }

  @Override
  public final void notifyConnectionChange() {
    for (final ConnectionChangeListener listener : listeners) {
      try {
        listener.connectionChanged(this);
      } catch (Exception e) {
        // We can't really do anything about this
      }
    }
  }

  @VisibleForTesting
  protected int numListeners() {
    return listeners.size();
  }
}
