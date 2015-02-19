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

import com.google.common.eventbus.EventBus;

public abstract class AbstractRawMemcacheClient implements RawMemcacheClient {
  private final EventBus eventBus = new EventBus("folsom eventbus");

  @Override
  public void registerForConnectionChanges(ConnectionChangeListener listener) {
    eventBus.register(listener);
    listener.connectionChanged(this);
  }

  @Override
  public void unregisterForConnectionChanges(ConnectionChangeListener listener) {
    eventBus.unregister(listener);
  }

  protected void notifyConnectionChange() {
    eventBus.post(this);
  }

}
