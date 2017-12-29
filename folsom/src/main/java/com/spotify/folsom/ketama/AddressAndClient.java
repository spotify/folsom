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

package com.spotify.folsom.ketama;

import com.spotify.folsom.guava.HostAndPort;
import com.spotify.folsom.RawMemcacheClient;


public class AddressAndClient {

  private final HostAndPort address;
  private final RawMemcacheClient client;

  public AddressAndClient(final HostAndPort address, final RawMemcacheClient client) {
    this.address = address;
    this.client = client;
  }

  public HostAndPort getAddress() {
    return address;
  }

  public RawMemcacheClient getClient() {
    return client;
  }
}
