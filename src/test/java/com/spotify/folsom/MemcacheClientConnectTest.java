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

package com.spotify.folsom;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;

import org.junit.Test;

public class MemcacheClientConnectTest {

  @Test(expected = MemcacheClosedException.class)
  public void testConnectFails() throws Throwable {
    final BinaryMemcacheClient<byte[]> client = MemcacheClientBuilder.newByteArrayClient()
        .withAddress(HostAndPort.fromParts("dummy.dummy", 56742))
        .connectBinary();
    Futures.getChecked(client.get("foo"), MemcacheClosedException.class);
  }

  @Test(expected = MemcacheClosedException.class)
  public void testConnectPort() throws Throwable {
    final BinaryMemcacheClient<byte[]> client = MemcacheClientBuilder.newByteArrayClient()
        .withAddress(HostAndPort.fromParts("127.0.0.1", 56742))
        .connectBinary();
    Futures.getChecked(client.get("foo"), MemcacheClosedException.class);
  }
}
