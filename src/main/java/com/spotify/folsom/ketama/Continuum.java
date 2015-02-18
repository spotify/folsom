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

import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.spotify.folsom.RawMemcacheClient;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkNotNull;


public class Continuum {

  private static final int VNODE_RATIO = 100;

  private final TreeMap<Integer, RawMemcacheClient> ringOfFire;

  public Continuum(final Collection<AddressAndClient> clients) {
    this.ringOfFire = buildRing(clients);
  }

  private TreeMap<Integer, RawMemcacheClient> buildRing(
    final Collection<AddressAndClient> clients) {

    final TreeMap<Integer, RawMemcacheClient> r = Maps.newTreeMap();
    for (final AddressAndClient client : clients) {
      final String address = client.getAddress().toString();

      byte[] hash = addressToBytes(address);
      for (int i = 0; i < VNODE_RATIO; i++) {
        final HashCode hashCode = Hasher.hash(hash);
        hash = hashCode.asBytes();
        r.put(hashCode.asInt(), client.getClient());
      }
    }
    return r;
  }

  public RawMemcacheClient findClient(final byte[] key) {
    final int keyHash = Hasher.hash(key).asInt();

    Entry<Integer, RawMemcacheClient> entry = ringOfFire.ceilingEntry(keyHash);
    if (entry == null) {
      // wrap around
      entry = ringOfFire.firstEntry();
    }

    for (int i = 0; i < ringOfFire.size(); i++) {
      // TODO: maybe loop for fewer rounds - what happens if all clients are disconnected?
      final RawMemcacheClient client = findClient(entry);
      if (client.isConnected()) {
        return client;
      }
      entry = ringOfFire.higherEntry(entry.getKey());
      if (entry == null) {
        // wrap around
        entry = ringOfFire.firstEntry();
      }
    }
    return ringOfFire.firstEntry().getValue();
  }

  private RawMemcacheClient findClient(final Entry<Integer, RawMemcacheClient> entry) {
    checkNotNull(entry);
    return entry.getValue();
  }

  private static byte[] addressToBytes(final String s) {
    final int length = s.length();
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) s.charAt(i);
    }
    return bytes;
  }
}
