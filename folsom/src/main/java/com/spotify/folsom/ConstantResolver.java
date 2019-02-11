/*
 * Copyright (c) 2019 Spotify AB
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

import com.google.common.collect.ImmutableList;
import com.spotify.folsom.guava.HostAndPort;
import java.util.List;

public class ConstantResolver implements Resolver {
  private final ImmutableList<ResolveResult> results;

  public ConstantResolver(ImmutableList<ResolveResult> results) {
    this.results = results;
  }

  static ConstantResolver fromAddresses(List<HostAndPort> addresses) {
    final long ttl = Long.MAX_VALUE; // can use a very long TTL since the resolver is constant
    final ImmutableList<ResolveResult> results =
        addresses
            .stream()
            .map(
                hostAndPort ->
                    new ResolveResult(hostAndPort.getHostText(), hostAndPort.getPort(), ttl))
            .collect(ImmutableList.toImmutableList());
    return new ConstantResolver(results);
  }

  @Override
  public List<ResolveResult> resolve() {
    return results;
  }
}
