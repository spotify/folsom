/*
 * Copyright (c) 2014-2019 Spotify AB
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

import static java.util.Objects.requireNonNull;

import com.google.common.base.Suppliers;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.DnsSrvResolvers;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SrvResolver implements Resolver {

  private static final Supplier<DnsSrvResolver> DEFAULT_DNS_RESOLVER =
      Suppliers.memoize(
              () ->
                  DnsSrvResolvers.newBuilder()
                      .cachingLookups(true)
                      .retainingDataOnFailures(true)
                      .build())
          ::get;

  public static class Builder {
    private final String srvRecord;
    private DnsSrvResolver srvResolver;

    private Builder(final String srvRecord) {
      this.srvRecord = requireNonNull(srvRecord, "srvRecord");
    }

    /**
     * Use a specific DNS resolver.
     *
     * @param srvResolver the resolver to use. Default is a caching resolver from {@link
     *     com.spotify.dns.DnsSrvResolvers}
     * @return itself
     */
    public Builder withSrvResolver(final DnsSrvResolver srvResolver) {
      this.srvResolver = requireNonNull(srvResolver, "srvResolver");
      return this;
    }

    public SrvResolver build() {
      DnsSrvResolver srvResolver = this.srvResolver;
      if (srvResolver == null) {
        srvResolver = DEFAULT_DNS_RESOLVER.get();
      }

      return new SrvResolver(srvResolver, srvRecord);
    }
  }

  public static Builder newBuilder(final String srvRecord) {
    return new Builder(srvRecord);
  }

  private final DnsSrvResolver dnsSrvResolver;
  private final String srvRecord;

  private SrvResolver(final DnsSrvResolver dnsSrvResolver, final String srvRecord) {
    this.dnsSrvResolver = dnsSrvResolver;
    this.srvRecord = srvRecord;
  }

  @Override
  public List<ResolveResult> resolve() {
    return dnsSrvResolver
        .resolve(srvRecord)
        .stream()
        .map(result -> new ResolveResult(result.host(), result.port(), result.ttl()))
        .collect(Collectors.toList());
  }
}
