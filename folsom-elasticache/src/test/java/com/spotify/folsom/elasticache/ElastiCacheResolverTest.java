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

package com.spotify.folsom.elasticache;

import static com.google.common.collect.Iterators.cycle;
import static com.spotify.folsom.guava.HostAndPort.fromParts;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

import com.spotify.folsom.Resolver.ResolveResult;
import java.util.Iterator;
import org.junit.Test;

public class ElastiCacheResolverTest {

  @Test
  public void resolve() {
    final Response response =
        new Response(123, asList(fromParts("host1", 11211), fromParts("host2", 11212)));

    final ElastiCacheResolver resolver = new ElastiCacheResolver(() -> response, 124);

    assertEquals(
        asList(new ResolveResult("host1", 11211, 124), new ResolveResult("host2", 11212, 124)),
        resolver.resolve());
  }

  @Test
  public void resolveNewerVersions() {
    final Response response1 = new Response(123, singletonList(fromParts("host1", 11211)));
    final Response response2 = new Response(124, singletonList(fromParts("host2", 11211)));
    final Iterator<Response> responses = cycle(response1, response2);

    final ElastiCacheResolver resolver = new ElastiCacheResolver(() -> responses.next(), 124);

    // version 123 response, no cached result so return resolve result
    assertEquals(singletonList(new ResolveResult("host1", 11211, 124)), resolver.resolve());

    // version 124 response, newer than cached result
    assertEquals(singletonList(new ResolveResult("host2", 11211, 124)), resolver.resolve());

    // version 123 response, return cached result
    assertEquals(singletonList(new ResolveResult("host2", 11211, 124)), resolver.resolve());
  }
}
