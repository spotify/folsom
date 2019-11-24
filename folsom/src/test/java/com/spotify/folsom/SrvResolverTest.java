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

import static com.google.common.collect.ImmutableList.of;
import static com.spotify.dns.LookupResult.create;
import static org.junit.Assert.assertEquals;

import com.spotify.dns.DnsSrvResolver;
import com.spotify.folsom.Resolver.ResolveResult;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SrvResolverTest {

  public static final String SRV_RECORD = "srv.record";
  @Mock private DnsSrvResolver dnsSrvResolver;

  @Test
  public void testResolve() {
    Mockito.when(dnsSrvResolver.resolve(SRV_RECORD))
        .thenReturn(of(create("host1", 1, 2, 3, 4), create("host2", 5, 6, 7, 8)));

    final SrvResolver resolver =
        SrvResolver.newBuilder(SRV_RECORD).withSrvResolver(dnsSrvResolver).build();

    final List<ResolveResult> result = resolver.resolve();

    assertEquals(of(new ResolveResult("host1", 1, 4), new ResolveResult("host2", 5, 8)), result);
  }
}
