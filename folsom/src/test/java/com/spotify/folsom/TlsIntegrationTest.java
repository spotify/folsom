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


import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.tls.DefaultSSLEngineFactory;
import java.util.*;
import org.junit.BeforeClass;

public class TlsIntegrationTest extends AbstractIntegrationTestBase {
  @BeforeClass
  public static void setUpClass() throws Exception {
    server = new MemcachedServer(true);
  }

  protected MemcacheClientBuilder<String> createClientBuilder() throws Exception {
    MemcacheClientBuilder<String> builder =
        MemcacheClientBuilder.newStringClient()
            .withAddress(server.getHost(), server.getPort())
            .withConnections(1)
            .withMaxOutstandingRequests(1000)
            .withMetrics(NoopMetrics.INSTANCE)
            .withRetry(false)
            .withSSLEngineFactory(new DefaultSSLEngineFactory(true))
            .withRequestTimeoutMillis(100);
    return builder;
  }
}
