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

import static com.spotify.folsom.AbstractRawMemcacheClientTest.verifyClientNotifications;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.Utils;
import com.spotify.folsom.client.tls.DefaultSSLEngineFactory;
import com.spotify.futures.CompletableFutures;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import junit.framework.AssertionFailedError;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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