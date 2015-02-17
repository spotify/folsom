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

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.client.Utils;
import com.spotify.folsom.transcoder.StringTranscoder;

import java.nio.charset.Charset;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LoadTestRunner {

  public static void main(final String[] args) throws Throwable {
    final BinaryMemcacheClient<String> client =
            new MemcacheClientBuilder<>(StringTranscoder.UTF8_INSTANCE)
                    .withAddress("127.0.0.1")
                    .withMaxOutstandingRequests(100000)
                    .connectBinary();

    final String[] keys = new String[10];
    for (int i = 0; i<10; i++) {
      keys[i] = "key" + i;
    }



    final List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    for (int r = 0; r < 100; r++) {
      for (final String keyProto : keys) {
        final String key = keyProto + ":" + r;

        final ListenableFuture<MemcacheStatus> setFuture = client.set(key, "value" + key, 100000);
        final ListenableFuture<String> getFuture = Utils.transform(setFuture, new AsyncFunction<MemcacheStatus, String>() {
          @Override
          public ListenableFuture<String> apply(final MemcacheStatus input) throws Exception {
            return client.get(key);
          }
        });
        final ListenableFuture<String> deleteFuture = Utils.transform(getFuture, new AsyncFunction<String, String>() {
          @Override
          public ListenableFuture<String> apply(final String value) throws Exception {
            return Utils.transform(client.delete(key), new Function<MemcacheStatus, String>() {
              @Override
              public String apply(final MemcacheStatus input) {
                return value;
              }});
          }});

        final ListenableFuture<Boolean> assertFuture = Utils.transform(deleteFuture, new Function<String, Boolean>() {
          @Override
          public Boolean apply(final String input) {
            return ("value" + key).equals(input);
          }});

        futures.add(assertFuture);
      }
    }

    final List<Boolean> asserts = Futures.allAsList(futures).get();

    int failed = 0;
    for (final boolean b : asserts) {
      if (!b) {
        failed++;
      }
    }
    System.out.println(failed + " failed of " + asserts.size());


    client.shutdown();
  }
}
