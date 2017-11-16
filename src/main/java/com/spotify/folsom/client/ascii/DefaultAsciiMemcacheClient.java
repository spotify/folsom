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

package com.spotify.folsom.client.ascii;

import com.google.common.collect.Lists;
import com.spotify.folsom.AsciiMemcacheClient;
import com.spotify.folsom.ConnectionChangeListener;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.Transcoder;
import com.spotify.folsom.client.MemcacheEncoder;
import com.spotify.folsom.client.TransformerUtil;
import com.spotify.folsom.client.Utils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The default implementation of {@link com.spotify.folsom.AsciiMemcacheClient}
 *
 * @param <V> The value type for all operations
 */
public class DefaultAsciiMemcacheClient<V> implements AsciiMemcacheClient<V> {

  private final RawMemcacheClient rawMemcacheClient;
  private final Metrics metrics;
  private final Transcoder<V> valueTranscoder;
  private final TransformerUtil<V> transformerUtil;
  private final Charset charset;

  public DefaultAsciiMemcacheClient(final RawMemcacheClient rawMemcacheClient,
                                    final Metrics metrics,
                                    final Transcoder<V> valueTranscoder,
                                    Charset charset) {
    this.rawMemcacheClient = rawMemcacheClient;
    this.metrics = metrics;
    this.valueTranscoder = valueTranscoder;
    this.charset = charset;
    this.transformerUtil = new TransformerUtil<>(valueTranscoder);
  }

  @Override
  public CompletionStage<MemcacheStatus> set(final String key, final V value, final int ttl) {
    checkNotNull(value);

    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.SET, key, charset, valueBytes, ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> set(String key, V value, int ttl, long cas) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.casSet(key, charset, valueBytes, ttl, cas);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> delete(final String key) {
    DeleteRequest request = new DeleteRequest(key, charset);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureDeleteFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> add(String key, V value, int ttl) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.ADD, key, charset, valueBytes, ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> replace(String key, V value, int ttl) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.REPLACE, key, charset, valueBytes, ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> append(String key, V value) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.APPEND, key, charset, valueBytes, 0);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> prepend(String key, V value) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.PREPEND, key, charset, valueBytes, 0);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<Long> incr(String key, long by) {
    IncrRequest request = IncrRequest.createIncr(key, charset, by);
    CompletionStage<Long> future = rawMemcacheClient.send(request);
    metrics.measureIncrDecrFuture(future);
    return future;
  }

  @Override
  public CompletionStage<Long> decr(String key, long by) {
    IncrRequest request = IncrRequest.createDecr(key, charset, by);
    CompletionStage<Long> future = rawMemcacheClient.send(request);
    metrics.measureIncrDecrFuture(future);
    return future;
  }

  @Override
  public CompletionStage<V> get(final String key) {
    return transformerUtil.unwrap(get(key, false));
  }

  @Override
  public CompletionStage<GetResult<V>> casGet(final String key) {
    return get(key, true);
  }

  private CompletionStage<GetResult<V>> get(final String key, final boolean withCas) {

    final CompletionStage<GetResult<byte[]>> future =
            rawMemcacheClient.send(new GetRequest(key, charset, withCas));

    metrics.measureGetFuture(future);
    return transformerUtil.decode(future);
  }

  @Override
  public CompletionStage<List<V>> get(final List<String> keys) {
    return transformerUtil.unwrapList(multiget(keys, false));
  }

  @Override
  public CompletionStage<List<GetResult<V>>> casGet(final List<String> keys) {
    return multiget(keys, true);
  }

  @Override
  public CompletionStage<MemcacheStatus> touch(String key, int ttl) {
    TouchRequest request = new TouchRequest(key, charset, ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureTouchFuture(future);
    return future;
  }

  private CompletionStage<List<GetResult<V>>> multiget(List<String> keys, boolean withCas) {

    final int size = keys.size();
    if (size == 0) {
      return CompletableFuture.completedFuture(Collections.<GetResult<V>>emptyList());
    }

    final List<List<String>> keyPartition =
        Lists.partition(keys, MemcacheEncoder.MAX_MULTIGET_SIZE);

    final List<CompletionStage<List<GetResult<byte[]>>>> futuresList =
        keyPartition.stream()
            .map(part -> rawMemcacheClient.send(MultigetRequest.create(part, charset, withCas)))
            .collect(Collectors.toList());

    final CompletableFuture<List<GetResult<byte[]>>> [] futures =
        futuresList.toArray(new CompletableFuture[keyPartition.size()]);

    final CompletableFuture<List<GetResult<byte[]>>> future = CompletableFuture
        .allOf(futures)
        .thenApply(__ -> Arrays.stream(futures)
            .map(CompletableFuture::join)
            .flatMap(List::stream)
            .collect(Collectors.toList()));

    metrics.measureMultigetFuture(future);
    return transformerUtil.decodeList(future);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#shutdown()
   */
  @Override
  public void shutdown() {
    rawMemcacheClient.shutdown();
  }

  @Override
  public void registerForConnectionChanges(ConnectionChangeListener listener) {
    rawMemcacheClient.registerForConnectionChanges(listener);
  }

  @Override
  public void unregisterForConnectionChanges(ConnectionChangeListener listener) {
    rawMemcacheClient.unregisterForConnectionChanges(listener);
  }

  /*
     * @see com.spotify.folsom.BinaryMemcacheClient#isConnected()
     */
  @Override
  public boolean isConnected() {
    return rawMemcacheClient.isConnected();
  }

  @Override
  public int numTotalConnections() {
    return rawMemcacheClient.numTotalConnections();
  }

  @Override
  public int numActiveConnections() {
    return rawMemcacheClient.numActiveConnections();
  }

  @Override
  public String toString() {
    return "AsciiMemcacheClient(" + rawMemcacheClient + ")";
  }

  @Override
  public RawMemcacheClient getRawMemcacheClient() {
    return rawMemcacheClient;
  }
}
