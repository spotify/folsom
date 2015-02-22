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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import java.util.Collections;
import java.util.List;

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
  public ListenableFuture<MemcacheStatus> set(final String key, final V value, final int ttl) {
    checkNotNull(value);

    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.SET, key, charset, valueBytes, ttl);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> set(String key, V value, int ttl, long cas) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.casSet(key, charset, valueBytes, ttl, cas);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> delete(final String key) {
    DeleteRequest request = new DeleteRequest(key, charset);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureDeleteFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> add(String key, V value, int ttl) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.ADD, key, charset, valueBytes, ttl);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> replace(String key, V value, int ttl) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.REPLACE, key, charset, valueBytes, ttl);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> append(String key, V value) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.APPEND, key, charset, valueBytes, 0);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> prepend(String key, V value) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.PREPEND, key, charset, valueBytes, 0);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<Long> incr(String key, long by) {
    IncrRequest request = IncrRequest.createIncr(key, charset, by);
    ListenableFuture<Long> future = rawMemcacheClient.send(request);
    metrics.measureIncrDecrFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<Long> decr(String key, long by) {
    IncrRequest request = IncrRequest.createDecr(key, charset, by);
    ListenableFuture<Long> future = rawMemcacheClient.send(request);
    metrics.measureIncrDecrFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<V> get(final String key) {
    return transformerUtil.unwrap(get(key, false));
  }

  @Override
  public ListenableFuture<GetResult<V>> casGet(final String key) {
    return get(key, true);
  }

  private ListenableFuture<GetResult<V>> get(final String key, final boolean withCas) {

    final ListenableFuture<GetResult<byte[]>> future =
            rawMemcacheClient.send(new GetRequest(key, charset, withCas));

    metrics.measureGetFuture(future);
    return transformerUtil.decode(future);
  }

  @Override
  public ListenableFuture<List<V>> get(final List<String> keys) {
    return transformerUtil.unwrapList(multiget(keys, false));
  }

  @Override
  public ListenableFuture<List<GetResult<V>>> casGet(final List<String> keys) {
    return multiget(keys, true);
  }

  @Override
  public ListenableFuture<MemcacheStatus> touch(String key, int ttl) {
    TouchRequest request = new TouchRequest(key, charset, ttl);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureTouchFuture(future);
    return future;
  }

  private ListenableFuture<List<GetResult<V>>> multiget(List<String> keys, boolean withCas) {
    final int size = keys.size();
    if (size == 0) {
      return Futures.immediateFuture(Collections.<GetResult<V>>emptyList());
    }

    final List<List<String>> keyPartition =
            Lists.partition(keys, MemcacheEncoder.MAX_MULTIGET_SIZE);
    final List<ListenableFuture<List<GetResult<byte[]>>>> futureList =
            new ArrayList<>(keyPartition.size());

    for (final List<String> part : keyPartition) {
      MultigetRequest request = MultigetRequest.create(part, charset, withCas);
      futureList.add(rawMemcacheClient.send(request));
    }

    final ListenableFuture<List<GetResult<byte[]>>> future =
            Utils.transform(Futures.allAsList(futureList), Utils.<GetResult<byte[]>>flatten());

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
}
