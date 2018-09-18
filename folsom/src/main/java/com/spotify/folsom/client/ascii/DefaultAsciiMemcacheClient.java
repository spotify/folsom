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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.spotify.folsom.client.Request.encodeKey;
import static com.spotify.folsom.client.Request.encodeKeys;

import com.google.common.collect.Lists;
import com.spotify.futures.CompletableFutures;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
  private final int maxKeyLength;

  public DefaultAsciiMemcacheClient(final RawMemcacheClient rawMemcacheClient,
                                    final Metrics metrics,
                                    final Transcoder<V> valueTranscoder,
                                    final Charset charset,
                                    final int maxKeyLength) {
    this.rawMemcacheClient = rawMemcacheClient;
    this.metrics = metrics;
    this.valueTranscoder = valueTranscoder;
    this.charset = charset;
    this.transformerUtil = new TransformerUtil<>(valueTranscoder);
    this.maxKeyLength = maxKeyLength;
  }

  @Override
  public CompletionStage<MemcacheStatus> set(final String key, final V value, final int ttl) {
    checkNotNull(value);

    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
        SetRequest.Operation.SET,
        encodeKey(key, charset, maxKeyLength),
        valueBytes,
        ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> set(String key, V value, int ttl, long cas) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    byte[] byteKey = encodeKey(key, charset, maxKeyLength);
    SetRequest request = SetRequest.casSet(byteKey, valueBytes, ttl, cas);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> delete(final String key) {
    DeleteRequest request = new DeleteRequest(encodeKey(key, charset, maxKeyLength));
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureDeleteFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> add(String key, V value, int ttl) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.ADD, encodeKey(key, charset, maxKeyLength), valueBytes, ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> replace(String key, V value, int ttl) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.REPLACE, encodeKey(key, charset, maxKeyLength), valueBytes, ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> append(String key, V value) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.APPEND, encodeKey(key, charset, maxKeyLength), valueBytes, 0);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> prepend(String key, V value) {
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(
            SetRequest.Operation.PREPEND, encodeKey(key, charset, maxKeyLength), valueBytes, 0);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public CompletionStage<Long> incr(String key, long by) {
    IncrRequest request = IncrRequest.createIncr(encodeKey(key, charset, maxKeyLength), by);
    CompletionStage<Long> future = rawMemcacheClient.send(request);
    metrics.measureIncrDecrFuture(future);
    return future;
  }

  @Override
  public CompletionStage<Long> decr(String key, long by) {
    IncrRequest request = IncrRequest.createDecr(encodeKey(key, charset, maxKeyLength), by);
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
            rawMemcacheClient.send(new GetRequest(encodeKey(key, charset, maxKeyLength), withCas));

    metrics.measureGetFuture(future);
    return transformerUtil.decode(future);
  }

  @Override
  public CompletionStage<List<V>> get(final List<String> keys) {
    final List<byte[]> byteKeys = encodeKeys(keys, charset, maxKeyLength);
    return transformerUtil.unwrapList(multiget(byteKeys, false));
  }

  @Override
  public CompletionStage<List<GetResult<V>>> casGet(final List<String> keys) {
    final List<byte[]> byteKeys = encodeKeys(keys, charset, maxKeyLength);
    return multiget(byteKeys, true);
  }

  @Override
  public CompletionStage<MemcacheStatus> touch(String key, int ttl) {
    TouchRequest request = new TouchRequest(encodeKey(key, charset, maxKeyLength), ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureTouchFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> flushAll(final int delay) {
    return rawMemcacheClient.send(new FlushRequest(delay));
  }

  private CompletionStage<List<GetResult<V>>> multiget(List<byte[]> keys, boolean withCas) {
    final int size = keys.size();
    if (size == 0) {
      return CompletableFuture.completedFuture(Collections.<GetResult<V>>emptyList());
    }

    final List<List<byte[]>> keyPartition =
            Lists.partition(keys, MemcacheEncoder.MAX_MULTIGET_SIZE);
    final List<CompletionStage<List<GetResult<byte[]>>>> futureList =
            new ArrayList<>(keyPartition.size());

    for (final List<byte[]> part : keyPartition) {
      MultigetRequest request = MultigetRequest.create(part, withCas);
      futureList.add(rawMemcacheClient.send(request));
    }

    final CompletionStage<List<GetResult<byte[]>>> future =
        ((CompletionStage<List<List<GetResult<byte[]>>>>) CompletableFutures.allAsList(futureList))
            .thenApply(Utils.flatten());

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
