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
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.Transcoder;
import com.spotify.folsom.client.TransformerUtil;

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

  public DefaultAsciiMemcacheClient(final RawMemcacheClient rawMemcacheClient,
                                    final Metrics metrics,
                                    final Transcoder<V> valueTranscoder
  ) {
    this.rawMemcacheClient = rawMemcacheClient;
    this.metrics = metrics;
    this.valueTranscoder = valueTranscoder;
    this.transformerUtil = new TransformerUtil<>(valueTranscoder);
  }

  @Override
  public ListenableFuture<MemcacheStatus> set(final String key, final V value, final int ttl) {
    final String keyBytes = encodeKey(key);
    checkNotNull(value);

    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(SetRequest.Operation.SET, keyBytes, valueBytes, ttl);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> set(String key, V value, int ttl, long cas) {
    final String keyBytes = encodeKey(key);
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.casSet(keyBytes, valueBytes, ttl, cas);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> delete(final String key) {
    final String keyBytes = encodeKey(key);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(new DeleteRequest(keyBytes));
    metrics.measureDeleteFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> add(String key, V value, int ttl) {
    final String keyBytes = encodeKey(key);
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(SetRequest.Operation.ADD, keyBytes, valueBytes, ttl);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> replace(String key, V value, int ttl) {
    final String keyBytes = encodeKey(key);
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(SetRequest.Operation.REPLACE, keyBytes, valueBytes, ttl);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> append(String key, V value) {
    final String keyBytes = encodeKey(key);
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(SetRequest.Operation.APPEND, keyBytes, valueBytes, 0);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<MemcacheStatus> prepend(String key, V value) {
    final String keyBytes = encodeKey(key);
    checkNotNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = SetRequest.create(SetRequest.Operation.PREPEND, keyBytes, valueBytes, 0);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<Long> incr(String key, long by) {
    final String keyBytes = encodeKey(key);
    IncrRequest request = IncrRequest.create(IncrRequest.Operation.INCR, keyBytes, by);
    ListenableFuture<Long> future = rawMemcacheClient.send(request);
    metrics.measureIncrDecrFuture(future);
    return future;
  }

  @Override
  public ListenableFuture<Long> decr(String key, long by) {
    final String keyBytes = encodeKey(key);
    IncrRequest request = IncrRequest.create(IncrRequest.Operation.DECR, keyBytes, by);
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
    final String keyBytes = encodeKey(key);

    final ListenableFuture<GetResult<byte[]>> future =
            rawMemcacheClient.send(new GetRequest(keyBytes, withCas));

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
    final String keyBytes = encodeKey(key);
    TouchRequest request = new TouchRequest(keyBytes, ttl);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureTouchFuture(future);
    return future;
  }

  private ListenableFuture<List<GetResult<V>>> multiget(List<String> keys, boolean withCas) {
    final int size = keys.size();
    if (size == 0) {
      return Futures.immediateFuture(Collections.<GetResult<V>>emptyList());
    }

    final List<String> rawKeys = Lists.newArrayList();
    for (final String key : keys) {
      rawKeys.add(encodeKey(key));
    }
    MultigetRequest request = MultigetRequest.create(rawKeys, withCas);
    final ListenableFuture<List<GetResult<byte[]>>> future = rawMemcacheClient.send(request);
    metrics.measureMultigetFuture(future);
    return transformerUtil.decodeList(future);
  }

  private String encodeKey(final String key) {
    checkNotNull(key, "key may not be null");
    return key;
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#shutdown()
   */
  @Override
  public ListenableFuture<Void> shutdown() {
    return rawMemcacheClient.shutdown();
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#isConnected()
   */
  @Override
  public boolean isConnected() {
    return rawMemcacheClient.isConnected();
  }

  @Override
  public String toString() {
    return "AsciiMemcacheClient(" + rawMemcacheClient + ")";
  }
}
