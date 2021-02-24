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

import static com.spotify.folsom.client.AbstractRequest.encodeKey;
import static com.spotify.folsom.client.AbstractRequest.encodeKeys;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.Lists;
import com.spotify.folsom.AsciiMemcacheClient;
import com.spotify.folsom.ConnectionChangeListener;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.MemcachedStats;
import com.spotify.folsom.Metrics;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.Tracer;
import com.spotify.folsom.Transcoder;
import com.spotify.folsom.client.MemcacheEncoder;
import com.spotify.folsom.client.TransformerUtil;
import com.spotify.folsom.client.Utils;
import com.spotify.futures.CompletableFutures;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * The default implementation of {@link com.spotify.folsom.AsciiMemcacheClient}
 *
 * @param <V> The value type for all operations
 */
public class DefaultAsciiMemcacheClient<V> implements AsciiMemcacheClient<V> {

  private final RawMemcacheClient rawMemcacheClient;
  private final Metrics metrics;
  private final Tracer tracer;
  private final Transcoder<V> valueTranscoder;
  private final TransformerUtil<V> transformerUtil;
  private final Charset charset;
  private final int maxKeyLength;

  private DefaultAsciiMemcacheClient(
      final RawMemcacheClient rawMemcacheClient,
      final Metrics metrics,
      final Tracer tracer,
      final Transcoder<V> valueTranscoder,
      final TransformerUtil<V> transformerUtil,
      final Charset charset,
      final int maxKeyLength) {
    this.rawMemcacheClient = rawMemcacheClient;
    this.metrics = metrics;
    this.tracer = tracer;
    this.valueTranscoder = valueTranscoder;
    this.charset = charset;
    this.transformerUtil = transformerUtil;
    this.maxKeyLength = maxKeyLength;
  }

  public DefaultAsciiMemcacheClient(
      final RawMemcacheClient rawMemcacheClient,
      final Metrics metrics,
      final Tracer tracer,
      final Transcoder<V> valueTranscoder,
      final Charset charset,
      final int maxKeyLength) {
    this(
        rawMemcacheClient,
        metrics,
        tracer,
        valueTranscoder,
        new TransformerUtil<>(valueTranscoder),
        charset,
        maxKeyLength);
  }

  @Override
  public CompletionStage<MemcacheStatus> set(final String key, final V value, final int ttl) {
    requireNonNull(value);

    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request =
        SetRequest.create(
            SetRequest.Operation.SET, encodeKey(key, charset, maxKeyLength), valueBytes, ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    tracer.span("folsom.set", future, "set", key, valueBytes);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> set(String key, V value, int ttl, long cas) {
    requireNonNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    byte[] byteKey = encodeKey(key, charset, maxKeyLength);
    SetRequest request = SetRequest.casSet(byteKey, valueBytes, ttl, cas);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    tracer.span("folsom.set", future, "set", key, valueBytes);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> delete(final String key) {
    DeleteRequest request = new DeleteRequest(encodeKey(key, charset, maxKeyLength));
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureDeleteFuture(future);
    tracer.span("folsom.delete", future, "delete", key);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> delete(String key, long cas) {
    DeleteWithCasRequest request =
        new DeleteWithCasRequest(encodeKey(key, charset, maxKeyLength), cas);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureDeleteFuture(future);
    tracer.span("folsom.deleteWithCas", future, "delete", key);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> deleteAll(String key) {
    DeleteAllRequest request = new DeleteAllRequest(encodeKey(key, charset, maxKeyLength));
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureDeleteFuture(future);
    tracer.span("folsom.delete_all", future, "delete", key);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> add(String key, V value, int ttl) {
    requireNonNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request =
        SetRequest.create(
            SetRequest.Operation.ADD, encodeKey(key, charset, maxKeyLength), valueBytes, ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    tracer.span("folsom.add", future, "add", key, valueBytes);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> replace(String key, V value, int ttl) {
    requireNonNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request =
        SetRequest.create(
            SetRequest.Operation.REPLACE, encodeKey(key, charset, maxKeyLength), valueBytes, ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    tracer.span("folsom.replace", future, "replace", key, valueBytes);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> append(String key, V value) {
    requireNonNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request =
        SetRequest.create(
            SetRequest.Operation.APPEND, encodeKey(key, charset, maxKeyLength), valueBytes, 0);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    tracer.span("folsom.append", future, "append", key, valueBytes);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> prepend(String key, V value) {
    requireNonNull(value);
    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request =
        SetRequest.create(
            SetRequest.Operation.PREPEND, encodeKey(key, charset, maxKeyLength), valueBytes, 0);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    tracer.span("folsom.prepend", future, "prepend", key, valueBytes);
    return future;
  }

  @Override
  public CompletionStage<Long> incr(String key, long by) {
    IncrRequest request = IncrRequest.createIncr(encodeKey(key, charset, maxKeyLength), by);
    CompletionStage<Long> future = rawMemcacheClient.send(request);
    metrics.measureIncrDecrFuture(future);
    tracer.span("folsom.incr", future, "incr", key);
    return future;
  }

  @Override
  public CompletionStage<Long> decr(String key, long by) {
    IncrRequest request = IncrRequest.createDecr(encodeKey(key, charset, maxKeyLength), by);
    CompletionStage<Long> future = rawMemcacheClient.send(request);
    metrics.measureIncrDecrFuture(future);
    tracer.span("folsom.decr", future, "decr", key);
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
    tracer.span("folsom.get", future, "get", key);
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
    tracer.span("folsom.touch", future, "touch", key);
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
    tracer.span("folsom.multiget", future, "get");

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
  public Throwable getConnectionFailure() {
    return rawMemcacheClient.getConnectionFailure();
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

  @Override
  public Map<String, AsciiMemcacheClient<V>> getAllNodes() {
    return rawMemcacheClient
        .getAllNodes()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> withClient(entry.getValue())));
  }

  private DefaultAsciiMemcacheClient<V> withClient(final RawMemcacheClient client) {
    return new DefaultAsciiMemcacheClient<>(
        client, metrics, tracer, valueTranscoder, transformerUtil, charset, maxKeyLength);
  }

  @Override
  public CompletionStage<Map<String, MemcachedStats>> getStats(final String key) {
    return rawMemcacheClient.send(new StatsRequest(key));
  }
}
