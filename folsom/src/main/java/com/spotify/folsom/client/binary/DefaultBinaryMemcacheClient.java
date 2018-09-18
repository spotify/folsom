/*
 * Copyright (c) 2014-2018 Spotify AB
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

package com.spotify.folsom.client.binary;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.spotify.folsom.client.Request.encodeKey;
import static com.spotify.folsom.client.Request.encodeKeys;

import com.google.common.collect.Lists;
import com.spotify.folsom.BinaryMemcacheClient;
import com.spotify.folsom.ConnectionChangeListener;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.Transcoder;
import com.spotify.folsom.client.MemcacheEncoder;
import com.spotify.folsom.client.OpCode;
import com.spotify.folsom.client.TransformerUtil;
import com.spotify.folsom.client.Utils;
import com.spotify.futures.CompletableFutures;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * The default implementation of {@link com.spotify.folsom.BinaryMemcacheClient}
 *
 * @param <V> The value type for all operations
 */
public class DefaultBinaryMemcacheClient<V> implements BinaryMemcacheClient<V> {

  private final RawMemcacheClient rawMemcacheClient;
  private final Metrics metrics;
  private final Transcoder<V> valueTranscoder;
  private final TransformerUtil<V> transformerUtil;
  private final int maxKeyLength;
  private final Charset charset;

  public DefaultBinaryMemcacheClient(final RawMemcacheClient rawMemcacheClient,
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

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#set(java.lang.String, V, int)
   */
  @Override
  public CompletionStage<MemcacheStatus> set(final String key, final V value, final int ttl) {
    return setInternal(OpCode.SET, key, value, ttl);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#set(java.lang.String, V, int, long)
   */
  @Override
  public CompletionStage<MemcacheStatus> set(
          final String key, final V value, final int ttl, final long cas) {
    return casSetInternal(OpCode.SET, key, value, ttl, cas);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#add(java.lang.String, V, int)
   */
  @Override
  public CompletionStage<MemcacheStatus> add(final String key, final V value, final int ttl) {
    return setInternal(OpCode.ADD, key, value, ttl);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#replace(java.lang.String, V, int)
   */
  @Override
  public CompletionStage<MemcacheStatus> replace(final String key, final V value, final int ttl) {
    return setInternal(OpCode.REPLACE, key, value, ttl);
  }

  private CompletionStage<MemcacheStatus> setInternal(final byte opcode,
                                             final String key,
                                             final V value,
                                             final int ttl) {
    return casSetInternal(opcode, key, value, ttl, 0);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#add(java.lang.String, V, int, long)
   */
  @Override
  public CompletionStage<MemcacheStatus> add(
          final String key, final V value, final int ttl, final long cas) {
    return casSetInternal(OpCode.ADD, key, value, ttl, cas);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#replace(java.lang.String, V, int, long)
   */
  @Override
  public CompletionStage<MemcacheStatus> replace(
          final String key, final V value, final int ttl, final long cas) {
    return casSetInternal(OpCode.REPLACE, key, value, ttl, cas);
  }

  private CompletionStage<MemcacheStatus> casSetInternal(final byte opcode,
                                                final String key,
                                                final V value,
                                                final int ttl,
                                                final long cas) {
    checkNotNull(value);

    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = new SetRequest(
            opcode, encodeKey(key, charset, maxKeyLength), valueBytes, ttl, cas);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#get(java.lang.String)
   */
  @Override
  public CompletionStage<V> get(final String key) {
    return getAndTouch(key, -1);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#getAndTouch(java.lang.String, int)
   */
  @Override
  public CompletionStage<V> getAndTouch(final String key, final int ttl) {
    return transformerUtil.unwrap(casGetAndTouch(key, ttl));
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#get(java.util.List)
   */
  @Override
  public CompletionStage<List<V>> get(final List<String> keys) {
    return getAndTouch(keys, -1);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#casGet(java.lang.String)
   */
  @Override
  public CompletionStage<GetResult<V>> casGet(final String key) {
    return getInternal(key, -1);
  }

  private CompletionStage<GetResult<V>> getInternal(final String key, final int ttl) {
    final byte opCode = ttl > -1 ? OpCode.GAT : OpCode.GET;
    GetRequest request = new GetRequest(encodeKey(key, charset, maxKeyLength), opCode, ttl);
    final CompletionStage<GetResult<byte[]>> future =
            rawMemcacheClient.send(request);
    metrics.measureGetFuture(future);
    return transformerUtil.decode(future);
  }

  @Override
  public CompletionStage<List<GetResult<V>>> casGet(List<String> keys) {
    final List<byte[]> byteKeys = encodeKeys(keys, charset, maxKeyLength);
    return multiget(byteKeys, -1);
  }

  private CompletionStage<List<GetResult<V>>> multiget(List<byte[]> keys, int ttl) {
    final int size = keys.size();
    if (size == 0) {
      return CompletableFuture.completedFuture(Collections.<GetResult<V>>emptyList());
    }

    final List<List<byte[]>> keyPartition =
          Lists.partition(keys, MemcacheEncoder.MAX_MULTIGET_SIZE);
    final List<CompletionStage<List<GetResult<byte[]>>>> futureList =
          new ArrayList<>(keyPartition.size());

    for (final List<byte[]> part : keyPartition) {
      MultigetRequest request = MultigetRequest.create(part, ttl);
      futureList.add(rawMemcacheClient.send(request));
    }

    final CompletionStage<List<GetResult<byte[]>>> future =
        CompletableFutures.allAsList(futureList)
            .thenApply(Utils.flatten());

    metrics.measureMultigetFuture(future);
    return transformerUtil.decodeList(future);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#getAndTouch(java.util.List, int)
   */
  @Override
  public CompletionStage<List<V>> getAndTouch(final List<String> keys, final int ttl) {
    final List<byte[]> byteKeys = encodeKeys(keys, charset, maxKeyLength);
    return transformerUtil.unwrapList(multiget(byteKeys, ttl));
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#casGetAndTouch(java.lang.String, int)
   */
  @Override
  public CompletionStage<GetResult<V>> casGetAndTouch(final String key, final int ttl) {
    return getInternal(key, ttl);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#touch(java.lang.String, int)
   */
  @Override
  public CompletionStage<MemcacheStatus> touch(final String key, final int ttl) {
    TouchRequest request = new TouchRequest(encodeKey(key, charset, maxKeyLength), ttl);
    CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureTouchFuture(future);
    return future;
  }

  @Override
  public CompletionStage<MemcacheStatus> flushAll(final int delay) {
    return rawMemcacheClient.send(new FlushRequest(delay));
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#delete(java.lang.String)
   */
  @Override
  public CompletionStage<MemcacheStatus> delete(final String key) {
    DeleteRequest request = new DeleteRequest(encodeKey(key, charset, maxKeyLength));
    final CompletionStage<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureDeleteFuture(future);
    return future;
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#incr(java.lang.String, long, long, int)
   */
  @Override
  public CompletionStage<Long> incr(
          final String key, final long by, final long initial, final int ttl) {
    return incrInternal(OpCode.INCREMENT, key, by, initial, ttl);
  }

  private CompletionStage<Long> incrInternal(final byte opcode,
                                              final String key,
                                              final long by,
                                              final long initial,
                                              final int ttl) {

    final CompletionStage<Long> future = rawMemcacheClient.send(
            new IncrRequest(encodeKey(key, charset, maxKeyLength), opcode, by, initial, ttl));
    metrics.measureIncrDecrFuture(future);
    return future;
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#decr(java.lang.String, long, long, int)
   */
  @Override
  public CompletionStage<Long> decr(
          final String key, final long by, final long initial, final int ttl) {
    return incrInternal(OpCode.DECREMENT, key, by, initial, ttl);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#append(java.lang.String, V)
   */
  @Override
  public CompletionStage<MemcacheStatus> append(final String key, final V value) {
    return casSetInternal(OpCode.APPEND, key, value, 0, 0);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#append(java.lang.String, V, long)
   */
  @Override
  public CompletionStage<MemcacheStatus> append(final String key, final V value, final long cas) {
    return casSetInternal(OpCode.APPEND, key, value, 0, cas);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#prepend(java.lang.String, V)
   */
  @Override
  public CompletionStage<MemcacheStatus> prepend(final String key, final V value) {
    return casSetInternal(OpCode.PREPEND, key, value, 0, 0);
  }


  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#prepend(java.lang.String, V, long)
   */
  @Override
  public CompletionStage<MemcacheStatus> prepend(final String key, final V value, final long cas) {
    return casSetInternal(OpCode.PREPEND, key, value, 0, cas);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#noop()
   */
  @Override
  public CompletionStage<Void> noop() {
    return rawMemcacheClient.send(new NoopRequest());
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
    rawMemcacheClient.registerForConnectionChanges(listener);
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
  public RawMemcacheClient getRawMemcacheClient() {
    return rawMemcacheClient;
  }

  @Override
  public String toString() {
    return "BinaryMemcacheClient(" + rawMemcacheClient + ")";
  }
}

