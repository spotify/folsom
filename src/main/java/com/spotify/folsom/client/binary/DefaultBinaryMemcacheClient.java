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

package com.spotify.folsom.client.binary;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.BinaryMemcacheClient;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.Transcoder;
import com.spotify.folsom.client.OpCode;
import com.spotify.folsom.client.TransformerUtil;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

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
  private final Charset charset;

  public DefaultBinaryMemcacheClient(final RawMemcacheClient rawMemcacheClient,
                                     final Metrics metrics,
                                     final Transcoder<V> valueTranscoder,
                                     final Charset charset) {
    this.rawMemcacheClient = rawMemcacheClient;
    this.metrics = metrics;
    this.valueTranscoder = valueTranscoder;
    this.charset = charset;
    this.transformerUtil = new TransformerUtil<>(valueTranscoder);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#set(java.lang.String, V, int)
   */
  @Override
  public ListenableFuture<MemcacheStatus> set(final String key, final V value, final int ttl) {
    return setInternal(OpCode.SET, key, value, ttl);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#set(java.lang.String, V, int, long)
   */
  @Override
  public ListenableFuture<MemcacheStatus> set(
          final String key, final V value, final int ttl, final long cas) {
    return casSetInternal(OpCode.SET, key, value, ttl, cas);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#add(java.lang.String, V, int)
   */
  @Override
  public ListenableFuture<MemcacheStatus> add(final String key, final V value, final int ttl) {
    return setInternal(OpCode.ADD, key, value, ttl);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#replace(java.lang.String, V, int)
   */
  @Override
  public ListenableFuture<MemcacheStatus> replace(final String key, final V value, final int ttl) {
    return setInternal(OpCode.REPLACE, key, value, ttl);
  }

  private ListenableFuture<MemcacheStatus> setInternal(final byte opcode,
                                             final String key,
                                             final V value,
                                             final int ttl) {
    return casSetInternal(opcode, key, value, ttl, 0);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#add(java.lang.String, V, int, long)
   */
  @Override
  public ListenableFuture<MemcacheStatus> add(
          final String key, final V value, final int ttl, final long cas) {
    return casSetInternal(OpCode.ADD, key, value, ttl, cas);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#replace(java.lang.String, V, int, long)
   */
  @Override
  public ListenableFuture<MemcacheStatus> replace(
          final String key, final V value, final int ttl, final long cas) {
    return casSetInternal(OpCode.REPLACE, key, value, ttl, cas);
  }

  private ListenableFuture<MemcacheStatus> casSetInternal(final byte opcode,
                                                final String key,
                                                final V value,
                                                final int ttl,
                                                final long cas) {
    checkNotNull(value);

    final byte[] valueBytes = valueTranscoder.encode(value);
    SetRequest request = new SetRequest(
            opcode, key, charset, valueBytes, ttl, cas);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureSetFuture(future);
    return future;
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#get(java.lang.String)
   */
  @Override
  public ListenableFuture<V> get(final String key) {
    return getAndTouch(key, -1);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#getAndTouch(java.lang.String, int)
   */
  @Override
  public ListenableFuture<V> getAndTouch(final String key, final int ttl) {
    return transformerUtil.unwrap(casGetAndTouch(key, ttl));
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#get(java.util.List)
   */
  @Override
  public ListenableFuture<List<V>> get(final List<String> keys) {
    return getAndTouch(keys, -1);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#casGet(java.lang.String)
   */
  @Override
  public ListenableFuture<GetResult<V>> casGet(final String key) {
    return getInternal(key, -1);
  }

  private ListenableFuture<GetResult<V>> getInternal(final String key, final int ttl) {
    GetRequest request = new GetRequest(key, charset, OpCode.GET, ttl);
    final ListenableFuture<GetResult<byte[]>> future =
            rawMemcacheClient.send(request);
    metrics.measureGetFuture(future);
    return transformerUtil.decode(future);
  }

  @Override
  public ListenableFuture<List<GetResult<V>>> casGet(List<String> keys) {
    return multiget(keys, -1);
  }

  private ListenableFuture<List<GetResult<V>>> multiget(List<String> keys, int ttl) {
    final int size = keys.size();
    if (size == 0) {
      return Futures.immediateFuture(Collections.<GetResult<V>>emptyList());
    }

    MultigetRequest request = MultigetRequest.create(keys, charset, ttl);
    final ListenableFuture<List<GetResult<byte[]>>> future = rawMemcacheClient.send(request);
    metrics.measureMultigetFuture(future);
    return transformerUtil.decodeList(future);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#getAndTouch(java.util.List, int)
   */
  @Override
  public ListenableFuture<List<V>> getAndTouch(final List<String> keys, final int ttl) {
    return transformerUtil.unwrapList(multiget(keys, ttl));
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#casGetAndTouch(java.lang.String, int)
   */
  @Override
  public ListenableFuture<GetResult<V>> casGetAndTouch(final String key, final int ttl) {
    return getInternal(key, ttl);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#touch(java.lang.String, int)
   */
  @Override
  public ListenableFuture<MemcacheStatus> touch(final String key, final int ttl) {
    TouchRequest request = new TouchRequest(key, charset, ttl);
    ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureTouchFuture(future);
    return future;
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#delete(java.lang.String)
   */
  @Override
  public ListenableFuture<MemcacheStatus> delete(final String key) {
    DeleteRequest request = new DeleteRequest(key, charset);
    final ListenableFuture<MemcacheStatus> future = rawMemcacheClient.send(request);
    metrics.measureDeleteFuture(future);
    return future;
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#incr(java.lang.String, long, long, int)
   */
  @Override
  public ListenableFuture<Long> incr(
          final String key, final long by, final long initial, final int ttl) {
    return incrInternal(OpCode.INCREMENT, key, by, initial, ttl);
  }

  private ListenableFuture<Long> incrInternal(final byte opcode,
                                              final String key,
                                              final long by,
                                              final long initial,
                                              final int ttl) {

    final ListenableFuture<Long> future = rawMemcacheClient.send(
            new IncrRequest(key, charset, opcode, by, initial, ttl));
    metrics.measureIncrDecrFuture(future);
    return future;
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#decr(java.lang.String, long, long, int)
   */
  @Override
  public ListenableFuture<Long> decr(
          final String key, final long by, final long initial, final int ttl) {
    return incrInternal(OpCode.DECREMENT, key, by, initial, ttl);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#append(java.lang.String, V)
   */
  @Override
  public ListenableFuture<MemcacheStatus> append(final String key, final V value) {
    return casSetInternal(OpCode.APPEND, key, value, 0, 0);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#append(java.lang.String, V, long)
   */
  @Override
  public ListenableFuture<MemcacheStatus> append(final String key, final V value, final long cas) {
    return casSetInternal(OpCode.APPEND, key, value, 0, cas);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#prepend(java.lang.String, V)
   */
  @Override
  public ListenableFuture<MemcacheStatus> prepend(final String key, final V value) {
    return casSetInternal(OpCode.PREPEND, key, value, 0, 0);
  }


  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#prepend(java.lang.String, V, long)
   */
  @Override
  public ListenableFuture<MemcacheStatus> prepend(final String key, final V value, final long cas) {
    return casSetInternal(OpCode.PREPEND, key, value, 0, cas);
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#noop()
   */
  @Override
  public ListenableFuture<Void> noop() {
    return rawMemcacheClient.send(new NoopRequest());
  }

  /*
   * @see com.spotify.folsom.BinaryMemcacheClient#shutdown()
   */
  @Override
  public void shutdown() {
    rawMemcacheClient.shutdown();
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
    return "BinaryMemcacheClient(" + rawMemcacheClient + ")";
  }
}

