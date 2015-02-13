/*
 * Copyright (c) 2015 Spotify AB
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
package com.spotify.folsom.client;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.Transcoder;

import java.util.List;

public class TransformerUtil<T> {
  private final Function<GetResult<T>, T> getResultToValue;
  private final ListResultUnwrapper<T> listResultUnwrapper;
  private final ResultDecoder<T> resultDecoder;
  private final ListResultDecoder<T> listResultDecoder;

  public TransformerUtil(Transcoder<T> transcoder) {
    this.getResultToValue = new ResultUnwrapper<>();
    this.listResultUnwrapper = new ListResultUnwrapper<>(getResultToValue);
    this.resultDecoder = new ResultDecoder<>(transcoder);
    this.listResultDecoder = new ListResultDecoder<>(resultDecoder);
  }


  public ListenableFuture<T> unwrap(ListenableFuture<GetResult<T>> future) {
    return Utils.transform(future, getResultToValue);
  }

  public ListenableFuture<GetResult<T>> decode(ListenableFuture<GetResult<byte[]>> future) {
    return Utils.transform(future, resultDecoder);
  }

  public ListenableFuture<List<T>> unwrapList(ListenableFuture<List<GetResult<T>>> future) {
    return Utils.transform(future, listResultUnwrapper);
  }

  public ListenableFuture<List<GetResult<T>>> decodeList(
          ListenableFuture<List<GetResult<byte[]>>> future) {
    return Utils.transform(future, listResultDecoder);
  }

  private static class ResultUnwrapper<T> implements Function<GetResult<T>, T> {
    @Override
    public T apply(GetResult<T> input) {
      if (input == null) {
        return null;
      }
      return input.getValue();
    }
  }

  private static class ListResultUnwrapper<T> implements Function<List<GetResult<T>>, List<T>> {
    private final Function<GetResult<T>, T> resultUnwrapper;

    public ListResultUnwrapper(Function<GetResult<T>, T> resultUnwrapper) {
      this.resultUnwrapper = resultUnwrapper;
    }

    @Override
    public List<T> apply(List<GetResult<T>> input) {
      return Lists.transform(input, resultUnwrapper);
    }
  }

  private static class ResultDecoder<T> implements Function<GetResult<byte[]>, GetResult<T>> {
    private final Transcoder<T> transcoder;

    public ResultDecoder(Transcoder<T> transcoder) {
      this.transcoder = transcoder;
    }

    @Override
    public GetResult<T> apply(GetResult<byte[]> input) {
      if (input == null) {
        return null;
      }
      return GetResult.success(transcoder.decode(input.getValue()), input.getCas());
    }
  }

  private static class ListResultDecoder<T>
          implements Function<List<GetResult<byte[]>>, List<GetResult<T>>> {
    private final ResultDecoder<T> resultDecoder;

    public ListResultDecoder(ResultDecoder<T> resultDecoder) {
      this.resultDecoder = resultDecoder;
    }

    @Override
    public List<GetResult<T>> apply(List<GetResult<byte[]>> input) {
      return Lists.transform(input, resultDecoder);
    }
  }
}
