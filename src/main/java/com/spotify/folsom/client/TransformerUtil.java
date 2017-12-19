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

import java.util.function.Function;
import java.util.concurrent.CompletionStage;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.Transcoder;

import java.util.List;
import java.util.stream.Collectors;

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


  public CompletionStage<T> unwrap(CompletionStage<GetResult<T>> future) {
    return future.thenApply(getResultToValue);
  }

  public CompletionStage<GetResult<T>> decode(CompletionStage<GetResult<byte[]>> future) {
    return future.thenApply(resultDecoder);
  }

  public CompletionStage<List<T>> unwrapList(CompletionStage<List<GetResult<T>>> future) {
    return future.thenApply(listResultUnwrapper);
  }

  public CompletionStage<List<GetResult<T>>> decodeList(
          CompletionStage<List<GetResult<byte[]>>> future) {
    return future.thenApply(listResultDecoder);
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
      return input.stream().map(resultUnwrapper).collect(Collectors.toList());
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
      return input.stream().map(resultDecoder).collect(Collectors.toList());
    }
  }
}
