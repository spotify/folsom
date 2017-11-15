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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

class CallbackSettableFuture<T> extends CompletableFuture<T> implements Runnable {
  private final CompletableFuture<T> future;

  public CallbackSettableFuture(CompletableFuture<T> future) {
    this.future = future;
  }

  @Override
  public void run() {
    try {
      complete(future.get());
    } catch (ExecutionException e) {
      completeExceptionally(e.getCause());
    } catch (InterruptedException | RuntimeException | Error e) {
      completeExceptionally(e);
    }
  }
}
