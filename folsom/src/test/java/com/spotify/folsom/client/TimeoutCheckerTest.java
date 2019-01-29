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

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeoutCheckerTest {

  private TimeoutChecker sut = TimeoutChecker.create(TimeUnit.MILLISECONDS, 100);
  private Request<Integer> request;

  @Test
  public void testTimeout0ms() throws Exception {
    request = new DummyRequest();
    assertFalse(sut.check(request));
  }

  @Test
  public void testTimeout95ms() throws Exception {
    request = new DummyRequest();
    Thread.sleep(95);
    assertFalse(sut.check(request));
  }

  @Test
  public void testTimeout101ms() throws Exception {
    request = new DummyRequest();
    Thread.sleep(101);
    assertTrue(sut.check(request));
  }

  @Test
  public void testTimeoutSeconds() throws Exception {
    sut = TimeoutChecker.create(TimeUnit.SECONDS, 100);
    request = new DummyRequest();
    Thread.sleep(1);
    assertFalse(sut.check(request));
  }

  @Test
  public void testElapsedLessThanOne() throws InterruptedException {
    request = new DummyRequest();
    assertTrue(sut.elapsedNanos(request) <
            TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testElapsedMoreThanOne() throws InterruptedException {
    request = new DummyRequest();
    Thread.sleep(1);
    assertTrue(sut.elapsedNanos(request) >
            TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS));
  }


  class DummyRequest extends Request<Integer> {

    DummyRequest() {
      super("foobar".getBytes(Charset.defaultCharset()));
    }

    @Override
    public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
      return null;
    }

    @Override
    public void handle(final Object response) throws IOException {

    }
  }
}
