/*
 * Copyright (c) 2026 Spotify AB
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 6, time = 1)
public class AsciiMemcacheDecoderBenchmark {

  @Param({"single", "batch16", "miss"})
  public String response;

  private AsciiMemcacheDecoder decoder;
  private ByteBuf buffer;
  private List<Object> output;

  @Setup(Level.Trial)
  public void setUp() {
    decoder = new AsciiMemcacheDecoder(StandardCharsets.US_ASCII);
    output = new ArrayList<>(1);

    StringBuilder wire = new StringBuilder();
    int values = "batch16".equals(response) ? 16 : "single".equals(response) ? 1 : 0;
    char[] value = new char[64];
    Arrays.fill(value, 'x');
    for (int i = 0; i < values; i++) {
      wire.append("VALUE key-").append(i).append(" 0 64\r\n");
      wire.append(value).append("\r\n");
    }
    wire.append("END\r\n");
    buffer = Unpooled.wrappedBuffer(wire.toString().getBytes(StandardCharsets.US_ASCII));
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    buffer.release();
  }

  @Benchmark
  public Object decode() throws Exception {
    buffer.readerIndex(0);
    output.clear();
    decoder.decode(null, buffer, output);
    return output.get(0);
  }
}
