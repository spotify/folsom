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

import static org.junit.Assert.assertEquals;

import com.spotify.folsom.GetResult;
import com.spotify.folsom.Transcoder;
import com.spotify.folsom.client.OpCode;
import com.spotify.folsom.guava.HostAndPort;
import com.spotify.folsom.transcoder.StringTranscoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class BinaryMemcacheDecoderTest {

  protected static final int ID = 17;
  protected static final byte[] CAS = new byte[] {0, 0, 0, 0, 0, 0, 1, 2};
  protected static final byte[] NO_CAS = new byte[] {0, 0, 0, 0, 0, 0, 0, 0};
  private static final String KEY = "foo";
  private static final String VALUE = "bar";

  private static final Transcoder<String> TRANSCODER = StringTranscoder.UTF8_INSTANCE;

  @Test
  public void test() throws Exception {
    GetRequest request = new GetRequest(KEY.getBytes(StandardCharsets.UTF_8), OpCode.GET, 123);
    BinaryMemcacheDecoder decoder = new BinaryMemcacheDecoder();

    ByteBuf cb = Unpooled.buffer(30);
    cb.writeByte(0x81);
    cb.writeByte(OpCode.GET.value());
    cb.writeShort(3);
    cb.writeByte(0);
    cb.writeZero(1);
    cb.writeShort(0);
    cb.writeInt(6);
    cb.writeInt(request.opaque);
    cb.writeLong(258);
    cb.writeBytes(KEY.getBytes());
    cb.writeBytes(VALUE.getBytes());

    List<Object> out = new ArrayList<>();
    decoder.decode(null, cb, out);
    @SuppressWarnings("unchecked")
    List<ResponsePacket> replies = (List<ResponsePacket>) out.get(0);
    request.handle(replies, HostAndPort.fromHost("server"));

    GetResult<byte[]> getResult = request.get();
    assertEquals(258, getResult.getCas());
    assertEquals(VALUE, TRANSCODER.decode(getResult.getValue()));
  }
}
