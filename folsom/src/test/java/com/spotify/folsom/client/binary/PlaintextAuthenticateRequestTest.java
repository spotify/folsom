package com.spotify.folsom.client.binary;

import com.google.common.collect.Lists;
import com.spotify.folsom.client.MemcacheEncoder;
import com.spotify.folsom.client.OpCode;
import com.spotify.folsom.client.binary.PlaintextAuthenticateRequest;
import com.spotify.folsom.client.binary.RequestTestTemplate;
import io.netty.buffer.ByteBuf;
import java.util.List;
import org.junit.Test;

public class PlaintextAuthenticateRequestTest extends RequestTestTemplate {
  private static final String KEY = "PLAIN";

  private static final String USERNAME = "memcached";
  private static final String PASSWORD = "foobar";
  private static final String VALUE = '\u0000' + USERNAME + '\u0000' + PASSWORD;

  @Test
  public void testBuffer() throws Exception {
    PlaintextAuthenticateRequest req = new PlaintextAuthenticateRequest(USERNAME, PASSWORD);

    MemcacheEncoder memcacheEncoder = new MemcacheEncoder();
    List<Object> out = Lists.newArrayList();
    memcacheEncoder.encode(ctx, req, out);
    ByteBuf b = (ByteBuf) out.get(0);

    assertHeader(b, OpCode.SASL_AUTH, KEY.length(), 0, KEY.length() + VALUE.length(), req.opaque, 0);
    assertString(KEY, b);
    assertString(VALUE, b);
    assertEOM(b);
  }
}
