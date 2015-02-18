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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

public class AsciiMemcacheDecoder extends ByteToMessageDecoder {

  private static final int MAX_RESPONSE_LINE = 500;

  private final ByteBuffer line = ByteBuffer.allocate(MAX_RESPONSE_LINE);
  private final ByteBuffer token = ByteBuffer.allocate(MAX_RESPONSE_LINE);
  private final Charset charset;

  private boolean valueMode = false;

  private ValueAsciiResponse valueResponse = new ValueAsciiResponse();

  private boolean consumed;

  private byte[] key = null;
  private byte[] value = null;
  private long cas = 0;
  private int valueOffset;

  public AsciiMemcacheDecoder(Charset charset) {
    this.charset = charset;
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf buf,
                        final List<Object> out) throws Exception {
    while (true) {
      int readableBytes = buf.readableBytes();
      if (readableBytes == 0) {
        return;
      }

      if (key != null) {
        final int toCopy = Math.min(value.length - valueOffset, readableBytes);
        if (toCopy > 0) {
          buf.readBytes(value, valueOffset, toCopy);
          readableBytes -= toCopy;
          valueOffset += toCopy;
          if (valueOffset < value.length) {
            return;
          }
        }
        final ByteBuffer line = readLine(buf, readableBytes);
        if (line == null) {
          return;
        }
        if (line.remaining() > 0) {
          throw new IOException(String.format("Unexpected end of data block: %s", lineToString()));
        }
        valueResponse.addGetResult(key, value, cas);
        key = null;
        value = null;
        cas = 0;
      } else {
        final ByteBuffer line = readLine(buf, readableBytes);
        if (line == null) {
          return;
        }
        readNextToken();
        int tokenLength = token.remaining();
        if (tokenLength < 1) {
          throw fail();
        }

        final byte firstChar = token.get();

        if (Character.isDigit(firstChar)) {
          try {
            long numeric = parseLong(firstChar, token);
            out.add(new NumericAsciiResponse(numeric));
          } catch (NumberFormatException e) {
            throw new IOException("Unexpected line: " + line, e);
          }
        } else if (tokenLength == 3) {
          expect(firstChar, "END");
          out.add(valueResponse);
          valueResponse = new ValueAsciiResponse();
          valueMode = false;
          return;
        } else if (tokenLength == 5) {
          expect(firstChar, "VALUE");
          valueMode = true;
          // VALUE <key> <flags> <bytes> [<cas unique>]\r\n

          // key
          readNextToken();
          int keyLen = token.remaining();
          if (keyLen <= 0) {
            throw fail();
          }
          byte[] key = new byte[token.remaining()];
          token.get(key);

          // flags
          readNextToken();
          int flagLen = token.remaining();
          if (flagLen <= 0) {
            throw fail();
          }

          // size
          readNextToken();
          int sizeLen = token.remaining();
          if (sizeLen <= 0) {
            throw fail();
          }
          final int size = (int) parseLong(token.get(), token);

          // cas
          readNextToken();
          int casLen = token.remaining();
          long cas = 0;
          if (casLen > 0) {
            cas = parseLong(token.get(), token);
          }

          this.key = key;
          this.value = new byte[size];
          this.valueOffset = 0;
          this.cas = cas;
        } else if (valueMode) {
          // when in valueMode, the only valid responses are "END" and "VALUE"
          throw fail();
        } else if (tokenLength == 6) {
          if (firstChar == 'S') {
            expect(firstChar, "STORED");
            out.add(AsciiResponse.STORED);
            return;
          } else {
            expect(firstChar, "EXISTS");
            out.add(AsciiResponse.EXISTS);
            return;
          }
        } else if (tokenLength == 7) {
          if (firstChar == 'T') {
            expect(firstChar, "TOUCHED");
            out.add(AsciiResponse.TOUCHED);
            return;
          } else {
            expect(firstChar, "DELETED");
            out.add(AsciiResponse.DELETED);
            return;
          }
        } else if (tokenLength == 9) {
          expect(firstChar, "NOT_FOUND");
          out.add(AsciiResponse.NOT_FOUND);
          return;
        } else if (tokenLength == 10) {
          expect(firstChar, "NOT_STORED");
          out.add(AsciiResponse.NOT_STORED);
          return;
        } else {
          throw fail();
        }
      }
    }
  }

  private void readNextToken() {
    token.clear();
    while (line.hasRemaining()) {
      byte b = line.get();
      if (b == ' ') {
        break;
      }
      token.put(b);
    }
    token.flip();
  }

  private IOException fail() {
    return new IOException("Unexpected line: " + lineToString());
  }

  private String lineToString() {
    line.rewind();
    return new String(line.array(), 0, line.remaining(), charset);
  }

  private void expect(byte firstChar, final String compareTo) throws IOException {
    if (firstChar != compareTo.charAt(0)) {
      throw fail();
    }
    final int length = compareTo.length();
    if (length != token.remaining() + 1) {
      throw fail();
    }
    for (int i = 1; i < length; i++) {
      if (token.get() != compareTo.charAt(i)) {
        throw fail();
      }

    }
  }

  private long parseLong(byte firstChar, ByteBuffer token) throws IOException {
    // firstChar must be guarantee to be a digit.
    long res = firstChar - '0';
    if (res < 0 || res > 9) {
      throw fail();
    }
    while (token.hasRemaining()) {
      final int digit = token.get() - '0';
      if (digit < 0 || digit > 9) {
        throw fail();
      }
      res *= 10;
      res += digit;
    }
    return res;
  }

  private ByteBuffer readLine(final ByteBuf buf, final int available) throws IOException {
    if (consumed) {
      consumed = false;
      line.clear();
    }
    for (int i = 0; i < available - 1; i++) {
      final byte b = buf.readByte();
      if (b == '\r') {
        if (buf.readByte() == '\n') {
          consumed = true;
          line.flip();
          return line;
        }
        throw new IOException("Expected newline, got something else");
      }
      line.put(b);
    }
    return null;
  }
}
