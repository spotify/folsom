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
import java.util.List;

public class AsciiMemcacheDecoder extends ByteToMessageDecoder {

  private final StringBuilder line = new StringBuilder();
  private boolean consumed = false;
  private boolean valueMode = false;

  private ValueAsciiResponse valueResponse = new ValueAsciiResponse();

  private String key = null;
  private byte[] value = null;
  private long cas = 0;
  private int valueOffset;

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
        final StringBuilder line = readLine(buf, readableBytes);
        if (line == null) {
          return;
        }
        if (line.length() > 0) {
          throw new IOException(String.format("Unexpected end of data block: %s", line));
        }
        valueResponse.addGetResult(key, value, cas);
        key = null;
        value = null;
        cas = 0;
      } else {
        final StringBuilder line = readLine(buf, readableBytes);
        if (line == null) {
          return;
        }

        final int firstEnd = endIndex(line, 0);
        if (firstEnd < 1) {
          throw new IOException("Unexpected line: " + line);
        }

        final char firstChar = line.charAt(0);

        if (Character.isDigit(firstChar)) {
          try {
            long numeric = Long.valueOf(line.toString());
            out.add(new NumericAsciiResponse(numeric));
          } catch (NumberFormatException e) {
            throw new IOException("Unexpected line: " + line, e);
          }
        } else if (firstEnd == 3) {
          expect(line, "END");
          out.add(valueResponse);
          valueResponse = new ValueAsciiResponse();
          valueMode = false;
          return;
        } else if (firstEnd == 5) {
          expect(line, "VALUE");
          valueMode = true;
          // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
          final int keyStart = firstEnd + 1;
          final int keyEnd = endIndex(line, keyStart);
          final String key = line.substring(keyStart, keyEnd);
          if (key.isEmpty()) {
            throw new IOException("Unexpected line: " + line);
          }

          final int flagsStart = keyEnd + 1;
          final int flagsEnd = endIndex(line, flagsStart);
          if (flagsEnd == flagsStart) {
            throw new IOException("Unexpected line: " + line);
          }

          final int sizeStart = flagsEnd + 1;
          final int sizeEnd = endIndex(line, sizeStart);
          final int size = (int) parseLong(line, sizeStart, sizeEnd);
          if (size < 0) {
            throw new IOException("Unexpected line: " + line);
          }

          final int casStart = sizeEnd + 1;
          final int casEnd = endIndex(line, casStart);
          long cas = 0;
          if (casStart != casEnd) {
            cas = parseLong(line, casStart, casEnd);
          }
          this.key = key;
          this.value = new byte[size];
          this.valueOffset = 0;
          this.cas = cas;
        } else if (valueMode) {
          // when in valueMode, the only valid responses are "END" and "VALUE"
          throw new IOException("Unexpected line: " + line);
        } else if (firstEnd == 6) {
          if (firstChar == 'S') {
            expect(line, "STORED");
            out.add(AsciiResponse.STORED);
            return;
          } else if (firstChar == 'E') {
            expect(line, "EXISTS");
            out.add(AsciiResponse.EXISTS);
            return;
          } else {
            throw new IOException("Unexpected line: " + line);
          }
        } else if (firstEnd == 7) {
          if (firstChar == 'T') {
            expect(line, "TOUCHED");
            out.add(AsciiResponse.TOUCHED);
            return;
          } else {
            expect(line, "DELETED");
            out.add(AsciiResponse.DELETED);
            return;
          }
        } else if (firstEnd == 9) {
          expect(line, "NOT_FOUND");
          out.add(AsciiResponse.NOT_FOUND);
          return;
        } else if (firstEnd == 10) {
          expect(line, "NOT_STORED");
          out.add(AsciiResponse.NOT_STORED);
          return;
        } else {
          throw new IOException("Unexpected line: " + line);
        }
      }
    }
  }

  private void expect(final StringBuilder line, final String compareTo) throws IOException {
    final int length = compareTo.length();
    for (int i = 0; i < length; i++) {
      if (line.charAt(i) != compareTo.charAt(i)) {
        throw new IOException("Unexpected line: " + line);
      }
    }
  }

  private long parseLong(final StringBuilder line, final int from, final int to) {
    if (from == to) {
      return -1;
    }
    long res = 0;
    for (int i = from; i < to; i++) {
      final int digit = line.charAt(i) - '0';
      if (digit < 0 || digit > 9) {
        return -1;
      }
      res *= 10;
      res += digit;
    }
    return res;
  }

  private int endIndex(final StringBuilder line, final int from) {
    final int length = line.length();
    for (int i = from; i < length; i++) {
      if (line.charAt(i) == ' ') {
        return i;
      }
    }
    return length;
  }

  private StringBuilder readLine(final ByteBuf buf, final int available) throws IOException {
    if (consumed) {
      line.setLength(0);
      consumed = false;
    }
    for (int i = 0; i < available - 1; i++) {
      final char b = (char) buf.readUnsignedByte();
      if (b == '\r') {
        if (buf.readUnsignedByte() == '\n') {
          consumed = true;
          return line;
        }
        throw new IOException("Expected newline, got something else");
      }
      line.append(b);
    }
    return null;
  }

}
