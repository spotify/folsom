//CHECKSTYLE:OFF
/**
 * This file is adopted from https://code.google.com/p/jmemcache-daemon/ in order to fix a ascii
 * fragmentation issue: https://code.google.com/p/jmemcache-daemon/issues/detail?id=32
 */

package com.thimbleware.jmemcached.protocol.text;

import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.protocol.CommandMessage;
import com.thimbleware.jmemcached.protocol.Op;
import com.thimbleware.jmemcached.protocol.SessionStatus;
import com.thimbleware.jmemcached.protocol.exceptions.IncorrectlyTerminatedPayloadException;
import com.thimbleware.jmemcached.protocol.exceptions.InvalidProtocolStateException;
import com.thimbleware.jmemcached.protocol.exceptions.MalformedCommandException;
import com.thimbleware.jmemcached.protocol.exceptions.UnknownCommandException;
import com.thimbleware.jmemcached.util.BufferUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferIndexFinder;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * The MemcachedCommandDecoder is responsible for taking lines from the MemcachedFrameDecoder and parsing them
 * into CommandMessage instances for handling by the MemcachedCommandHandler
 * <p/>
 * Protocol status is held in the SessionStatus instance which is shared between each of the decoders in the pipeline.
 */
public final class MemcachedCommandDecoder extends FrameDecoder {

    private static final int MIN_BYTES_LINE = 2;
    private SessionStatus status;

    private static final ChannelBuffer NOREPLY = ChannelBuffers.wrappedBuffer("noreply".getBytes());


    public MemcachedCommandDecoder(SessionStatus status) {
        this.status = status;
    }

    /**
     * Index finder which locates a byte which is neither a {@code CR ('\r')}
     * nor a {@code LF ('\n')}.
     */
    static ChannelBufferIndexFinder CRLF_OR_WS = (buffer, guessedIndex) -> {
        byte b = buffer.getByte(guessedIndex);
        return b == ' ' || b == '\r' || b == '\n';
    };

    static boolean eol(int pos, ChannelBuffer buffer) {
        return buffer.readableBytes() - pos >= MIN_BYTES_LINE && buffer.getByte(buffer.readerIndex() + pos) == '\r' && buffer.getByte(buffer.readerIndex() + pos+1) == '\n';
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        if (status.state == SessionStatus.State.READY) {
            ChannelBuffer in = buffer.slice();

            // split into pieces
            List<ChannelBuffer> pieces = new ArrayList<>(6);
            if (in.readableBytes() < MIN_BYTES_LINE) return null;
            int pos = in.bytesBefore(CRLF_OR_WS);
            boolean eol = false;
            do {
                if (pos != -1) {
                    eol = eol(pos, in);
                    int skip = eol ? MIN_BYTES_LINE : 1;
                    ChannelBuffer slice = in.slice(in.readerIndex(), pos);
                    slice.readerIndex(0);
                    pieces.add(slice);
                    in.skipBytes(pos + skip);
                    if (eol) break;
                }
            } while ((pos = in.bytesBefore(CRLF_OR_WS)) != -1);
            if (eol) {
                buffer.skipBytes(in.readerIndex());

                return processLine(pieces, channel, ctx);
            }
            if (status.state != SessionStatus.State.WAITING_FOR_DATA) status.ready();
        } else if (status.state == SessionStatus.State.WAITING_FOR_DATA) {
            if (buffer.readableBytes() >= status.bytesNeeded + MemcachedResponseEncoder.CRLF.capacity()) {

                // verify delimiter matches at the right location
                ChannelBuffer dest = buffer.slice(buffer.readerIndex() + status.bytesNeeded, MIN_BYTES_LINE);

                if (!dest.equals(MemcachedResponseEncoder.CRLF)) {
                    // before we throw error... we're ready for the next command
                    status.ready();

                    // error, no delimiter at end of payload
                    throw new IncorrectlyTerminatedPayloadException("payload not terminated correctly");
                } else {
                    status.processingMultiline();

                    // There's enough bytes in the buffer and the delimiter is at the end. Read it.
                    ChannelBuffer result = buffer.copy(buffer.readerIndex(), status.bytesNeeded);

                    buffer.skipBytes(status.bytesNeeded + MemcachedResponseEncoder.CRLF.capacity());

                    CommandMessage commandMessage = continueSet(channel, status, result, ctx);

                    if (status.state != SessionStatus.State.WAITING_FOR_DATA) status.ready();

                    return commandMessage;
                }
            }
        } else {
            throw new InvalidProtocolStateException("invalid protocol state");
        }
        return null;
    }

    /**
     * Process an individual complete protocol line and either passes the command for processing by the
     * session handler, or (in the case of SET-type commands) partially parses the command and sets the session into
     * a state to wait for additional data.
     *
     * @param parts                 the (originally space separated) parts of the command
     * @param channel               the netty channel to operate on
     * @param channelHandlerContext the netty channel handler context
     * @throws com.thimbleware.jmemcached.protocol.exceptions.MalformedCommandException
     * @throws com.thimbleware.jmemcached.protocol.exceptions.UnknownCommandException
     */
    private Object processLine(List<ChannelBuffer> parts, Channel channel, ChannelHandlerContext channelHandlerContext) throws UnknownCommandException, MalformedCommandException {
        final int numParts = parts.size();

        // Turn the command into an enum for matching on
        Op op;
        try {
            op = Op.FindOp(parts.get(0));
            if (op == null)
                throw new IllegalArgumentException("unknown operation: " + parts.get(0).toString());
        } catch (IllegalArgumentException e) {
            throw new UnknownCommandException("unknown operation: " + parts.get(0).toString());
        }

        // Produce the initial command message, for filling in later
        CommandMessage cmd = CommandMessage.command(op);


        switch (op) {
            case DELETE:
                cmd.setKey(parts.get(1));

                if (numParts >= MIN_BYTES_LINE) {
                    if (parts.get(numParts - 1).equals(NOREPLY)) {
                        cmd.noreply = true;
                        if (numParts == 4)
                            cmd.time = BufferUtils.atoi(parts.get(MIN_BYTES_LINE));
                    } else if (numParts == 3)
                        cmd.time = BufferUtils.atoi(parts.get(MIN_BYTES_LINE));
                }

                return cmd;
            case DECR:
            case INCR:
                // Malformed
                if (numParts < MIN_BYTES_LINE || numParts > 3)
                    throw new MalformedCommandException("invalid increment command");

                cmd.setKey(parts.get(1));
                cmd.incrAmount = BufferUtils.atoi(parts.get(MIN_BYTES_LINE));

                if (numParts == 3 && parts.get(MIN_BYTES_LINE).equals(NOREPLY)) {
                    cmd.noreply = true;
                }

                return cmd;
            case FLUSH_ALL:
                if (numParts >= 1) {
                    if (parts.get(numParts - 1).equals(NOREPLY)) {
                        cmd.noreply = true;
                        if (numParts == 3)
                            cmd.time = BufferUtils.atoi((parts.get(1)));
                    } else if (numParts == MIN_BYTES_LINE)
                        cmd.time = BufferUtils.atoi((parts.get(1)));
                }
                return cmd;
            case VERBOSITY: // verbosity <time> [noreply]\r\n
                // Malformed
                if (numParts < MIN_BYTES_LINE || numParts > 3)
                    throw new MalformedCommandException("invalid verbosity command");

                cmd.time = BufferUtils.atoi((parts.get(1))); // verbose level

                if (numParts > 1 && parts.get(MIN_BYTES_LINE).equals(NOREPLY))
                    cmd.noreply = true;

                return cmd;
            case APPEND:
            case PREPEND:
            case REPLACE:
            case ADD:
            case SET:
            case CAS:
                // if we don't have all the parts, it's malformed
                if (numParts < 5) {
                    throw new MalformedCommandException("invalid command length");
                }

                // Fill in all the elements of the command
                int size = BufferUtils.atoi(parts.get(4));
                int expire = BufferUtils.atoi(parts.get(3));
                int flags = BufferUtils.atoi(parts.get(MIN_BYTES_LINE));
                cmd.element = new LocalCacheElement(new Key(parts.get(1).slice()), flags, expire != 0 && expire < CacheElement.THIRTY_DAYS ? LocalCacheElement.Now() + expire : expire, 0L);

                // look for cas and "noreply" elements
                if (numParts > 5) {
                    int noreply = op == Op.CAS ? 6 : 5;
                    if (op == Op.CAS) {
                        cmd.cas_key = BufferUtils.atol(parts.get(5));
                    }

                    if (numParts == noreply + 1 && parts.get(noreply).equals(NOREPLY))
                        cmd.noreply = true;
                }

                // Now indicate that we need more for this command by changing the session status's state.
                // This instructs the frame decoder to start collecting data for us.
                status.needMore(size, cmd);
                break;

            //
            case GET:
            case GETS:
            case STATS:
            case VERSION:
            case QUIT:
                // Get all the keys
                cmd.setKeys(parts.subList(1, numParts));

                // Pass it on.
                return cmd;
            default:
                throw new UnknownCommandException("unknown command: " + op);
        }

        return null;
    }

    /**
     * Handles the continuation of a SET/ADD/REPLACE command with the data it was waiting for.
     *
     * @param channel               netty channel
     * @param state                 the current session status (unused)
     * @param remainder             the bytes picked up
     * @param channelHandlerContext netty channel handler context
     */
    private CommandMessage continueSet(Channel channel, SessionStatus state, ChannelBuffer remainder, ChannelHandlerContext channelHandlerContext) {
        state.cmd.element.setData(remainder);
        return state.cmd;
    }
}
