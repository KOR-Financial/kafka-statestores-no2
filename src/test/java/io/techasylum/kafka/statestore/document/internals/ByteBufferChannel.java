package io.techasylum.kafka.statestore.document.internals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.kafka.common.network.TransferableChannel;

public class ByteBufferChannel implements TransferableChannel {
    private final ByteBuffer buf;
    private boolean closed = false;

    public ByteBufferChannel(long size) {
        if (size > Integer.MAX_VALUE)
            throw new IllegalArgumentException("size should be not be greater than Integer.MAX_VALUE");
        this.buf = ByteBuffer.allocate((int) size);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) {
        if ((offset < 0) || (length < 0) || (offset > srcs.length - length))
            throw new IndexOutOfBoundsException();
        int position = buf.position();
        int count = offset + length;
        for (int i = offset; i < count; i++) buf.put(srcs[i].duplicate());
        return buf.position() - position;
    }

    @Override
    public long write(ByteBuffer[] srcs) {
        return write(srcs, 0, srcs.length);
    }

    @Override
    public int write(ByteBuffer src) {
        return (int) write(new ByteBuffer[]{src});
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() {
        buf.flip();
        closed = true;
    }

    public ByteBuffer buffer() {
        return buf;
    }

    @Override
    public boolean hasPendingWrites() {
        return false;
    }

    @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        return fileChannel.transferTo(position, count, this);
    }
}
