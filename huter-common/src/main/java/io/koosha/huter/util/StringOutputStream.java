package io.koosha.huter.util;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class StringOutputStream extends OutputStream {

    private final Object LOCK = new Object();

    private final StringBuilder sb;
    private final Charset encoding;


    public static StringOutputStream forUtf8() {
        return new StringOutputStream(StandardCharsets.UTF_8);
    }

    public StringOutputStream(final Charset encoding) {
        this.sb = new StringBuilder();
        this.encoding = encoding;
    }


    @Override
    public String toString() {
        synchronized (LOCK) {
            return this.sb.toString();
        }
    }

    public Charset encoding() {
        return this.encoding;
    }

    @Override
    public void close() {
        synchronized (LOCK) {
            this.sb.setLength(0);
        }
    }

    @Override
    public void write(final int b) {
        synchronized (LOCK) {
            this.sb.append((char) b);
        }
    }

    @Override
    public void write(final byte[] b) {
        final char[] arr = new String(b, this.encoding).toCharArray();
        synchronized (LOCK) {
            this.sb.append(arr);
        }
    }

    @Override
    public void write(final byte[] b, int off, final int len) {
        if (off < 0 || len < 0 || off + len > b.length)
            throw new IndexOutOfBoundsException();

        final byte[] bytes = new byte[len];
        for (int i = 0; i < len; ++i, ++off)
            bytes[i] = b[off];

        this.write(bytes);
    }

    public StringOutputStream writeUtf8(final String utf8) {
        this.write(utf8.getBytes(StandardCharsets.UTF_8));
        return this;
    }

}
