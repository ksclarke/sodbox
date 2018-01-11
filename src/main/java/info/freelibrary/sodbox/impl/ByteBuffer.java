
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import info.freelibrary.sodbox.SodboxOutputStream;
import info.freelibrary.sodbox.StorageError;

public class ByteBuffer {

    class ByteBufferObjectOutputStream extends SodboxOutputStream {

        ByteBufferObjectOutputStream() {
            super(new ByteBufferOutputStream());
        }

        @Override
        public void writeObject(final Object obj) throws IOException {
            try {
                flush();
                db.swizzle(ByteBuffer.this, used, obj);
            } catch (final Exception x) {
                throw new StorageError(StorageError.ACCESS_VIOLATION, x);
            }
        }

        @Override
        public void writeString(final String str) throws IOException {
            flush();
            packString(used, str);
        }
    }

    class ByteBufferOutputStream extends OutputStream {

        @Override
        public void write(final byte b[], final int off, final int len) {
            final int pos = used;
            extend(pos + len);
            System.arraycopy(b, off, arr, pos, len);
        }

        @Override
        public void write(final int b) {
            write(new byte[] { (byte) b }, 0, 1);
        }
    }

    public byte[] arr;

    public int used;

    public String encoding;

    public Object parent;

    public boolean finalized;

    public StorageImpl db;

    ByteBuffer() {
        arr = new byte[64];
    }

    ByteBuffer(final StorageImpl db, final Object parent, final boolean finalized) {
        this();
        this.db = db;
        encoding = db.myEncoding;
        this.parent = parent;
        this.finalized = finalized;
    }

    public final void extend(final int size) {
        if (size > arr.length) {
            final int newLen = size > arr.length * 2 ? size : arr.length * 2;
            final byte[] newArr = new byte[newLen];
            System.arraycopy(arr, 0, newArr, 0, used);
            arr = newArr;
        }
        used = size;
    }

    public SodboxOutputStream getOutputStream() {
        return new ByteBufferObjectOutputStream();
    }

    int packI4(final int dst, final int value) {
        extend(dst + 4);
        Bytes.pack4(arr, dst, value);
        return dst + 4;
    }

    int packString(int dst, final String value) {
        if (value == null) {
            extend(dst + 4);
            Bytes.pack4(arr, dst, -1);
            dst += 4;
        } else {
            final int length = value.length();
            if (encoding == null) {
                extend(dst + 4 + 2 * length);
                Bytes.pack4(arr, dst, length);
                dst += 4;
                for (int i = 0; i < length; i++) {
                    Bytes.pack2(arr, dst, (short) value.charAt(i));
                    dst += 2;
                }
            } else {
                try {
                    final byte[] bytes = value.getBytes(encoding);
                    extend(dst + 4 + bytes.length);
                    Bytes.pack4(arr, dst, -2 - bytes.length);
                    System.arraycopy(bytes, 0, arr, dst + 4, bytes.length);
                    dst += 4 + bytes.length;
                } catch (final UnsupportedEncodingException x) {
                    throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
                }
            }
        }
        return dst;
    }

    public int size() {
        return used;
    }

    final byte[] toArray() {
        final byte[] result = new byte[used];
        System.arraycopy(arr, 0, result, 0, used);
        return result;
    }
}
