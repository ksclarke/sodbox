
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import info.freelibrary.sodbox.SodboxOutputStream;
import info.freelibrary.sodbox.StorageError;

public class ByteBuffer {

    public byte[] myByteArray;

    public int myUsed;

    public String myEncoding;

    public Object myParent;

    public boolean isFinalized;

    public StorageImpl myStorage;

    ByteBuffer(final StorageImpl aStorage, final Object aParent, final boolean aFinalized) {
        this();
        myStorage = aStorage;
        myEncoding = aStorage.myEncoding;
        myParent = aParent;
        isFinalized = aFinalized;
    }

    ByteBuffer() {
        myByteArray = new byte[64];
    }

    /**
     * Gets output stream.
     *
     * @return An output stream
     */
    public SodboxOutputStream getOutputStream() {
        return new ByteBufferObjectOutputStream();
    }

    /**
     * Gets size.
     *
     * @return Size of the byte buffer
     */
    public int size() {
        return myUsed;
    }

    /**
     * Extend the byte buffer by the supplied size.
     *
     * @param aSize The amount to extend the buffer
     */
    public final void extend(final int aSize) {
        if (aSize > myByteArray.length) {
            final int newLen = aSize > myByteArray.length * 2 ? aSize : myByteArray.length * 2;
            final byte[] newArr = new byte[newLen];

            System.arraycopy(myByteArray, 0, newArr, 0, myUsed);

            myByteArray = newArr;
        }

        myUsed = aSize;
    }

    final byte[] toArray() {
        final byte[] result = new byte[myUsed];
        System.arraycopy(myByteArray, 0, result, 0, myUsed);
        return result;
    }

    int packI4(final int aDest, final int aValue) {
        extend(aDest + 4);
        Bytes.pack4(myByteArray, aDest, aValue);
        return aDest + 4;
    }

    int packString(final int aDest, final String aValue) {
        int dest = aDest;

        if (aValue == null) {
            extend(dest + 4);
            Bytes.pack4(myByteArray, dest, -1);
            dest += 4;
        } else {
            final int length = aValue.length();

            if (myEncoding == null) {
                extend(dest + 4 + 2 * length);
                Bytes.pack4(myByteArray, dest, length);
                dest += 4;

                for (int i = 0; i < length; i++) {
                    Bytes.pack2(myByteArray, dest, (short) aValue.charAt(i));
                    dest += 2;
                }
            } else {
                try {
                    final byte[] bytes = aValue.getBytes(myEncoding);

                    extend(dest + 4 + bytes.length);
                    Bytes.pack4(myByteArray, dest, -2 - bytes.length);
                    System.arraycopy(bytes, 0, myByteArray, dest + 4, bytes.length);
                    dest += 4 + bytes.length;
                } catch (final UnsupportedEncodingException details) {
                    throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
                }
            }
        }

        return dest;
    }

    class ByteBufferOutputStream extends OutputStream {

        @Override
        public void write(final int aByte) {
            write(new byte[] { (byte) aByte }, 0, 1);
        }

        @Override
        public void write(final byte[] aBytes, final int aOffset, final int aLength) {
            final int position = myUsed;

            extend(position + aLength);
            System.arraycopy(aBytes, aOffset, myByteArray, position, aLength);
        }
    }

    class ByteBufferObjectOutputStream extends SodboxOutputStream {

        ByteBufferObjectOutputStream() {
            super(new ByteBufferOutputStream());
        }

        @Override
        public void writeObject(final Object aObject) throws IOException {
            try {
                flush();
                myStorage.swizzle(ByteBuffer.this, myUsed, aObject);
            } catch (final Exception details) {
                throw new StorageError(StorageError.ACCESS_VIOLATION, details);
            }
        }

        @Override
        public void writeString(final String aString) throws IOException {
            flush();
            packString(myUsed, aString);
        }
    }

}
