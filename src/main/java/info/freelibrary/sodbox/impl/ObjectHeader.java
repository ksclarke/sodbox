
package info.freelibrary.sodbox.impl;

final class ObjectHeader {

    static final int SIZE_OF = 8;

    private ObjectHeader() {
    }

    static int getSize(final byte[] aBytes, final int aOffset) {
        return Bytes.unpack4(aBytes, aOffset);
    }

    static void setSize(final byte[] aArray, final int aOffset, final int aSize) {
        Bytes.pack4(aArray, aOffset, aSize);
    }

    static int getType(final byte[] aBytes, final int aOffset) {
        return Bytes.unpack4(aBytes, aOffset + 4);
    }

    static void setType(final byte[] aArray, final int aOffset, final int aType) {
        Bytes.pack4(aArray, aOffset + 4, aType);
    }

}
