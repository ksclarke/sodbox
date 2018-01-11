
package info.freelibrary.sodbox.impl;

class ObjectHeader {

    static final int sizeof = 8;

    static int getSize(final byte[] arr, final int offs) {
        return Bytes.unpack4(arr, offs);
    }

    static int getType(final byte[] arr, final int offs) {
        return Bytes.unpack4(arr, offs + 4);
    }

    static void setSize(final byte[] arr, final int offs, final int size) {
        Bytes.pack4(arr, offs, size);
    }

    static void setType(final byte[] arr, final int offs, final int type) {
        Bytes.pack4(arr, offs + 4, type);
    }
}
