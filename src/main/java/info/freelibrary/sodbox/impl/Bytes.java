
package info.freelibrary.sodbox.impl;

import java.io.UnsupportedEncodingException;

import info.freelibrary.sodbox.StorageError;

class ArrayPos {

    byte[] myBody;

    int myOffset;

    ArrayPos(final byte[] aBody, final int aOffset) {
        myBody = aBody;
        myOffset = aOffset;
    }
}

/**
 * Class for packing/unpacking data
 */
public final class Bytes {

    private Bytes() {
    }

    /**
     * Unpacks short.
     *
     * @param aBytes A byte array
     * @param aOffset An offset
     * @return An unpacked value
     */
    public static short unpack2(final byte[] aBytes, final int aOffset) {
        return (short) (aBytes[aOffset] << 8 | aBytes[aOffset + 1] & 0xFF);
    }

    /**
     * Unpacks integer.
     *
     * @param aBytes A byte array
     * @param aOffset An offset
     * @return An unpacked integer
     */
    @SuppressWarnings("BooleanExpressionComplexity")
    public static int unpack4(final byte[] aBytes, final int aOffset) {
        return aBytes[aOffset] << 24 | (aBytes[aOffset + 1] & 0xFF) << 16 | (aBytes[aOffset + 2] & 0xFF) << 8 |
                aBytes[aOffset + 3] & 0xFF;
    }

    /**
     * Unpacks long.
     *
     * @param aBytes A byte array
     * @param aOffset An offset
     * @return An unpacked long
     */
    public static long unpack8(final byte[] aBytes, final int aOffset) {
        return (long) unpack4(aBytes, aOffset) << 32 | unpack4(aBytes, aOffset + 4) & 0xFFFFFFFFL;
    }

    /**
     * Unpacks float.
     *
     * @param aBytes A byte array
     * @param aOffset An offset
     * @return An unpacked float
     */
    public static float unpackF4(final byte[] aBytes, final int aOffset) {
        return Float.intBitsToFloat(Bytes.unpack4(aBytes, aOffset));
    }

    /**
     * Unpacks double.
     *
     * @param abytes A byte array
     * @param aOffset An offset
     * @return An unpacked double
     */
    public static double unpackF8(final byte[] abytes, final int aOffset) {
        return Double.longBitsToDouble(Bytes.unpack8(abytes, aOffset));
    }

    /**
     * Skips string.
     *
     * @param aBytes A byte array
     * @param aOffset An offset
     * @return An skipped count
     */
    public static int skipString(final byte[] aBytes, final int aOffset) {
        final int length = unpack4(aBytes, aOffset);

        int offset = aOffset + 4;

        if (length > 0) {
            offset += length * 2;
        } else if (length < -1) {
            offset -= length + 2;
        }

        return offset;
    }

    /**
     * Unpacks string.
     *
     * @param aBytes A byte array
     * @param aOffset A offset
     * @param aEncoding An encoding
     * @return An unpacked string
     */
    public static String unpackString(final byte[] aBytes, final int aOffset, final String aEncoding) {
        int length = unpack4(aBytes, aOffset);
        int offset = aOffset + 4;
        String string = null;

        if (length >= 0) {
            final char[] chars = new char[length];

            for (int i = 0; i < length; i++) {
                chars[i] = (char) unpack2(aBytes, offset);
                offset += 2;
            }

            string = new String(chars);
        } else if (length < -1) {
            length = -length - 2;

            if (aEncoding != null) {
                try {
                    string = new String(aBytes, offset, length, aEncoding);
                } catch (final UnsupportedEncodingException details) {
                    throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
                }
            } else {
                string = new String(aBytes, offset, length);
            }

            offset += length;
        }

        return string;
    }

    /**
     * Unpacks string.
     *
     * @param aPosition An array position
     * @param aEncoding An encoding
     * @return A unpacked string
     */
    public static String unpackString(final ArrayPos aPosition, final String aEncoding) {
        final byte[] bytes = aPosition.myBody;

        int offset = aPosition.myOffset;
        int length = unpack4(bytes, offset);
        String string = null;

        offset += 4;

        if (length >= 0) {
            final char[] chars = new char[length];

            for (int i = 0; i < length; i++) {
                chars[i] = (char) unpack2(bytes, offset);
                offset += 2;
            }

            string = new String(chars);
        } else if (length < -1) {
            length = -length - 2;

            if (aEncoding != null) {
                try {
                    string = new String(bytes, offset, length, aEncoding);
                } catch (final UnsupportedEncodingException x) {
                    throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
                }
            } else {
                string = new String(bytes, offset, length);
            }

            offset += length;
        }

        aPosition.myOffset = offset;

        return string;
    }

    /**
     * Packs short.
     *
     * @param aBytes A byte array
     * @param aOffset An offset
     * @param aValue A short to pack
     */
    public static void pack2(final byte[] aBytes, final int aOffset, final short aValue) {
        aBytes[aOffset] = (byte) (aValue >> 8);
        aBytes[aOffset + 1] = (byte) aValue;
    }

    /**
     * Packs integer.
     *
     * @param aBytes An array
     * @param aOffset An offset
     * @param aValue An integer to pack
     */
    public static void pack4(final byte[] aBytes, final int aOffset, final int aValue) {
        aBytes[aOffset] = (byte) (aValue >> 24);
        aBytes[aOffset + 1] = (byte) (aValue >> 16);
        aBytes[aOffset + 2] = (byte) (aValue >> 8);
        aBytes[aOffset + 3] = (byte) aValue;
    }

    /**
     * Packs long.
     *
     * @param aBytes An array
     * @param aOffset An offset
     * @param aValue A long to pack
     */
    public static void pack8(final byte[] aBytes, final int aOffset, final long aValue) {
        pack4(aBytes, aOffset, (int) (aValue >> 32));
        pack4(aBytes, aOffset + 4, (int) aValue);
    }

    /**
     * Packs float.
     *
     * @param aBytes An array
     * @param aOffset An offset
     * @param aValue A float to pack
     */
    public static void packF4(final byte[] aBytes, final int aOffset, final float aValue) {
        pack4(aBytes, aOffset, Float.floatToIntBits(aValue));
    }

    /**
     * Packs double.
     *
     * @param aBytes An array
     * @param aOffset An offset
     * @param aValue A double to pack
     */
    public static void packF8(final byte[] aBytes, final int aOffset, final double aValue) {
        pack8(aBytes, aOffset, Double.doubleToLongBits(aValue));
    }

    /**
     * Packs string.
     *
     * @param aBytes A byte array
     * @param aOffset An offset
     * @param aString A String to pack
     * @param aEncoding An encoding
     * @return Packed space
     */
    public static int packString(final byte[] aBytes, final int aOffset, final String aString,
            final String aEncoding) {
        int offset = aOffset;

        if (aString == null) {
            Bytes.pack4(aBytes, offset, -1);
            offset += 4;
        } else if (aEncoding == null) {
            final int n = aString.length();

            Bytes.pack4(aBytes, offset, n);
            offset += 4;

            for (int i = 0; i < n; i++) {
                Bytes.pack2(aBytes, offset, (short) aString.charAt(i));
                offset += 2;
            }
        } else {
            try {
                final byte[] bytes = aString.getBytes(aEncoding);

                pack4(aBytes, offset, -2 - bytes.length);
                System.arraycopy(bytes, 0, aBytes, offset + 4, bytes.length);
                offset += 4 + bytes.length;
            } catch (final UnsupportedEncodingException x) {
                throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
            }
        }

        return offset;
    }

    /**
     * Size of string
     *
     * @param aString A string value
     * @param aEncoding An encoding
     * @return The size of the string
     */
    public static int sizeof(final String aString, final String aEncoding) {
        try {
            return aString == null ? 4 : aEncoding == null ? 4 + aString.length() * 2 : 4 + new String(aString)
                    .getBytes(aEncoding).length;
        } catch (final UnsupportedEncodingException x) {
            throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
        }
    }

    /**
     * A size of array value.
     *
     * @param aBytes A byte array
     * @param aOffset An offset
     * @return The size of the array value
     */
    public static int sizeof(final byte[] aBytes, final int aOffset) {
        final int length = unpack4(aBytes, aOffset);

        if (length >= 0) {
            return 4 + length * 2;
        } else if (length < -1) {
            return 4 - 2 - length;
        } else {
            return 4;
        }
    }

}
