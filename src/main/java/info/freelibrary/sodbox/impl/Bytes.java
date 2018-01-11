
package info.freelibrary.sodbox.impl;

import java.io.UnsupportedEncodingException;

import info.freelibrary.sodbox.StorageError;

class ArrayPos {

    byte[] body;

    int offs;

    ArrayPos(final byte[] body, final int offs) {
        this.body = body;
        this.offs = offs;
    }
}

//
// Class for packing/unpacking data
//
public class Bytes {

    public static void pack2(final byte[] arr, final int offs, final short val) {
        arr[offs] = (byte) (val >> 8);
        arr[offs + 1] = (byte) val;
    }

    public static void pack4(final byte[] arr, final int offs, final int val) {
        arr[offs] = (byte) (val >> 24);
        arr[offs + 1] = (byte) (val >> 16);
        arr[offs + 2] = (byte) (val >> 8);
        arr[offs + 3] = (byte) val;
    }

    public static void pack8(final byte[] arr, final int offs, final long val) {
        pack4(arr, offs, (int) (val >> 32));
        pack4(arr, offs + 4, (int) val);
    }

    public static void packF4(final byte[] arr, final int offs, final float val) {
        pack4(arr, offs, Float.floatToIntBits(val));
    }

    public static void packF8(final byte[] arr, final int offs, final double val) {
        pack8(arr, offs, Double.doubleToLongBits(val));
    }

    public static int packString(final byte[] arr, int offs, final String str, final String encoding) {
        if (str == null) {
            Bytes.pack4(arr, offs, -1);
            offs += 4;
        } else if (encoding == null) {
            final int n = str.length();
            Bytes.pack4(arr, offs, n);
            offs += 4;
            for (int i = 0; i < n; i++) {
                Bytes.pack2(arr, offs, (short) str.charAt(i));
                offs += 2;
            }
        } else {
            try {
                final byte[] bytes = str.getBytes(encoding);
                pack4(arr, offs, -2 - bytes.length);
                System.arraycopy(bytes, 0, arr, offs + 4, bytes.length);
                offs += 4 + bytes.length;
            } catch (final UnsupportedEncodingException x) {
                throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
            }
        }
        return offs;
    }

    public static int sizeof(final byte[] arr, final int offs) {
        final int len = unpack4(arr, offs);
        if (len >= 0) {
            return 4 + len * 2;
        } else if (len < -1) {
            return 4 - 2 - len;
        } else {
            return 4;
        }
    }

    public static int sizeof(final String str, final String encoding) {
        try {
            return str == null ? 4 : encoding == null ? 4 + str.length() * 2 : 4 + new String(str).getBytes(
                    encoding).length;
        } catch (final UnsupportedEncodingException x) {
            throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
        }
    }

    public static int skipString(final byte[] arr, int offs) {
        final int len = unpack4(arr, offs);
        offs += 4;
        if (len > 0) {
            offs += len * 2;
        } else if (len < -1) {
            offs -= len + 2;
        }
        return offs;
    }

    public static short unpack2(final byte[] arr, final int offs) {
        return (short) (arr[offs] << 8 | arr[offs + 1] & 0xFF);
    }

    public static int unpack4(final byte[] arr, final int offs) {
        return arr[offs] << 24 | (arr[offs + 1] & 0xFF) << 16 | (arr[offs + 2] & 0xFF) << 8 | arr[offs + 3] & 0xFF;
    }

    public static long unpack8(final byte[] arr, final int offs) {
        return (long) unpack4(arr, offs) << 32 | unpack4(arr, offs + 4) & 0xFFFFFFFFL;
    }

    public static float unpackF4(final byte[] arr, final int offs) {
        return Float.intBitsToFloat(Bytes.unpack4(arr, offs));
    }

    public static double unpackF8(final byte[] arr, final int offs) {
        return Double.longBitsToDouble(Bytes.unpack8(arr, offs));
    }

    public static String unpackString(final ArrayPos pos, final String encoding) {
        int offs = pos.offs;
        final byte[] arr = pos.body;
        int len = unpack4(arr, offs);
        offs += 4;
        String str = null;
        if (len >= 0) {
            final char[] chars = new char[len];
            for (int i = 0; i < len; i++) {
                chars[i] = (char) unpack2(arr, offs);
                offs += 2;
            }
            str = new String(chars);
        } else if (len < -1) {
            len = -len - 2;
            if (encoding != null) {
                try {
                    str = new String(arr, offs, len, encoding);
                } catch (final UnsupportedEncodingException x) {
                    throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
                }
            } else {
                str = new String(arr, offs, len);
            }
            offs += len;
        }
        pos.offs = offs;
        return str;
    }

    public static String unpackString(final byte[] arr, int offs, final String encoding) {
        int len = unpack4(arr, offs);
        offs += 4;
        String str = null;
        if (len >= 0) {
            final char[] chars = new char[len];
            for (int i = 0; i < len; i++) {
                chars[i] = (char) unpack2(arr, offs);
                offs += 2;
            }
            str = new String(chars);
        } else if (len < -1) {
            len = -len - 2;
            if (encoding != null) {
                try {
                    str = new String(arr, offs, len, encoding);
                } catch (final UnsupportedEncodingException x) {
                    throw new StorageError(StorageError.UNSUPPORTED_ENCODING);
                }
            } else {
                str = new String(arr, offs, len);
            }
            offs += len;
        }
        return str;
    }
}
