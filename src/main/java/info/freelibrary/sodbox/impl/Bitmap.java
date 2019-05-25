
package info.freelibrary.sodbox.impl;

/**
 * A bitmap.
 */
public final class Bitmap {

    static final byte FIRST_HOLE_SIZE[] = { 8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3,
        0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0,
        1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1,
        0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0,
        3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2,
        0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0,
        1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1,
        0, 3, 0, 1, 0, 2, 0, 1, 0 };

    static final byte LAST_HOLE_SIZE[] = { 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0 };

    static final byte MAX_HOLE_SIZE[] = { 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 3, 3, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2,
        2, 2, 6, 5, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 5, 4, 3,
        3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 7, 6, 5, 5, 4, 4, 4, 4,
        3, 3, 3, 3, 3, 3, 3, 3, 4, 3, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2,
        1, 1, 1, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 6, 5, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3,
        2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1, 1,
        1, 3, 2, 1, 1, 2, 1, 1, 0 };

    static final byte MAX_HOLE_OFFSET[] = { 0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 5, 5, 5, 5, 5, 5, 0,
        5, 5, 5, 5, 5, 5, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 6, 6, 0, 6, 6, 6, 0, 1, 2, 2, 0, 6, 6, 6, 0, 1, 6, 6, 0, 6,
        6, 6, 0, 1, 2, 2, 3, 3, 3, 3, 0, 1, 4, 4, 0, 4, 4, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 2,
        2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 2, 2, 3, 3, 3, 3,
        0, 4, 4, 4, 4, 4, 4, 4, 0, 1, 2, 2, 0, 5, 5, 5, 0, 1, 5, 5, 0, 5, 5, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0,
        1, 0, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 2, 2, 3, 3, 3, 3, 0, 1, 4, 4, 0, 4, 4, 4, 0, 1,
        2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1, 0,
        3, 0, 1, 0, 2, 0, 1, 0, 0 };

    private Bitmap() {
        super();
    }

    /**
     * Allocate for supplied bitmap.
     *
     * @param aBitmap A bitmap for which to allocate.
     * @param aBegin A beginning point
     * @param aEnd An ending point
     * @param aObjBitSize The object bit size
     * @return Amount allocated
     */
    public static long allocate(final byte[] aBitmap, final int aBegin, final int aEnd, final long aObjBitSize) {
        long holeBitSize = 0;

        for (int i = aBegin; i < aEnd; i++) {
            final int mask = aBitmap[i] & 0xFF;

            if (holeBitSize + FIRST_HOLE_SIZE[mask] >= aObjBitSize) {
                aBitmap[i] |= (byte) ((1 << (int) (aObjBitSize - holeBitSize)) - 1);

                final long pos = (long) i * 8 - holeBitSize;

                if (holeBitSize != 0) {
                    while ((holeBitSize -= 8) > 0) {
                        aBitmap[--i] = (byte) 0xFF;
                    }

                    aBitmap[i - 1] |= (byte) ~((1 << -(int) holeBitSize) - 1);
                }

                return pos;
            } else if (Bitmap.MAX_HOLE_SIZE[mask] >= aObjBitSize) {
                final int holeBitOffset = MAX_HOLE_OFFSET[mask];

                aBitmap[i] |= (byte) ((1 << (int) aObjBitSize) - 1 << holeBitOffset);

                return (long) i * 8 + holeBitOffset;
            } else {
                if (LAST_HOLE_SIZE[mask] == 8) {
                    holeBitSize += 8;
                } else {
                    holeBitSize = LAST_HOLE_SIZE[mask];
                }
            }
        }

        return -1;
    }

    /**
     * Locate end for bitmap.
     *
     * @param aBitmap A bitmap
     * @param aOffset An offset
     * @return The end for bitmap
     */
    public static int locateBitmapEnd(final byte[] aBitmap, final int aOffset) {
        int offset = aOffset;

        while (offset != 0 && aBitmap[--offset] == 0) {
        }

        return offset;
    }

    /**
     * Locate the hole's end.
     *
     * @param aBitmap A bitmap
     * @param aOffset An offset
     * @return The end of the hole
     */
    public static int locateHoleEnd(final byte[] aBitmap, final int aOffset) {
        int offset = aOffset;

        while (offset < aBitmap.length && aBitmap[offset++] == -1) {
        }

        return offset < aBitmap.length ? offset : aBitmap.length;
    }

    /**
     * Free space.
     *
     * @param aBitmap A bitmap
     * @param aObjBitPosition A object bit position
     * @param aObjBitSize A object bit size
     */
    public static void free(final byte[] aBitmap, final long aObjBitPosition, final long aObjBitSize) {
        final int bitOffs = (int) aObjBitPosition & 7;

        long objBitSize = aObjBitSize;
        int offs = (int) (aObjBitPosition >>> 3);

        if (objBitSize > 8 - bitOffs) {
            objBitSize -= 8 - bitOffs;
            aBitmap[offs++] &= (1 << bitOffs) - 1;

            while ((objBitSize -= 8) > 0) {
                aBitmap[offs++] = (byte) 0;
            }

            aBitmap[offs] &= (byte) ~((1 << (int) objBitSize + 8) - 1);
        } else {
            aBitmap[offs] &= (byte) ~((1 << (int) objBitSize) - 1 << bitOffs);
        }
    }

    /**
     * Reserve space.
     *
     * @param aBitmap A bitmap
     * @param aObjBitPosition A object bit position
     * @param aObjBitSize A object bit size
     */
    public static void reserve(final byte[] aBitmap, final long aObjBitPosition, final long aObjBitSize) {
        long objBitPosition = aObjBitPosition;
        long objBitSize = aObjBitSize;

        while (--objBitSize >= 0) {
            aBitmap[(int) (objBitPosition >>> 3)] |= 1 << (int) (objBitPosition & 7);
            objBitPosition += 1;
        }
    }

}
