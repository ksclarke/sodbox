
package info.freelibrary.sodbox.impl;

import java.util.TreeMap;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.CustomAllocator;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

public class BitmapCustomAllocator extends Persistent implements CustomAllocator {

    static final int BITMAP_PAGE_SIZE = Page.PAGE_SIZE - ObjectHeader.SIZE_OF - 4;

    static final int BITMAP_PAGE_BITS = BITMAP_PAGE_SIZE * 8;

    protected int myQuantum;

    protected int myQuantumBits;

    protected long myBase;

    protected long myLimit;

    protected Link myPages;

    protected int myExtensionPages;

    transient int myCurrentPage;

    transient int myCurrentOffset;

    transient TreeMap myReserved = new TreeMap();

    /**
     * A custom bitmap allocator.
     *
     * @param aStorage A database storage
     * @param aQuantum A quantum
     * @param aBase A base
     * @param aExtension An extension
     * @param aLimit A limit
     */
    public BitmapCustomAllocator(final Storage aStorage, final int aQuantum, final long aBase, final long aExtension,
            final long aLimit) {
        super(aStorage);

        myQuantum = aQuantum;
        myBase = aBase;
        myLimit = aLimit;

        int bits = 0;

        for (int q = aQuantum; q != 1; q >>>= 1) {
            bits += 1;
        }

        myQuantumBits = bits;
        Assert.that(1 << bits == aQuantum);

        myExtensionPages = (int) ((aExtension + ((long) BITMAP_PAGE_BITS << myQuantumBits) - 1) /
                ((long) BITMAP_PAGE_BITS << myQuantumBits));
        myPages = aStorage.createLink();
    }

    protected BitmapCustomAllocator() {
    }

    @Override
    public long getSegmentBase() {
        return myBase;
    }

    @Override
    public long getSegmentSize() {
        return myLimit;
    }

    @SuppressWarnings("unchecked")
    @Override
    public long allocate(final long aSize) {
        final long objBitSize;

        int firstPage = myCurrentPage;
        int lastPage = myPages.size();
        int offset = myCurrentOffset;
        long lastHoleSize = 0;
        long holeBitSize = 0;
        long size = aSize;
        long position;

        size = size + myQuantum - 1 & ~(myQuantum - 1);
        objBitSize = size >> myQuantumBits;

        while (true) {
            for (int i = firstPage; i < lastPage; i++) {
                BitmapPage page = (BitmapPage) myPages.get(i);

                while (offset < BITMAP_PAGE_SIZE) {
                    final int mask = page.myData[offset] & 0xFF;

                    if (holeBitSize + Bitmap.FIRST_HOLE_SIZE[mask] >= objBitSize) {
                        position = myBase + (((long) i * BITMAP_PAGE_SIZE + offset) * 8 -
                                holeBitSize << myQuantumBits);

                        final long nextPos = wasReserved(position, size);

                        if (nextPos != 0) {
                            final long quantNo = (nextPos - myBase >>> myQuantumBits) + 7;

                            i = (int) (quantNo / BITMAP_PAGE_BITS);
                            offset = (int) (quantNo - (long) i * BITMAP_PAGE_BITS) >> 3;
                            holeBitSize = 0;

                            continue;
                        }

                        myCurrentPage = i;
                        myCurrentOffset = offset;
                        page.myData[offset] |= (byte) ((1 << (int) (objBitSize - holeBitSize)) - 1);
                        page.modify();

                        if (holeBitSize != 0) {
                            if (holeBitSize > offset * 8) {
                                memset(page, 0, 0xFF, offset);
                                holeBitSize -= offset * 8;
                                page = (BitmapPage) myPages.get(--i);
                                offset = Page.PAGE_SIZE;
                            }

                            while (holeBitSize > BITMAP_PAGE_BITS) {
                                memset(page, 0, 0xFF, BITMAP_PAGE_SIZE);
                                holeBitSize -= BITMAP_PAGE_BITS;
                                page = (BitmapPage) myPages.get(--i);
                            }

                            while ((holeBitSize -= 8) > 0) {
                                page.myData[--offset] = (byte) 0xFF;
                            }

                            page.myData[offset - 1] |= (byte) ~((1 << -(int) holeBitSize) - 1);
                            page.modify();
                        }

                        return position;
                    } else if (Bitmap.MAX_HOLE_SIZE[mask] >= objBitSize) {
                        final int holeBitOffset = Bitmap.MAX_HOLE_OFFSET[mask];
                        position = myBase + (((long) i * BITMAP_PAGE_SIZE + offset) * 8 +
                                holeBitOffset << myQuantumBits);
                        final long nextPos = wasReserved(position, size);

                        if (nextPos != 0) {
                            final long quantNo = (nextPos - myBase >>> myQuantumBits) + 7;

                            i = (int) (quantNo / BITMAP_PAGE_BITS);
                            offset = (int) (quantNo - (long) i * BITMAP_PAGE_BITS) >> 3;
                            holeBitSize = 0;

                            continue;
                        }

                        myCurrentPage = i;
                        myCurrentOffset = offset;
                        page.myData[offset] |= (byte) ((1 << (int) objBitSize) - 1 << holeBitOffset);
                        page.modify();

                        return position;
                    }

                    offset += 1;

                    if (Bitmap.LAST_HOLE_SIZE[mask] == 8) {
                        holeBitSize += 8;
                    } else {
                        holeBitSize = Bitmap.LAST_HOLE_SIZE[mask];
                    }
                }

                offset = 0;
            }

            if (firstPage == 0) {
                final int numOfPages;

                firstPage = myPages.size();
                numOfPages = (int) (size / (BITMAP_PAGE_BITS * myQuantum));
                lastPage = firstPage + (numOfPages > myExtensionPages ? numOfPages : myExtensionPages);

                if ((long) lastPage * BITMAP_PAGE_BITS * myQuantum > myLimit) {
                    throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
                }

                myPages.setSize(lastPage);

                for (int index = firstPage; index < lastPage; index++) {
                    final BitmapPage page = new BitmapPage();

                    page.myData = new byte[BITMAP_PAGE_SIZE];
                    myPages.setObject(index, page);
                }

                holeBitSize = lastHoleSize;
            } else {
                lastHoleSize = holeBitSize;
                holeBitSize = 0;
                lastPage = firstPage + 1;
                firstPage = 0;
            }
        }
    }

    @Override
    public long reallocate(final long aPosition, final long aOldSize, final long aNewSize) {
        long position = aPosition;

        if ((aNewSize + myQuantum - 1 & ~(myQuantum - 1)) > (aOldSize + myQuantum - 1 & ~(myQuantum - 1))) {
            final long newPos = allocate(aNewSize);

            free0(position, aOldSize);
            position = newPos;
        }

        return position;
    }

    @Override
    public void free(final long aPosition, final long aSize) {
        reserve(aPosition, aSize);
        free0(aPosition, aSize);
    }

    private long wasReserved(final long aPosition, final long aSize) {
        final Location loc = new Location(aPosition, aSize);
        final Location r = (Location) myReserved.get(loc);
        if (r != null) {
            return Math.max(aPosition + aSize, r.myPosition + r.mySize);
        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    private void reserve(final long aPosition, final long aSize) {
        final Location loc = new Location(aPosition, aSize);

        myReserved.put(loc, loc);
    }

    private void free0(final long aPosition, final long aSize) {
        final long quantNo = aPosition - myBase >>> myQuantumBits;
        long objBitSize = aSize + myQuantum - 1 >>> myQuantumBits;
        int pageId = (int) (quantNo / BITMAP_PAGE_BITS);
        int offs = (int) (quantNo - (long) pageId * BITMAP_PAGE_BITS) >> 3;
        BitmapPage page = (BitmapPage) myPages.get(pageId);
        final int bitOffs = (int) quantNo & 7;

        if (objBitSize > 8 - bitOffs) {
            objBitSize -= 8 - bitOffs;
            page.myData[offs++] &= (1 << bitOffs) - 1;

            while (objBitSize + offs * 8 > BITMAP_PAGE_BITS) {
                memset(page, offs, 0, BITMAP_PAGE_SIZE - offs);
                page = (BitmapPage) myPages.get(++pageId);
                objBitSize -= (BITMAP_PAGE_SIZE - offs) * 8;
                offs = 0;
            }

            while ((objBitSize -= 8) > 0) {
                page.myData[offs++] = (byte) 0;
            }

            page.myData[offs] &= (byte) ~((1 << (int) objBitSize + 8) - 1);
        } else {
            page.myData[offs] &= (byte) ~((1 << (int) objBitSize) - 1 << bitOffs);
        }

        page.modify();
    }

    static final void memset(final BitmapPage aPage, final int aOffset, final int aPattern, final int aLength) {
        final byte[] bytes = aPage.myData;
        final byte pat = (byte) aPattern;

        int length = aLength;
        int offset = aOffset;

        while (--length >= 0) {
            bytes[offset++] = pat;
        }

        aPage.modify();
    }

    @Override
    public void commit() {
        myReserved.clear();
    }

    static class Location implements Comparable {

        long myPosition;

        long mySize;

        Location(final long aPosition, final long aSize) {
            myPosition = aPosition;
            mySize = aSize;
        }

        @Override
        public int compareTo(final Object aObject) {
            final Location loc = (Location) aObject;

            return myPosition + mySize <= loc.myPosition ? -1 : loc.myPosition + loc.mySize <= myPosition ? 1 : 0;
        }
    }

    static class BitmapPage extends Persistent {

        byte[] myData;

    }
}
