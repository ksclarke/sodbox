
package info.freelibrary.sodbox.impl;

import java.util.TreeMap;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.CustomAllocator;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

public class BitmapCustomAllocator extends Persistent implements CustomAllocator {

    static class BitmapPage extends Persistent {

        byte[] data;
    }

    static class Location implements Comparable {

        long pos;

        long size;

        Location(final long pos, final long size) {
            this.pos = pos;
            this.size = size;
        }

        @Override
        public int compareTo(final Object o) {
            final Location loc = (Location) o;
            return pos + size <= loc.pos ? -1 : loc.pos + loc.size <= pos ? 1 : 0;
        }
    }

    static final int BITMAP_PAGE_SIZE = Page.pageSize - ObjectHeader.sizeof - 4;

    static final int BITMAP_PAGE_BITS = BITMAP_PAGE_SIZE * 8;

    static final void memset(final BitmapPage pg, int offs, final int pattern, int len) {
        final byte[] arr = pg.data;
        final byte pat = (byte) pattern;
        while (--len >= 0) {
            arr[offs++] = pat;
        }
        pg.modify();
    }

    protected int quantum;

    protected int quantumBits;

    protected long base;

    protected long limit;

    protected Link pages;

    protected int extensionPages;

    transient int currPage;

    transient int currOffs;

    transient TreeMap reserved = new TreeMap();

    protected BitmapCustomAllocator() {
    }

    public BitmapCustomAllocator(final Storage storage, final int quantum, final long base, final long extension,
            final long limit) {
        super(storage);
        this.quantum = quantum;
        this.base = base;
        this.limit = limit;
        int bits = 0;
        for (int q = quantum; q != 1; q >>>= 1) {
            bits += 1;
        }
        quantumBits = bits;
        Assert.that(1 << bits == quantum);
        extensionPages = (int) ((extension + ((long) BITMAP_PAGE_BITS << quantumBits) - 1) /
                ((long) BITMAP_PAGE_BITS << quantumBits));
        pages = storage.createLink();
    }

    @Override
    public long allocate(long size) {
        size = size + quantum - 1 & ~(quantum - 1);
        final long objBitSize = size >> quantumBits;
        long pos;
        long holeBitSize = 0;
        int firstPage = currPage;
        int lastPage = pages.size();
        int offs = currOffs;
        long lastHoleSize = 0;

        while (true) {
            for (int i = firstPage; i < lastPage; i++) {
                BitmapPage pg = (BitmapPage) pages.get(i);
                while (offs < BITMAP_PAGE_SIZE) {
                    final int mask = pg.data[offs] & 0xFF;
                    if (holeBitSize + Bitmap.firstHoleSize[mask] >= objBitSize) {
                        pos = base + (((long) i * BITMAP_PAGE_SIZE + offs) * 8 - holeBitSize << quantumBits);
                        final long nextPos = wasReserved(pos, size);
                        if (nextPos != 0) {
                            final long quantNo = (nextPos - base >>> quantumBits) + 7;
                            i = (int) (quantNo / BITMAP_PAGE_BITS);
                            offs = (int) (quantNo - (long) i * BITMAP_PAGE_BITS) >> 3;
                            holeBitSize = 0;
                            continue;
                        }
                        currPage = i;
                        currOffs = offs;
                        pg.data[offs] |= (byte) ((1 << (int) (objBitSize - holeBitSize)) - 1);
                        pg.modify();
                        if (holeBitSize != 0) {
                            if (holeBitSize > offs * 8) {
                                memset(pg, 0, 0xFF, offs);
                                holeBitSize -= offs * 8;
                                pg = (BitmapPage) pages.get(--i);
                                offs = Page.pageSize;
                            }
                            while (holeBitSize > BITMAP_PAGE_BITS) {
                                memset(pg, 0, 0xFF, BITMAP_PAGE_SIZE);
                                holeBitSize -= BITMAP_PAGE_BITS;
                                pg = (BitmapPage) pages.get(--i);
                            }
                            while ((holeBitSize -= 8) > 0) {
                                pg.data[--offs] = (byte) 0xFF;
                            }
                            pg.data[offs - 1] |= (byte) ~((1 << -(int) holeBitSize) - 1);
                            pg.modify();
                        }
                        return pos;
                    } else if (Bitmap.maxHoleSize[mask] >= objBitSize) {
                        final int holeBitOffset = Bitmap.maxHoleOffset[mask];
                        pos = base + (((long) i * BITMAP_PAGE_SIZE + offs) * 8 + holeBitOffset << quantumBits);
                        final long nextPos = wasReserved(pos, size);
                        if (nextPos != 0) {
                            final long quantNo = (nextPos - base >>> quantumBits) + 7;
                            i = (int) (quantNo / BITMAP_PAGE_BITS);
                            offs = (int) (quantNo - (long) i * BITMAP_PAGE_BITS) >> 3;
                            holeBitSize = 0;
                            continue;
                        }
                        currPage = i;
                        currOffs = offs;
                        pg.data[offs] |= (byte) ((1 << (int) objBitSize) - 1 << holeBitOffset);
                        pg.modify();
                        return pos;
                    }
                    offs += 1;
                    if (Bitmap.lastHoleSize[mask] == 8) {
                        holeBitSize += 8;
                    } else {
                        holeBitSize = Bitmap.lastHoleSize[mask];
                    }
                }
                offs = 0;
            }
            if (firstPage == 0) {
                firstPage = pages.size();
                final int nPages = (int) (size / (BITMAP_PAGE_BITS * quantum));
                lastPage = firstPage + (nPages > extensionPages ? nPages : extensionPages);
                if ((long) lastPage * BITMAP_PAGE_BITS * quantum > limit) {
                    throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
                }
                pages.setSize(lastPage);
                for (int i = firstPage; i < lastPage; i++) {
                    final BitmapPage pg = new BitmapPage();
                    pg.data = new byte[BITMAP_PAGE_SIZE];
                    pages.setObject(i, pg);
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
    public void commit() {
        reserved.clear();
    }

    @Override
    public void free(final long pos, final long size) {
        reserve(pos, size);
        free0(pos, size);
    }

    private void free0(final long pos, final long size) {
        final long quantNo = pos - base >>> quantumBits;
        long objBitSize = size + quantum - 1 >>> quantumBits;
        int pageId = (int) (quantNo / BITMAP_PAGE_BITS);
        int offs = (int) (quantNo - (long) pageId * BITMAP_PAGE_BITS) >> 3;
        BitmapPage pg = (BitmapPage) pages.get(pageId);
        final int bitOffs = (int) quantNo & 7;

        if (objBitSize > 8 - bitOffs) {
            objBitSize -= 8 - bitOffs;
            pg.data[offs++] &= (1 << bitOffs) - 1;
            while (objBitSize + offs * 8 > BITMAP_PAGE_BITS) {
                memset(pg, offs, 0, BITMAP_PAGE_SIZE - offs);
                pg = (BitmapPage) pages.get(++pageId);
                objBitSize -= (BITMAP_PAGE_SIZE - offs) * 8;
                offs = 0;
            }
            while ((objBitSize -= 8) > 0) {
                pg.data[offs++] = (byte) 0;
            }
            pg.data[offs] &= (byte) ~((1 << (int) objBitSize + 8) - 1);
        } else {
            pg.data[offs] &= (byte) ~((1 << (int) objBitSize) - 1 << bitOffs);
        }
        pg.modify();
    }

    @Override
    public long getSegmentBase() {
        return base;
    }

    @Override
    public long getSegmentSize() {
        return limit;
    }

    @Override
    public long reallocate(long pos, final long oldSize, final long newSize) {
        getStorage();
        if ((newSize + quantum - 1 & ~(quantum - 1)) > (oldSize + quantum - 1 & ~(quantum - 1))) {
            final long newPos = allocate(newSize);
            free0(pos, oldSize);
            pos = newPos;
        }
        return pos;
    }

    private void reserve(final long pos, final long size) {
        final Location loc = new Location(pos, size);
        reserved.put(loc, loc);
    }

    private long wasReserved(final long pos, final long size) {
        final Location loc = new Location(pos, size);
        final Location r = (Location) reserved.get(loc);
        if (r != null) {
            return Math.max(pos + size, r.pos + r.size);
        }
        return 0;
    }
}
