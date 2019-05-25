
package info.freelibrary.sodbox.impl;

import java.util.Arrays;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.IFile;

class PagePool {

    static final int INFINITE_POOL_INITIAL_SIZE = 8;

    LRU myLRU;

    Page myFreePages;

    Page myHashTable[];

    int myPoolSize;

    boolean isAutoExtended;

    IFile myFile;

    long myLruLimit;

    int myDirtyPagesCount;

    Page myDirtyPages[];

    boolean isFlushing;

    /**
     * Creates a page pool.
     *
     * @param poolSize
     * @param aLruLimit
     */
    PagePool(final int aPoolSize, final long aLruLimit) {
        if (aPoolSize == 0) {
            isAutoExtended = true;
            myPoolSize = INFINITE_POOL_INITIAL_SIZE;
        } else {
            myPoolSize = aPoolSize;
        }

        myLruLimit = aLruLimit;
    }

    final Page find(final long aAddress, final int aState) {
        final int pageNo = (int) (aAddress >>> Page.PAGE_SIZE_LOG);

        int hashCode = pageNo % myPoolSize;
        Page page;

        synchronized (this) {
            for (page = myHashTable[hashCode]; page != null; page = page.myCollisionChain) {
                if (page.myOffset == aAddress) {
                    if (page.myAccessCount++ == 0) {
                        page.unlink();
                    }

                    break;
                }
            }

            if (page == null) {
                page = myFreePages;

                if (page != null) {
                    if (page.myData == null) {
                        page.myData = new byte[Page.PAGE_SIZE];
                    }

                    myFreePages = (Page) page.myNext;
                } else if (isAutoExtended) {
                    if (pageNo >= myPoolSize) {
                        final int newPoolSize = pageNo >= myPoolSize * 2 ? pageNo + 1 : myPoolSize * 2;
                        final Page[] newHashTable = new Page[newPoolSize];

                        System.arraycopy(myHashTable, 0, newHashTable, 0, myHashTable.length);

                        myHashTable = newHashTable;
                        myPoolSize = newPoolSize;
                    }

                    page = new Page();
                    page.myData = new byte[Page.PAGE_SIZE];
                    hashCode = pageNo;
                } else {
                    Assert.that("unfixed page available", myLRU.myPrevious != myLRU);

                    page = (Page) myLRU.myPrevious;
                    page.unlink();

                    synchronized (page) {
                        if ((page.myState & Page.PS_DIRTY) != 0) {
                            page.myState = 0;
                            myFile.write(page.myOffset, page.myData);

                            if (!isFlushing) {
                                myDirtyPages[page.myWriteQueueIndex] = myDirtyPages[--myDirtyPagesCount];
                                myDirtyPages[page.myWriteQueueIndex].myWriteQueueIndex = page.myWriteQueueIndex;
                            }
                        }
                    }

                    final int h = (int) (page.myOffset >> Page.PAGE_SIZE_LOG) % myPoolSize;

                    Page current = myHashTable[h];
                    Page previous = null;

                    while (current != page) {
                        previous = current;
                        current = current.myCollisionChain;
                    }

                    if (previous == null) {
                        myHashTable[h] = page.myCollisionChain;
                    } else {
                        previous.myCollisionChain = page.myCollisionChain;
                    }
                }

                page.myAccessCount = 1;
                page.myOffset = aAddress;
                page.myState = Page.PS_RAW;
                page.myCollisionChain = myHashTable[hashCode];
                myHashTable[hashCode] = page;
            }

            if ((page.myState & Page.PS_DIRTY) == 0 && (aState & Page.PS_DIRTY) != 0) {
                Assert.that(!isFlushing);

                if (myDirtyPagesCount >= myDirtyPages.length) {
                    final Page[] newDirtyPages = new Page[myDirtyPagesCount * 2];

                    System.arraycopy(myDirtyPages, 0, newDirtyPages, 0, myDirtyPages.length);

                    myDirtyPages = newDirtyPages;
                }

                myDirtyPages[myDirtyPagesCount] = page;
                page.myWriteQueueIndex = myDirtyPagesCount++;
                page.myState |= Page.PS_DIRTY;
            }

            if ((page.myState & Page.PS_RAW) != 0) {
                if (myFile.read(page.myOffset, page.myData) < Page.PAGE_SIZE) {
                    for (int i = 0; i < Page.PAGE_SIZE; i++) {
                        page.myData[i] = 0;
                    }
                }

                page.myState &= ~Page.PS_RAW;
            }
        }

        return page;
    }

    final synchronized void copy(final long aDest, final long aSrc, final long aSize) {
        long dest = aDest;
        long src = aSrc;
        long size = aSize;
        int dstOffs = (int) dest & Page.PAGE_SIZE - 1;
        int srcOffs = (int) src & Page.PAGE_SIZE - 1;

        dest -= dstOffs;
        src -= srcOffs;

        Page dstPage = find(dest, Page.PS_DIRTY);
        Page srcPage = find(src, 0);

        do {
            if (dstOffs == Page.PAGE_SIZE) {
                unfix(dstPage);

                dest += Page.PAGE_SIZE;
                dstPage = find(dest, Page.PS_DIRTY);
                dstOffs = 0;
            }

            if (srcOffs == Page.PAGE_SIZE) {
                unfix(srcPage);

                src += Page.PAGE_SIZE;
                srcPage = find(src, 0);
                srcOffs = 0;
            }

            long len = size;

            if (len > Page.PAGE_SIZE - srcOffs) {
                len = Page.PAGE_SIZE - srcOffs;
            }

            if (len > Page.PAGE_SIZE - dstOffs) {
                len = Page.PAGE_SIZE - dstOffs;
            }

            System.arraycopy(srcPage.myData, srcOffs, dstPage.myData, dstOffs, (int) len);

            srcOffs += len;
            dstOffs += len;
            size -= len;
        } while (size != 0);

        unfix(dstPage);
        unfix(srcPage);
    }

    final void write(final long aDestPosition, final byte[] aSrc) {
        Assert.that((aDestPosition & Page.PAGE_SIZE - 1) == 0);
        Assert.that((aSrc.length & Page.PAGE_SIZE - 1) == 0);

        long destPosition = aDestPosition;

        for (int index = 0; index < aSrc.length;) {
            final Page page = find(destPosition, Page.PS_DIRTY);
            final byte[] dest = page.myData;

            for (int j = 0; j < Page.PAGE_SIZE; j++) {
                dest[j] = aSrc[index++];
            }

            unfix(page);
            destPosition += Page.PAGE_SIZE;
        }
    }

    final void open(final IFile aFile) {
        myFile = aFile;
        reset();
    }

    final void reset() {
        myLRU = new LRU();
        myFreePages = null;
        myHashTable = new Page[myPoolSize];
        myDirtyPages = new Page[myPoolSize];
        myDirtyPagesCount = 0;

        if (!isAutoExtended) {
            for (int i = myPoolSize; --i >= 0;) {
                final Page page = new Page();

                page.myNext = myFreePages;
                myFreePages = page;
            }
        }
    }

    final void clear() {
        Assert.that(myDirtyPagesCount == 0);
        reset();
    }

    final synchronized void close() {
        myFile.close();
        myHashTable = null;
        myDirtyPages = null;
        myLRU = null;
        myFreePages = null;
    }

    final synchronized void unfix(final Page aPage) {
        Assert.that(aPage.myAccessCount > 0);

        if (--aPage.myAccessCount == 0) {
            if (aPage.myOffset <= myLruLimit) {
                myLRU.link(aPage);
            } else {
                myLRU.myPrevious.link(aPage);
            }
        }
    }

    final synchronized void modify(final Page aPage) {
        Assert.that(aPage.myAccessCount > 0);

        if ((aPage.myState & Page.PS_DIRTY) == 0) {
            Assert.that(!isFlushing);

            aPage.myState |= Page.PS_DIRTY;

            if (myDirtyPagesCount >= myDirtyPages.length) {
                final Page[] newDirtyPages = new Page[myDirtyPagesCount * 2];

                System.arraycopy(myDirtyPages, 0, newDirtyPages, 0, myDirtyPages.length);

                myDirtyPages = newDirtyPages;
            }

            myDirtyPages[myDirtyPagesCount] = aPage;
            aPage.myWriteQueueIndex = myDirtyPagesCount++;
        }
    }

    final Page getPage(final long aAddress) {
        return find(aAddress, 0);
    }

    final Page putPage(final long aAddress) {
        return find(aAddress, Page.PS_DIRTY);
    }

    final byte[] get(final long aPosition) {
        Assert.that(aPosition != 0);

        long position = aPosition;
        int offset = (int) position & Page.PAGE_SIZE - 1;
        Page page = find(position - offset, 0);
        int size = ObjectHeader.getSize(page.myData, offset);

        Assert.that(size >= ObjectHeader.SIZE_OF);

        final byte[] obj = new byte[size];

        int dest = 0;

        while (size > Page.PAGE_SIZE - offset) {
            System.arraycopy(page.myData, offset, obj, dest, Page.PAGE_SIZE - offset);
            unfix(page);

            size -= Page.PAGE_SIZE - offset;
            position += Page.PAGE_SIZE - offset;
            dest += Page.PAGE_SIZE - offset;
            page = find(position, 0);
            offset = 0;
        }

        System.arraycopy(page.myData, offset, obj, dest, size);
        unfix(page);

        return obj;
    }

    final void put(final long aPosition, final byte[] aObject) {
        put(aPosition, aObject, aObject.length);
    }

    final void put(final long aPosition, final byte[] aBytes, final int aSize) {
        int size = aSize;
        long position = aPosition;
        int offset = (int) position & Page.PAGE_SIZE - 1;
        Page page = find(position - offset, Page.PS_DIRTY);
        int src = 0;

        while (size > Page.PAGE_SIZE - offset) {
            System.arraycopy(aBytes, src, page.myData, offset, Page.PAGE_SIZE - offset);
            unfix(page);

            size -= Page.PAGE_SIZE - offset;
            position += Page.PAGE_SIZE - offset;
            src += Page.PAGE_SIZE - offset;
            page = find(position, Page.PS_DIRTY);
            offset = 0;
        }

        System.arraycopy(aBytes, src, page.myData, offset, size);
        unfix(page);
    }

    void flush() {
        synchronized (this) {
            isFlushing = true;
            Arrays.sort(myDirtyPages, 0, myDirtyPagesCount);
        }

        for (int index = 0; index < myDirtyPagesCount; index++) {
            final Page page = myDirtyPages[index];

            synchronized (page) {
                if ((page.myState & Page.PS_DIRTY) != 0) {
                    myFile.write(page.myOffset, page.myData);
                    page.myState &= ~Page.PS_DIRTY;
                }
            }
        }

        myFile.sync();
        myDirtyPagesCount = 0;
        isFlushing = false;
    }

}
