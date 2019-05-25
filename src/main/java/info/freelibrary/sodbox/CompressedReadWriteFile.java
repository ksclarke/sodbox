
package info.freelibrary.sodbox;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import info.freelibrary.sodbox.impl.Bitmap;
import info.freelibrary.sodbox.impl.Bytes;
import info.freelibrary.sodbox.impl.Page;

/**
 * Compressed read-write database file. To work with compressed database file you should pass instance of this class
 * in <code>Storage.open</code> method
 */
public class CompressedReadWriteFile implements IFile {

    static final int ALLOCATION_QUANTUM_LOG = 9;

    static final int ALLOCATION_QUANTUM = 1 << ALLOCATION_QUANTUM_LOG;

    static final long MAX_PAGE_MAP_SIZE = 1000000;

    private static final String READ = "r";

    private static final String READWRITE = "rw";

    byte[] myBitmap;

    int myBitmapPosition;

    int myBitmapStart;

    int myBitmapExtensionQuantum;

    long myPageIndexSize;

    long myPageIndexCheckpointThreshold;

    Deflater myDeflater;

    Inflater myInflater;

    byte[] myCompressionBuffer;

    RandomAccessFile myDataFile;

    RandomAccessFile myPageIndexFile;

    RandomAccessFile myPageIndexLogFile;

    FileChannel myDataChannel;

    FileChannel myPageIndexChannel;

    MappedByteBuffer myPageIndexBuffer;

    PageMap myPageMap;

    boolean isNoFlush;

    FileLock myFileLock;

    byte[] myPattern;

    /**
     * Constructor of compressed file with default parameter values
     *
     * @param aDataFilePath path to the data file
     */
    public CompressedReadWriteFile(final String aDataFilePath) {
        this(aDataFilePath, null);
    }

    /**
     * Constructor of compressed file with default parameter values
     *
     * @param aDataFilePath path to the data file
     * @param aCipherKey cipher key (if null, then no encryption is performed)
     */
    public CompressedReadWriteFile(final String aDataFilePath, final String aCipherKey) {
        this(aDataFilePath, aDataFilePath + ".map", aDataFilePath + ".log", 8 * 1024 * 1024, 1024 * 1024, 1024 * 1024,
                false, false, aCipherKey);
    }

    /**
     * Constructor of compressed file
     *
     * @param aDataFilePath path to the data file
     * @param aPageIndexFilePath path to the page index file
     * @param aPageIndexLogFilePath path to the transaction log file for page index
     * @param aDataFileExtensionQuantum quantum of extending data file
     * @param aPageIndexCheckpointThreshold maximal size of page index log file, after reaching this size page index
     *        is flushed and log is truncated
     * @param aPageIndexInitSize initial size of page index
     * @param aReadOnly whether access to the file is read-only
     * @param aNoFlush whether synchronous write to the disk should be performed
     * @param aCipherKey cipher key (if null, then no encryption is performed)
     */
    public CompressedReadWriteFile(final String aDataFilePath, final String aPageIndexFilePath,
            final String aPageIndexLogFilePath, final long aDataFileExtensionQuantum,
            final long aPageIndexCheckpointThreshold, final long aPageIndexInitSize, final boolean aReadOnly,
            final boolean aNoFlush, final String aCipherKey) {
        this.myPageIndexCheckpointThreshold = aPageIndexCheckpointThreshold;
        this.isNoFlush = aNoFlush;

        if (aCipherKey != null) {
            setKey(aCipherKey.getBytes());
        }

        try {
            myDataFile = new RandomAccessFile(aDataFilePath, aReadOnly ? READ : READWRITE);
            myDataChannel = myDataFile.getChannel();

            myPageIndexFile = new RandomAccessFile(aPageIndexFilePath, aReadOnly ? READ : READWRITE);
            myPageIndexChannel = myPageIndexFile.getChannel();
            long size = myPageIndexChannel.size();
            myPageIndexSize = aReadOnly || size > aPageIndexInitSize ? size : aPageIndexInitSize;
            myPageIndexBuffer = myPageIndexChannel.map(aReadOnly ? FileChannel.MapMode.READ_ONLY
                    : FileChannel.MapMode.READ_WRITE, 0, // position
                    myPageIndexSize);
            myDeflater = new Deflater();
            myInflater = new Inflater();
            myCompressionBuffer = new byte[Page.PAGE_SIZE];

            if (!aReadOnly) {
                long pageMapSize = aPageIndexInitSize / 8;
                if (pageMapSize > MAX_PAGE_MAP_SIZE) {
                    pageMapSize = MAX_PAGE_MAP_SIZE;
                }
                myPageMap = new PageMap((int) pageMapSize);
                myPageIndexLogFile = new RandomAccessFile(aPageIndexLogFilePath, READWRITE);
                performRecovery();

                myBitmapExtensionQuantum = (int) (aDataFileExtensionQuantum >>> ALLOCATION_QUANTUM_LOG + 3);
                myBitmap = new byte[(int) (myDataChannel.size() >>> ALLOCATION_QUANTUM_LOG + 3) +
                        myBitmapExtensionQuantum];
                myBitmapPosition = myBitmapStart = Page.PAGE_SIZE >>> ALLOCATION_QUANTUM_LOG + 3;

                final byte[] buf = new byte[8];
                size = myPageIndexChannel.size();
                myPageIndexBuffer.position(0);

                while ((size -= 8) >= 0) {
                    myPageIndexBuffer.get(buf, 0, 8);
                    final long pagePos = Bytes.unpack8(buf, 0);
                    final long pageBitOffs = pagePos >>> Page.PAGE_SIZE_LOG + ALLOCATION_QUANTUM_LOG;
                    final long pageBitSize = (pagePos & Page.PAGE_SIZE - 1) +
                            ALLOCATION_QUANTUM >>> ALLOCATION_QUANTUM_LOG;
                    Bitmap.reserve(myBitmap, pageBitOffs, pageBitSize);
                }
            }
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    @Override
    public void write(final long aPageAddress, final byte[] aBuffer) {
        byte[] buffer = aBuffer;

        try {
            long pageOffs = 0;
            int pageSize = buffer.length;

            if (aPageAddress != 0) {
                Assert.that(pageSize == Page.PAGE_SIZE);
                Assert.that((aPageAddress & Page.PAGE_SIZE - 1) == 0);

                long pagePos = myPageMap.get(aPageAddress);
                boolean firstUpdate = false;

                if (pagePos == 0) {
                    final int bp = (int) (aPageAddress >>> Page.PAGE_SIZE_LOG - 3);

                    if (bp + 8 <= myPageIndexSize) {
                        final byte[] posBuf = new byte[8];
                        myPageIndexBuffer.position(bp);
                        myPageIndexBuffer.get(posBuf, 0, 8);
                        pagePos = Bytes.unpack8(posBuf, 0);
                    }

                    firstUpdate = true;
                }

                pageSize = ((int) pagePos & Page.PAGE_SIZE - 1) + 1;
                myDeflater.reset();
                myDeflater.setInput(buffer, 0, buffer.length);
                myDeflater.finish();

                final int newPageSize = myDeflater.deflate(myCompressionBuffer);

                if (newPageSize == Page.PAGE_SIZE) {
                    System.arraycopy(buffer, 0, myCompressionBuffer, 0, newPageSize);
                }

                buffer = myCompressionBuffer;

                final int newPageBitSize = newPageSize + ALLOCATION_QUANTUM - 1 >>> ALLOCATION_QUANTUM_LOG;
                final int oldPageBitSize = pageSize + ALLOCATION_QUANTUM - 1 >>> ALLOCATION_QUANTUM_LOG;

                if (firstUpdate || newPageBitSize != oldPageBitSize) {
                    if (!firstUpdate) {
                        Bitmap.free(myBitmap, pagePos >>> Page.PAGE_SIZE_LOG + ALLOCATION_QUANTUM_LOG, oldPageBitSize);
                    }

                    pageOffs = allocate(newPageBitSize);
                } else {
                    pageOffs = pagePos >>> Page.PAGE_SIZE_LOG;
                }

                pageSize = newPageSize;
                myPageMap.put(aPageAddress, pageOffs << Page.PAGE_SIZE_LOG | pageSize - 1, pagePos);
                crypt(buffer, pageSize);
            }

            myDataFile.seek(pageOffs);
            myDataFile.write(buffer, 0, pageSize);
        } catch (final IOException details) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, details);
        }
    }

    @Override
    public int read(final long aPageAddress, final byte[] aBuffer) {
        try {
            if (aPageAddress != 0) {
                Assert.that((aPageAddress & Page.PAGE_SIZE - 1) == 0);

                long pagePos = 0;

                if (myPageMap != null) {
                    pagePos = myPageMap.get(aPageAddress);
                }

                if (pagePos == 0) {
                    final int bp = (int) (aPageAddress >>> Page.PAGE_SIZE_LOG - 3);

                    if (bp + 8 <= myPageIndexSize) {
                        final byte[] posBuf = new byte[8];

                        myPageIndexBuffer.position(bp);
                        myPageIndexBuffer.get(posBuf, 0, 8);
                        pagePos = Bytes.unpack8(posBuf, 0);
                    }
                    if (pagePos == 0) {
                        return 0;
                    }
                }

                myDataFile.seek(pagePos >>> Page.PAGE_SIZE_LOG);

                final int size = ((int) pagePos & Page.PAGE_SIZE - 1) + 1;

                int rc = myDataFile.read(myCompressionBuffer, 0, size);

                if (rc != size) {
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR);
                }

                crypt(myCompressionBuffer, size);

                if (size < Page.PAGE_SIZE) {
                    myInflater.reset();
                    myInflater.setInput(myCompressionBuffer, 0, size);
                    rc = myInflater.inflate(aBuffer);

                    Assert.that(rc == Page.PAGE_SIZE);
                } else {
                    System.arraycopy(myCompressionBuffer, 0, aBuffer, 0, rc);
                }

                return rc;
            } else {
                myDataFile.seek(0);
                return myDataFile.read(aBuffer, 0, aBuffer.length);
            }
        } catch (final Exception x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    @Override
    public void sync() {
        try {
            // Flush data to the main database file
            if (!isNoFlush) {
                myDataFile.getFD().sync();
            }

            final int txSize = myPageMap.size();

            if (txSize == 0) {
                return;
            }

            // Make sure that new page mapping is saved in transaction log
            final byte[] buf = new byte[4 + 16 * txSize];

            Bytes.pack4(buf, 0, txSize);
            int pos = 4;

            for (final PageMap.Entry e : myPageMap) {
                Bytes.pack8(buf, pos, e.myAddress);
                Bytes.pack8(buf, pos + 8, e.myNewPosition);
                pos += 16;
            }

            myPageIndexLogFile.write(buf, 0, pos);

            if (!isNoFlush) {
                myPageIndexLogFile.getFD().sync();
            }

            // Store new page mapping in the page index file
            for (final PageMap.Entry e : myPageMap) {
                setPosition(e.myAddress);
                Bytes.pack8(buf, 0, e.myNewPosition);
                myPageIndexBuffer.put(buf, 0, 8);

                if (e.myOldPosition != 0) {
                    Bitmap.free(myBitmap, e.myOldPosition >>> Page.PAGE_SIZE_LOG + ALLOCATION_QUANTUM_LOG,
                            (e.myOldPosition & Page.PAGE_SIZE - 1) + ALLOCATION_QUANTUM >>> ALLOCATION_QUANTUM_LOG);
                }
            }

            myPageMap.clear();

            // Truncate log if necessary
            if (myPageIndexLogFile.length() > myPageIndexCheckpointThreshold) {
                myPageIndexBuffer.force();
                myPageIndexLogFile.setLength(0);
            }
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    @Override
    public boolean tryLock(final boolean aShared) {
        try {
            myFileLock = myDataChannel.tryLock(0, Long.MAX_VALUE, aShared);
            return myFileLock != null;
        } catch (final IOException x) {
            return true;
        }
    }

    @Override
    public void lock(final boolean aShared) {
        try {
            myFileLock = myDataChannel.lock(0, Long.MAX_VALUE, aShared);
        } catch (final IOException x) {
            throw new StorageError(StorageError.LOCK_FAILED, x);
        }
    }

    @Override
    public void unlock() {
        try {
            myFileLock.release();
        } catch (final IOException x) {
            throw new StorageError(StorageError.LOCK_FAILED, x);
        }
    }

    @Override
    public void close() {
        try {
            myDataChannel.close();
            myDataFile.close();

            if (myPageIndexLogFile != null) {
                Assert.that(myPageMap.size() == 0);

                myPageIndexBuffer.force();
                myPageIndexLogFile.setLength(0);
                myPageIndexLogFile.close();
            }

            myPageIndexChannel.close();
            myPageIndexFile.close();
        } catch (final IOException x) {
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    @Override
    public long length() {
        try {
            return myDataChannel.size();
        } catch (final IOException x) {
            return -1;
        }
    }

    long allocate(final int aBitSize) {
        long pos = Bitmap.allocate(myBitmap, myBitmapPosition, myBitmap.length, aBitSize);
        if (pos < 0) {
            pos = Bitmap.allocate(myBitmap, myBitmapStart, Bitmap.locateHoleEnd(myBitmap, myBitmapPosition),
                    aBitSize);
            if (pos < 0) {
                final byte[] newBitmap = new byte[myBitmap.length + myBitmapExtensionQuantum];
                System.arraycopy(myBitmap, 0, newBitmap, 0, myBitmap.length);
                pos = Bitmap.allocate(newBitmap, Bitmap.locateBitmapEnd(newBitmap, myBitmap.length), newBitmap.length,
                        aBitSize);
                Assert.that(pos >= 0);
                myBitmap = newBitmap;
            }
        }

        myBitmapPosition = (int) (pos + aBitSize >>> 3);

        return pos << ALLOCATION_QUANTUM_LOG;
    }

    void setPosition(final long aAddress) throws IOException {
        final long pos = aAddress >>> Page.PAGE_SIZE_LOG - 3;

        if (pos + 8 > myPageIndexSize) {
            do {
                myPageIndexSize *= 2;
            } while (pos + 8 > myPageIndexSize);

            myPageIndexBuffer = myPageIndexChannel.map(FileChannel.MapMode.READ_WRITE, 0, myPageIndexSize);
        }

        myPageIndexBuffer.position((int) pos);
    }

    void performRecovery() throws IOException {
        final byte[] hdr = new byte[4];

        while (myPageIndexLogFile.read(hdr, 0, 4) == 4) {
            final int nPages = Bytes.unpack4(hdr, 0);
            final byte[] buf = new byte[nPages * 16];

            if (myPageIndexLogFile.read(buf, 0, buf.length) == buf.length) {
                for (int i = 0; i < nPages; i++) {
                    setPosition(Bytes.unpack8(buf, i * 16));
                    myPageIndexBuffer.put(buf, i * 16 + 8, 8);
                }
            } else {
                break;
            }
        }
    }

    private void setKey(final byte[] aKey) {
        final byte[] state = new byte[256];

        for (int counter = 0; counter < 256; ++counter) {
            state[counter] = (byte) counter;
        }

        int index1 = 0;
        int index2 = 0;

        for (int counter = 0; counter < 256; ++counter) {
            index2 = aKey[index1] + state[counter] + index2 & 0xff;
            final byte temp = state[counter];
            state[counter] = state[index2];
            state[index2] = temp;
            index1 = (index1 + 1) % aKey.length;
        }

        myPattern = new byte[Page.PAGE_SIZE];
        int x = 0;
        int y = 0;

        for (int i = 0; i < Page.PAGE_SIZE; i++) {
            x = x + 1 & 0xff;
            y = y + state[x] & 0xff;
            final byte temp = state[x];
            state[x] = state[y];
            state[y] = temp;
            myPattern[i] = state[state[x] + state[y] & 0xff];
        }
    }

    private void crypt(final byte[] aBuffer, final int aLength) {
        for (int i = 0; i < aLength; i++) {
            aBuffer[i] ^= myPattern[i];
        }
    }

    static class PageMap implements Iterable<PageMap.Entry> {

        static final float LOAD_FACTOR = 0.75f;

        static final int PRIME_NUMBERS[] = { 17, /* 0 */
            37, /* 1 */
            79, /* 2 */
            163, /* 3 */
            331, /* 4 */
            673, /* 5 */
            1361, /* 6 */
            2729, /* 7 */
            5471, /* 8 */
            10949, /* 9 */
            21911, /* 10 */
            43853, /* 11 */
            87719, /* 12 */
            175447, /* 13 */
            350899, /* 14 */
            701819, /* 15 */
            1403641, /* 16 */
            2807303, /* 17 */
            5614657, /* 18 */
            11229331, /* 19 */
            22458671, /* 20 */
            44917381, /* 21 */
            89834777, /* 22 */
            179669557, /* 23 */
            359339171, /* 24 */
            718678369, /* 25 */
            1437356741, /* 26 */
            2147483647 /* 27 (largest signed int prime) */
        };

        Entry myTable[];

        int myCount;

        int myTableSizePrime;

        int myTableSize;

        int myThreshold;

        PageMap(final int aInitialCapacity) {
            for (myTableSizePrime = 0; PRIME_NUMBERS[myTableSizePrime] < aInitialCapacity; myTableSizePrime++) {
                //
            }
            myTableSize = PRIME_NUMBERS[myTableSizePrime];
            myThreshold = (int) (myTableSize * LOAD_FACTOR);
            myTable = new Entry[myTableSize];
        }

        public void put(final long addr, final long aNewPosition, final long aOldPosition) {
            Entry tab[] = myTable;

            int index = (int) ((addr >>> Page.PAGE_SIZE_LOG) % myTableSize);

            for (Entry e = tab[index]; e != null; e = e.myNext) {
                if (e.myAddress == addr) {
                    e.myNewPosition = aNewPosition;
                    return;
                }
            }

            if (myCount >= myThreshold) {
                // Rehash the table if the threshold is exceeded
                rehash();
                tab = myTable;
                index = (int) ((addr >>> Page.PAGE_SIZE_LOG) % myTableSize);
            }

            // Creates the new entry.
            tab[index] = new Entry(addr, aNewPosition, aOldPosition, tab[index]);
            myCount += 1;
        }

        public long get(final long aAddress) {
            final int index = (int) ((aAddress >>> Page.PAGE_SIZE_LOG) % myTableSize);

            for (Entry entry = myTable[index]; entry != null; entry = entry.myNext) {
                if (entry.myAddress == aAddress) {
                    return entry.myNewPosition;
                }
            }

            return 0;
        }

        public void clear() {
            final Entry tab[] = myTable;
            final int size = myTableSize;

            for (int i = 0; i < size; i++) {
                tab[i] = null;
            }

            myCount = 0;
        }

        void rehash() {
            final int oldCapacity = myTableSize;
            final int newCapacity = myTableSize = PRIME_NUMBERS[++myTableSizePrime];
            final Entry oldMap[] = myTable;
            final Entry newMap[] = new Entry[newCapacity];

            myThreshold = (int) (newCapacity * LOAD_FACTOR);
            myTable = newMap;
            myTableSize = newCapacity;

            for (int i = 0; i < oldCapacity; i++) {
                for (Entry old = oldMap[i]; old != null;) {
                    final Entry e = old;
                    old = old.myNext;
                    final int index = (int) ((e.myAddress >>> Page.PAGE_SIZE_LOG) % newCapacity);
                    e.myNext = newMap[index];
                    newMap[index] = e;
                }
            }
        }

        @Override
        public Iterator<Entry> iterator() {
            return new PageMapIterator();
        }

        public int size() {
            return myCount;
        }

        class PageMapIterator implements Iterator<Entry> {

            Entry myCurrent;

            int myIndex;

            PageMapIterator() {
                moveForward();
            }

            @Override
            public boolean hasNext() {
                return myCurrent != null;
            }

            @Override
            public Entry next() {
                final Entry entry = myCurrent;

                if (entry == null) {
                    throw new NoSuchElementException();
                }

                moveForward();
                return entry;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void moveForward() {
                if (myCurrent != null) {
                    myCurrent = myCurrent.myNext;
                }

                while (myCurrent == null && myIndex < myTableSize) {
                    myCurrent = myTable[myIndex++];
                }
            }

        }

        static class Entry {

            Entry myNext;

            long myAddress;

            long myNewPosition;

            long myOldPosition;

            Entry(final long aAddress, final long aNewPosition, final long aOldPosition, final Entry aChain) {
                myNext = aChain;
                myAddress = aAddress;
                myNewPosition = aNewPosition;
                myOldPosition = aOldPosition;
            }
        }
    }

}
