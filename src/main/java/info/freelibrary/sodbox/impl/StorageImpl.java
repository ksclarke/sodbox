
package info.freelibrary.sodbox.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Blob;
import info.freelibrary.sodbox.CompressedReadWriteFile;
import info.freelibrary.sodbox.Constants;
import info.freelibrary.sodbox.CustomAllocator;
import info.freelibrary.sodbox.CustomSerializer;
import info.freelibrary.sodbox.FieldIndex;
import info.freelibrary.sodbox.IFile;
import info.freelibrary.sodbox.IFileOutputStream;
import info.freelibrary.sodbox.ILoadable;
import info.freelibrary.sodbox.INamedClassLoader;
import info.freelibrary.sodbox.IPersistent;
import info.freelibrary.sodbox.IPersistentHash;
import info.freelibrary.sodbox.IPersistentList;
import info.freelibrary.sodbox.IPersistentMap;
import info.freelibrary.sodbox.IPersistentSet;
import info.freelibrary.sodbox.IResource;
import info.freelibrary.sodbox.IStoreable;
import info.freelibrary.sodbox.IValue;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.MemoryUsage;
import info.freelibrary.sodbox.MessageCodes;
import info.freelibrary.sodbox.MultidimensionalComparator;
import info.freelibrary.sodbox.MultidimensionalIndex;
import info.freelibrary.sodbox.PatriciaTrie;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentComparator;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.PersistentResource;
import info.freelibrary.sodbox.Relation;
import info.freelibrary.sodbox.SelfSerializable;
import info.freelibrary.sodbox.SodboxInputStream;
import info.freelibrary.sodbox.SortedCollection;
import info.freelibrary.sodbox.SpatialIndex;
import info.freelibrary.sodbox.SpatialIndexR2;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;
import info.freelibrary.sodbox.StorageListener;
import info.freelibrary.sodbox.TimeSeries;
import info.freelibrary.sodbox.XMLImportException;
import info.freelibrary.sodbox.fulltext.FullTextIndex;
import info.freelibrary.sodbox.fulltext.FullTextSearchHelper;
import info.freelibrary.util.Logger;
import info.freelibrary.util.LoggerFactory;

public class StorageImpl implements Storage {

    static final int DB_ALLOCATION_QUANTUM_BITS = 5;

    static final int DB_ALLOCATION_QUANTUM = 1 << DB_ALLOCATION_QUANTUM_BITS;

    static final int DB_BITMAP_ID = 1;

    static final int DB_DATABASE_OFFSET_BITS = 32; // up to 4 GB

    static final int DB_BITMAP_SEGMENT_BITS = Page.PAGE_SIZE_LOG + 3 + DB_ALLOCATION_QUANTUM_BITS;

    static final int DB_BITMAP_PAGES = 1 << DB_DATABASE_OFFSET_BITS - DB_BITMAP_SEGMENT_BITS;

    static final int DB_BITMAP_SEGMENT_SIZE = 1 << DB_BITMAP_SEGMENT_BITS;

    /**
     * Current version of database format. 0 means that database is not initialized. Used to provide backward
     * compatibility of Sodbox releases.
     */
    static final byte DB_DATABASE_FORMAT_VERSION = (byte) 3;

    static final int DB_DATABASE_OID_BITS = 31; // up to 2 billion objects

    /**
     * Database extension quantum. Memory is allocate by scanning bitmap. If there is no large enough hole, then
     * database is extended by the value of <code>dbDefaultExtensionQuantum</code>. This parameter should not be
     * smaller than <code>dbFirstUserId</code>.
     */
    static final long DB_DEFAULT_EXTENSION_QUANTUM = 1024 * 1024;

    /**
     * Initial database index size. Increasing it reduces the number of index reallocations but also increases the
     * initial database size. It should be set before opening connection.
     */
    static final int DB_DEFAULT_INIT_INDEX_SIZE = 1024;

    /**
     * Initial capacity of object hash.
     */
    static final int DB_DEFAULT_OBJECT_CACHE_INIT_SIZE = 1319;

    static final long DB_DEFAULT_PAGE_POOL_LRU_LIMIT = 1L << 60;

    static final int DB_HANDLES_PER_PAGE_BITS = Page.PAGE_SIZE_LOG - 3;

    static final int DB_DIRTY_PAGE_BITMAP_SIZE = 1 << DB_DATABASE_OID_BITS - DB_HANDLES_PER_PAGE_BITS - 3;

    static final int DB_FIRST_USER_ID = DB_BITMAP_ID + DB_BITMAP_PAGES;

    static final int DB_FLAGS_BITS = 3;

    static final int DB_FLAGS_MASK = 7;

    static final int DB_FREE_HANDLE_FLAG = 4;

    static final int DB_HANDLES_PER_PAGE = 1 << DB_HANDLES_PER_PAGE_BITS;

    static final int DB_INVALID_ID = 0;

    static final int DB_LARGE_DATABASE_OFFSET_BITS = 40; // up to 1 TB

    static final int DB_LARGE_BITMAP_PAGES = 1 << DB_LARGE_DATABASE_OFFSET_BITS - DB_BITMAP_SEGMENT_BITS;

    static final int DB_MAX_OBJECT_OID = (1 << DB_DATABASE_OID_BITS) - 1;

    static final int DB_MODIFIED_FLAG = 2;

    static final int DB_PAGE_OBJECT_FLAG = 1;

    static final int INC = Page.PAGE_SIZE / DB_ALLOCATION_QUANTUM / 8;

    static final int PAGE_BITS = Page.PAGE_SIZE * 8;

    private static Logger LOGGER = LoggerFactory.getLogger(StorageImpl.class, Constants.MESSAGES);

    private static final String UTIL_PACKAGE = "java.util.";

    protected int myObjectCacheInitSize = DB_DEFAULT_OBJECT_CACHE_INIT_SIZE;

    protected long myExtensionQuantum = DB_DEFAULT_EXTENSION_QUANTUM;

    protected int myInitIndexSize = DB_DEFAULT_INIT_INDEX_SIZE;

    protected boolean myAlternativeBtree = false;

    protected boolean myBackgroundGc = false;

    protected String myCacheKind = "default";

    protected int myCompatibilityMode = 0;

    protected boolean myForceStore = false;

    protected boolean myLockFile = false;

    protected boolean myMulticlientSupport = false;

    protected boolean myNoFlush = false;

    protected long myPagePoolLruLimit = DB_DEFAULT_PAGE_POOL_LRU_LIMIT;

    protected boolean myReadOnly = false;

    protected boolean myReloadObjectsOnRollback = false;

    protected boolean myReuseOid = true;

    protected boolean mySerializeSystemCollections = true;

    ClassDescriptor myClassDescriptor;

    long myAllocatedDelta;

    Object myBackgroundGcMonitor;

    Object myBackgroundGcStartMonitor;

    int myBitmapExtentBase;

    int[] myBitmapPageAvailableSpace;

    int[] myBlackBitmap; // bitmap of objects marked during GC

    HashMap myClassDescriptorMap;

    CloneNode myCloneList;

    int myCommittedIndexSize;

    boolean myConcurrentIterator = false;

    int myCurrentIndex; // copy of header.root, used to allow read access to the database during transaction commit

    int myCurrentIndexSize;

    int myCurrPBitmapOffs;// offset in current bitmap page for allocating
    // page objects

    int myCurrPBitmapPage;// current bitmap page for allocating page objects

    int myCurrRBitmapOffs;// offset in current bitmap page for allocating
    // unaligned records

    int myCurrRBitmapPage;// current bitmap page for allocating records

    int[] myDirtyPagesMap; // bitmap of changed pages in current index

    String myEncoding = null;

    IFile myFile;

    boolean myGcActive;

    boolean myGcDone;

    GcThread myGcThread;

    long myGcThreshold;

    int[] myGreyBitmap; // bitmap of visited during GC but not yet marked object

    Header myHeader; // base address of database file mapping

    boolean myInsideCloneBitmap;

    StorageListener myListener;

    ClassLoader myLoader;

    HashMap myLoaderMap;

    boolean myModified;

    int myNumOfBlockedTransactions;

    int myNumOfCommittedTransactions;

    int myNumOfNestedTransactions;

    OidHashTable myObjectCache;

    boolean myOpened;

    PagePool myPool;

    Properties myProperties = new Properties();

    HashMap myRecursiveLoadingPolicy;

    boolean myRecursiveLoadingPolicyDefined;

    boolean myReplicationAck = false;

    Location myReservedChain;

    long myScheduledCommitTime;

    CustomSerializer mySerializer;

    int mySlaveConnectionTimeout = 60; // seconds

    final ThreadLocal myTransactionContext = new ThreadLocal() {

        @Override
        protected synchronized Object initialValue() {
            return new ThreadTransactionContext();
        }
    };

    long myTransactionId;

    PersistentResource myTransactionLock;

    Object myTransactionMonitor;

    long myUsedSize; // total size of allocated objects since the beginning of the session

    boolean myUseSerializableTransactions;

    private ArrayList myCustomAllocatorList;

    private HashMap myCustomAllocatorMap;

    private CustomAllocator myDefaultAllocator;

    private ObjectMap myObjMap;

    static void checkIfFinal(final ClassDescriptor aDescriptor) {
        final Class cls = aDescriptor.myClass;

        if (cls != null) {
            for (ClassDescriptor next = aDescriptor.myNextCD; next != null; next = next.myNextCD) {
                next.load();

                if (cls.isAssignableFrom(next.myClass)) {
                    aDescriptor.hasSubclasses = true;
                } else if (next.myClass.isAssignableFrom(cls)) {
                    next.hasSubclasses = true;
                }
            }
        }
    }

    static final void memset(final Page aPage, final int aOffset, final int aPattern, final int aLength) {
        final byte[] array = aPage.myData;
        final byte pattern = (byte) aPattern;

        int length = aLength;
        int offset = aOffset;

        while (--length >= 0) {
            array[offset++] = pattern;
        }
    }

    @Override
    public void backup(final OutputStream aOutputStream) throws IOException {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        synchronized (this) {
            myObjectCache.flush();
        }

        final int current = 1 - myCurrentIndex;
        final int nObjects = myHeader.myRoot[current].myIndexUsed;
        final long indexOffs = myHeader.myRoot[current].myIndex;
        final int nUsedIndexPages = (nObjects + DB_HANDLES_PER_PAGE - 1) / DB_HANDLES_PER_PAGE;
        final int nIndexPages = (myHeader.myRoot[current].myIndexSize + DB_HANDLES_PER_PAGE - 1) /
                DB_HANDLES_PER_PAGE;
        final long[] index = new long[nObjects];
        final int[] oids = new int[nObjects];
        long totalRecordsSize = 0;
        long nPagedObjects = 0;
        int i;
        int j;
        int k;
        int bitmapExtent = myHeader.myRoot[current].myBitmapExtent;

        if (bitmapExtent == 0) {
            bitmapExtent = Integer.MAX_VALUE;
        }

        for (i = 0, j = 0; i < nUsedIndexPages; i++) {
            final Page pg = myPool.getPage(indexOffs + (long) i * Page.PAGE_SIZE);

            for (k = 0; k < DB_HANDLES_PER_PAGE && j < nObjects; k++, j++) {
                final long position = Bytes.unpack8(pg.myData, k * 8);

                index[j] = position;
                oids[j] = j;

                if ((position & DB_FREE_HANDLE_FLAG) == 0) {
                    if ((position & DB_PAGE_OBJECT_FLAG) != 0) {
                        nPagedObjects += 1;
                    } else if (position != 0) {
                        final int offs = (int) position & Page.PAGE_SIZE - 1;
                        final Page op = myPool.getPage(position - offs);
                        int size = ObjectHeader.getSize(op.myData, offs & ~DB_FLAGS_MASK);

                        size = size + DB_ALLOCATION_QUANTUM - 1 & ~(DB_ALLOCATION_QUANTUM - 1);
                        totalRecordsSize += size;
                        myPool.unfix(op);
                    }
                }
            }

            myPool.unfix(pg);
        }

        final Header newHeader = new Header();
        newHeader.myCurrentRoot = 0;
        newHeader.isDirty = false;
        newHeader.myDatabaseFormatVersion = myHeader.myDatabaseFormatVersion;

        long newFileSize = (nPagedObjects + nIndexPages * 2 + 1) * Page.PAGE_SIZE + totalRecordsSize;
        newFileSize = newFileSize + Page.PAGE_SIZE - 1 & ~(Page.PAGE_SIZE - 1);
        newHeader.myRoot = new RootPage[2];
        newHeader.myRoot[0] = new RootPage();
        newHeader.myRoot[1] = new RootPage();
        newHeader.myRoot[0].mySize = newHeader.myRoot[1].mySize = newFileSize;
        newHeader.myRoot[0].myIndex = newHeader.myRoot[1].myShadowIndex = Page.PAGE_SIZE;
        newHeader.myRoot[0].myShadowIndex = newHeader.myRoot[1].myIndex = Page.PAGE_SIZE + (long) nIndexPages *
                Page.PAGE_SIZE;
        newHeader.myRoot[0].myShadowIndexSize = newHeader.myRoot[0].myIndexSize =
                newHeader.myRoot[1].myShadowIndexSize = newHeader.myRoot[1].myIndexSize = nIndexPages *
                        DB_HANDLES_PER_PAGE;
        newHeader.myRoot[0].myIndexUsed = newHeader.myRoot[1].myIndexUsed = nObjects;
        newHeader.myRoot[0].myFreeList = newHeader.myRoot[1].myFreeList = myHeader.myRoot[current].myFreeList;
        newHeader.myRoot[0].myBitmapEnd = newHeader.myRoot[1].myBitmapEnd = myHeader.myRoot[current].myBitmapEnd;

        newHeader.myRoot[0].myRootObject = newHeader.myRoot[1].myRootObject = myHeader.myRoot[current].myRootObject;
        newHeader.myRoot[0].myClassDescList = newHeader.myRoot[1].myClassDescList =
                myHeader.myRoot[current].myClassDescList;
        newHeader.myRoot[0].myBitmapExtent = newHeader.myRoot[1].myBitmapExtent =
                myHeader.myRoot[current].myBitmapExtent;

        final byte[] page = new byte[Page.PAGE_SIZE];
        newHeader.pack(page);
        aOutputStream.write(page);

        long pageOffs = (long) (nIndexPages * 2 + 1) * Page.PAGE_SIZE;
        long recOffs = (nPagedObjects + nIndexPages * 2 + 1) * Page.PAGE_SIZE;

        GenericSort.sort(new GenericSortArray() {

            @Override
            public int compare(final int aIndex, final int aJndex) {
                return index[aIndex] < index[aJndex] ? -1 : index[aIndex] == index[aJndex] ? 0 : 1;
            }

            @Override
            public int size() {
                return nObjects;
            }

            @Override
            public void swap(final int aIndex, final int aJndex) {
                final long position = index[aIndex];
                final int oid = oids[aIndex];

                index[aIndex] = index[aJndex];
                index[aJndex] = position;

                oids[aIndex] = oids[aJndex];
                oids[aJndex] = oid;
            }
        });

        final byte[] newIndex = new byte[nIndexPages * DB_HANDLES_PER_PAGE * 8];

        for (i = 0; i < nObjects; i++) {
            final long pos = index[i];
            final int oid = oids[i];

            if ((pos & DB_FREE_HANDLE_FLAG) == 0) {
                if ((pos & DB_PAGE_OBJECT_FLAG) != 0) {
                    Bytes.pack8(newIndex, oid * 8, pageOffs | DB_PAGE_OBJECT_FLAG);
                    pageOffs += Page.PAGE_SIZE;
                } else if (pos != 0) {
                    Bytes.pack8(newIndex, oid * 8, recOffs);
                    final int offs = (int) pos & Page.PAGE_SIZE - 1;
                    final Page op = myPool.getPage(pos - offs);
                    int size = ObjectHeader.getSize(op.myData, offs & ~DB_FLAGS_MASK);

                    size = size + DB_ALLOCATION_QUANTUM - 1 & ~(DB_ALLOCATION_QUANTUM - 1);
                    recOffs += size;
                    myPool.unfix(op);
                }
            } else {
                Bytes.pack8(newIndex, oid * 8, pos);
            }
        }

        aOutputStream.write(newIndex);
        aOutputStream.write(newIndex);

        for (i = 0; i < nObjects; i++) {
            final long pos = index[i];

            if (((int) pos & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) == DB_PAGE_OBJECT_FLAG) {
                if (oids[i] < DB_BITMAP_ID + DB_BITMAP_PAGES || oids[i] >= bitmapExtent && oids[i] < bitmapExtent +
                        DB_LARGE_BITMAP_PAGES - DB_BITMAP_PAGES) {
                    final int pageId = oids[i] < DB_BITMAP_ID + DB_BITMAP_PAGES ? oids[i] - DB_BITMAP_ID : oids[i] -
                            bitmapExtent + myBitmapExtentBase;
                    final long mappedSpace = (long) pageId * Page.PAGE_SIZE * 8 * DB_ALLOCATION_QUANTUM;

                    if (mappedSpace >= newFileSize) {
                        Arrays.fill(page, (byte) 0);
                    } else if (mappedSpace + Page.PAGE_SIZE * 8 * DB_ALLOCATION_QUANTUM <= newFileSize) {
                        Arrays.fill(page, (byte) -1);
                    } else {
                        final int nBits = (int) (newFileSize - mappedSpace >> DB_ALLOCATION_QUANTUM_BITS);
                        Arrays.fill(page, 0, nBits >> 3, (byte) -1);
                        page[nBits >> 3] = (byte) ((1 << (nBits & 7)) - 1);
                        Arrays.fill(page, (nBits >> 3) + 1, Page.PAGE_SIZE, (byte) 0);
                    }

                    aOutputStream.write(page);
                } else {
                    final Page pg = myPool.getPage(pos & ~DB_FLAGS_MASK);
                    aOutputStream.write(pg.myData);
                    myPool.unfix(pg);
                }
            }
        }

        for (i = 0; i < nObjects; i++) {
            long pos = index[i];

            if (pos != 0 && ((int) pos & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) == 0) {
                pos &= ~DB_FLAGS_MASK;
                int offs = (int) pos & Page.PAGE_SIZE - 1;
                Page pg = myPool.getPage(pos - offs);
                int size = ObjectHeader.getSize(pg.myData, offs);
                size = size + DB_ALLOCATION_QUANTUM - 1 & ~(DB_ALLOCATION_QUANTUM - 1);

                while (true) {
                    if (Page.PAGE_SIZE - offs >= size) {
                        aOutputStream.write(pg.myData, offs, size);
                        break;
                    }

                    aOutputStream.write(pg.myData, offs, Page.PAGE_SIZE - offs);
                    size -= Page.PAGE_SIZE - offs;
                    pos += Page.PAGE_SIZE - offs;
                    offs = 0;
                    myPool.unfix(pg);
                    pg = myPool.getPage(pos);
                }

                myPool.unfix(pg);
            }
        }

        if (recOffs != newFileSize) {
            Assert.that(newFileSize - recOffs < Page.PAGE_SIZE);
            final int align = (int) (newFileSize - recOffs);
            Arrays.fill(page, 0, align, (byte) 0);
            aOutputStream.write(page, 0, align);
        }
    }

    @Override
    public void backup(final String aFilePath, final String aCryptKey) throws IOException {
        backup(new IFileOutputStream(aCryptKey != null ? (IFile) new Rc4File(aFilePath, false, false, aCryptKey)
                : (IFile) new OSFile(aFilePath, false, false)));
    }

    @Override
    public void beginSerializableTransaction() {
        if (myMulticlientSupport) {
            beginThreadTransaction(EXCLUSIVE_TRANSACTION);
        } else {
            beginThreadTransaction(SERIALIZABLE_TRANSACTION);
        }
    }

    @Override
    public void beginExclusiveTransaction() {
        beginThreadTransaction(EXCLUSIVE_TRANSACTION);
    }

    @Override
    public void beginCooperativeTransaction() {
        beginThreadTransaction(COOPERATIVE_TRANSACTION);
    }

    @Override
    public void beginReplicationSlaveTransaction() {
        throw new IllegalArgumentException(LOGGER.getMessage(MessageCodes.SB_044));
    }

    @Override
    public void endReplicationSlaveTransaction() {
        throw new IllegalArgumentException(LOGGER.getMessage(MessageCodes.SB_044));
    }

    @Override
    public void clearObjectCache() {
        myObjectCache.clear();
    }

    @Override
    public void close() throws StorageError {
        if (myBackgroundGcMonitor == null) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        synchronized (myBackgroundGcMonitor) {
            commit();
            myOpened = false;
        }

        if (myGcThread != null) {
            myGcThread.activate();

            try {
                myGcThread.join();
            } catch (final InterruptedException details) {
                LOGGER.warn(details.getMessage(), details);
            }
        }

        if (isDirty()) {
            final Page page = myPool.putPage(0);

            myHeader.pack(page.myData);
            myPool.flush();
            myPool.modify(page);
            myHeader.isDirty = false;
            myHeader.pack(page.myData);
            myPool.unfix(page);
            myPool.flush();
        }

        myPool.close();

        // make GC easier
        myPool = null;
        myObjectCache = null;
        myClassDescriptorMap = null;
        myBitmapPageAvailableSpace = null;
        myDirtyPagesMap = null;
        myClassDescriptor = null;
    }

    @Override
    public void commit() {
        if (myUseSerializableTransactions && getTransactionContext().myNested != 0) {
            // Store should not be used in serializable transaction mode
            throw new StorageError(StorageError.INVALID_OPERATION, "commit");
        }

        synchronized (myBackgroundGcMonitor) {
            synchronized (this) {
                if (!myOpened) {
                    throw new StorageError(StorageError.STORAGE_NOT_OPENED);
                }

                if (!myModified) {
                    return;
                }

                myObjectCache.flush();

                if (myCustomAllocatorList != null) {
                    final Iterator iterator = myCustomAllocatorList.iterator();

                    while (iterator.hasNext()) {
                        final CustomAllocator alloc = (CustomAllocator) iterator.next();

                        if (alloc.isModified()) {
                            alloc.store();
                        }

                        alloc.commit();
                    }
                }

                commit0();
                myModified = false;
            }
        }
    }

    @Override
    public synchronized <T> IPersistentSet<T> createBag() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final IPersistentSet<T> set = myAlternativeBtree ? (IPersistentSet<T>) new AltPersistentSet<T>(false)
                : (IPersistentSet<T>) new PersistentSet<T>(false);
        set.assignOid(this, 0, false);
        return set;
    }

    @Override
    public CustomAllocator createBitmapAllocator(final int aQuantum, final long aBase, final long aExtension,
            final long aLimit) {
        return new BitmapCustomAllocator(this, aQuantum, aBase, aExtension, aLimit);
    }

    @Override
    public Blob createBlob() {
        return new BlobImpl(this, Page.PAGE_SIZE - BlobImpl.HEADER_SIZE);
    }

    @Override
    public <T> FieldIndex<T> createFieldIndex(final Class aType, final String aFieldName,
            final boolean aUniqueKeyIndex) {
        return this.<T>createFieldIndex(aType, aFieldName, aUniqueKeyIndex, false);
    }

    @Override
    public <T> FieldIndex<T> createFieldIndex(final Class aType, final String aFieldName,
            final boolean aUniqueKeyIndex, final boolean aCaseInsensitiveFlag) {
        return this.<T>createFieldIndex(aType, aFieldName, aUniqueKeyIndex, aCaseInsensitiveFlag, false);
    }

    @Override
    public synchronized <T> FieldIndex<T> createFieldIndex(final Class aType, final String aFieldName,
            final boolean aUniqueKeyIndex, final boolean aCaseInsensitiveFlag, final boolean aThickIndex) {
        final FieldIndex<T> index;

        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        index = aThickIndex ? aCaseInsensitiveFlag ? (FieldIndex<T>) new ThickCaseInsensitiveFieldIndex(this, aType,
                aFieldName) : (FieldIndex<T>) new ThickFieldIndex(this, aType, aFieldName) : aCaseInsensitiveFlag
                        ? myAlternativeBtree ? (FieldIndex<T>) new AltBtreeCaseInsensitiveFieldIndex<T>(aType,
                                aFieldName, aUniqueKeyIndex) : (FieldIndex<T>) new BtreeCaseInsensitiveFieldIndex<T>(
                                        aType, aFieldName, aUniqueKeyIndex) : myAlternativeBtree
                                                ? (FieldIndex<T>) new AltBtreeFieldIndex<T>(aType, aFieldName,
                                                        aUniqueKeyIndex) : (FieldIndex<T>) new BtreeFieldIndex<T>(
                                                                aType, aFieldName, aUniqueKeyIndex);
        index.assignOid(this, 0, false);

        return index;
    }

    @Override
    public <T> FieldIndex<T> createFieldIndex(final Class aType, final String[] aFieldNames,
            final boolean aUniqueKeyIndex) {
        return this.<T>createFieldIndex(aType, aFieldNames, aUniqueKeyIndex, false);
    }

    @Override
    public synchronized <T> FieldIndex<T> createFieldIndex(final Class aType, final String[] aFieldNames,
            final boolean aUniqueKeyIndex, final boolean aCaseInsensitiveFlag) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final FieldIndex<T> index = aCaseInsensitiveFlag ? myAlternativeBtree
                ? (FieldIndex<T>) new AltBtreeCaseInsensitiveMultiFieldIndex(aType, aFieldNames, aUniqueKeyIndex)
                : (FieldIndex<T>) new BtreeCaseInsensitiveMultiFieldIndex(aType, aFieldNames, aUniqueKeyIndex)
                : myAlternativeBtree ? (FieldIndex<T>) new AltBtreeMultiFieldIndex(aType, aFieldNames,
                        aUniqueKeyIndex) : (FieldIndex<T>) new BtreeMultiFieldIndex(aType, aFieldNames,
                                aUniqueKeyIndex);
        index.assignOid(this, 0, false);
        return index;
    }

    @Override
    public FullTextIndex createFullTextIndex() {
        return createFullTextIndex(new FullTextSearchHelper(this));
    }

    @Override
    public FullTextIndex createFullTextIndex(final FullTextSearchHelper aHelper) {
        return new FullTextIndexImpl(this, aHelper);
    }

    @Override
    public <K, V> IPersistentHash<K, V> createHash() {
        return createHash(101, 2);
    }

    @Override
    public <K, V> IPersistentHash<K, V> createHash(final int aPageSize, final int aLoadFactor) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new PersistentHashImpl<>(this, aPageSize, aLoadFactor);
    }

    @Override
    public synchronized <T> Index<T> createIndex(final Class aKeyType, final boolean aUniqueKeyIndex) {
        final Index<T> index;

        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        index = myAlternativeBtree ? (Index<T>) new AltBtree<T>(aKeyType, aUniqueKeyIndex) : (Index<T>) new Btree<T>(
                aKeyType, aUniqueKeyIndex);
        index.assignOid(this, 0, false);

        return index;
    }

    @Override
    public synchronized <T> Index<T> createIndex(final Class[] aKeyTypes, final boolean aUniqueKeyIndex) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final Index<T> index = myAlternativeBtree ? (Index<T>) new AltBtreeCompoundIndex<T>(aKeyTypes,
                aUniqueKeyIndex) : (Index<T>) new BtreeCompoundIndex<T>(aKeyTypes, aUniqueKeyIndex);
        index.assignOid(this, 0, false);
        return index;
    }

    @Override
    public <T> Link<T> createLink() {
        return createLink(8);
    }

    @Override
    public <T> Link<T> createLink(final int aInitialSize) {
        return new LinkImpl<>(this, aInitialSize);
    }

    @Override
    public <T> IPersistentList<T> createList() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new PersistentListImpl<>(this);
    }

    @Override
    public <K extends Comparable, V> IPersistentMap<K, V> createMap(final Class aKeyType) {
        return createMap(aKeyType, 4);
    }

    @Override
    public <K extends Comparable, V> IPersistentMap<K, V> createMap(final Class aKeyType, final int aInitialSize) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new PersistentMapImpl<>(this, aKeyType, aInitialSize);
    }

    @Override
    public synchronized <T> MultidimensionalIndex<T> createMultidimensionalIndex(final Class aType,
            final String[] aFieldNames, final boolean aTreatZeroAsUndefinedValueFlag) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new KDTree<>(this, aType, aFieldNames, aTreatZeroAsUndefinedValueFlag);
    }

    @Override
    public synchronized <T> MultidimensionalIndex<T> createMultidimensionalIndex(
            final MultidimensionalComparator<T> aComparator) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new KDTree<>(this, aComparator);
    }

    @Override
    public <T> PatriciaTrie<T> createPatriciaTrie() {
        return new PTrie<>();
    }

    @Override
    public Blob createRandomAccessBlob() {
        return new RandomAccessBlobImpl(this);
    }

    @Override
    public <T> FieldIndex<T> createRandomAccessFieldIndex(final Class aType, final String aFieldName,
            final boolean aUniqueKeyIndex) {
        return this.<T>createRandomAccessFieldIndex(aType, aFieldName, aUniqueKeyIndex, false);
    }

    @Override
    public synchronized <T> FieldIndex<T> createRandomAccessFieldIndex(final Class aType, final String aFieldName,
            final boolean aUniqueKeyIndex, final boolean aCaseInsensitiveFlag) {
        final FieldIndex<T> index;

        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        index = aCaseInsensitiveFlag ? (FieldIndex) new RndBtreeCaseInsensitiveFieldIndex<T>(aType, aFieldName,
                aUniqueKeyIndex) : (FieldIndex) new RndBtreeFieldIndex<T>(aType, aFieldName, aUniqueKeyIndex);
        index.assignOid(this, 0, false);

        return index;
    }

    @Override
    public <T> FieldIndex<T> createRandomAccessFieldIndex(final Class aType, final String[] aFieldNames,
            final boolean aUniqueKeyIndex) {
        return this.<T>createRandomAccessFieldIndex(aType, aFieldNames, aUniqueKeyIndex, false);
    }

    @Override
    public synchronized <T> FieldIndex<T> createRandomAccessFieldIndex(final Class aType, final String[] aFieldNames,
            final boolean aUniqueKeyIndex, final boolean aCaseInsensitiveFlag) {
        final FieldIndex<T> index;

        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        index = aCaseInsensitiveFlag ? (FieldIndex) new RndBtreeCaseInsensitiveMultiFieldIndex(aType, aFieldNames,
                aUniqueKeyIndex) : (FieldIndex) new RndBtreeMultiFieldIndex(aType, aFieldNames, aUniqueKeyIndex);
        index.assignOid(this, 0, false);

        return index;
    }

    @Override
    public synchronized <T> Index<T> createRandomAccessIndex(final Class aKeyType, final boolean aUniqueKeyIndex) {
        final Index<T> index;

        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        index = new RndBtree<>(aKeyType, aUniqueKeyIndex);
        index.assignOid(this, 0, false);

        return index;
    }

    @Override
    public synchronized <T> Index<T> createRandomAccessIndex(final Class[] aKeyTypes, final boolean aUniqueKeyIndex) {
        final Index<T> index;

        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        index = new RndBtreeCompoundIndex<>(aKeyTypes, aUniqueKeyIndex);
        index.assignOid(this, 0, false);

        return index;
    }

    @Override
    public <M, O> Relation<M, O> createRelation(final O aOwner) {
        return new RelationImpl<>(this, aOwner);
    }

    @Override
    public <T> IPersistentList<T> createScalableList() {
        return createScalableList(8);
    }

    @Override
    public <T> IPersistentList<T> createScalableList(final int aInitialSize) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new ScalableList<>(this, aInitialSize);
    }

    @Override
    public synchronized <T> IPersistentSet<T> createScalableSet() {
        return createScalableSet(8);
    }

    @Override
    public synchronized <T> IPersistentSet<T> createScalableSet(final int aInitialSize) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new ScalableSet(this, aInitialSize);
    }

    @Override
    public synchronized <T> IPersistentSet<T> createSet() {
        final IPersistentSet<T> set;

        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        set = myAlternativeBtree ? (IPersistentSet<T>) new AltPersistentSet<T>(true)
                : (IPersistentSet<T>) new PersistentSet<T>(true);
        set.assignOid(this, 0, false);

        return set;
    }

    /**
     * Create a sorted collection.
     *
     * @param aUniqueKeyCollection A unique key collection
     * @return A sorted collection
     */
    public <T extends Comparable> SortedCollection<T> createSortedCollection(final boolean aUniqueKeyCollection) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new Ttree<>(this, new DefaultPersistentComparator<T>(), aUniqueKeyCollection);
    }

    @Override
    public <T> SortedCollection<T> createSortedCollection(final PersistentComparator<T> aComparator,
            final boolean aUniqueKeyCollection) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new Ttree<>(this, aComparator, aUniqueKeyCollection);
    }

    @Override
    public synchronized <T> SpatialIndex<T> createSpatialIndex() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new Rtree<>();
    }

    @Override
    public synchronized <T> SpatialIndexR2<T> createSpatialIndexR2() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new RtreeR2<>(this);
    }

    @Override
    public synchronized <T> Index<T> createThickIndex(final Class aKeyType) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new ThickIndex<>(this, aKeyType);
    }

    @Override
    public <T extends TimeSeries.Tick> TimeSeries<T> createTimeSeries(final Class aBlockClass,
            final long aMaxBlockTimeInterval) {
        return new TimeSeriesImpl<>(this, aBlockClass, aMaxBlockTimeInterval);
    }

    @Override
    public void deallocate(final Object aObj) {
        deallocateObject(aObj);
    }

    @Override
    public synchronized void deallocateObject(final Object aObj) {
        synchronized (myObjectCache) {
            if (getOid(aObj) == 0) {
                return;
            }

            if (myUseSerializableTransactions) {
                final ThreadTransactionContext aThreadTransactionContex = getTransactionContext();

                // serializable transaction
                if (aThreadTransactionContex.myNested != 0) {
                    aThreadTransactionContex.myDeleted.add(aObj);
                    return;
                }
            }

            deallocateObject0(aObj);
        }
    }

    @Override
    public void endCooperativeTransaction() {
        endThreadTransaction(Integer.MAX_VALUE);
    }

    @Override
    public void endCooperativeTransaction(final int aMaxDelay) {
        endThreadTransaction(aMaxDelay);
    }

    @Override
    public void endExclusiveTransaction() {
        endThreadTransaction(Integer.MAX_VALUE);
    }

    @Override
    public void endSerializableTransaction() {
        if (!isInsideThreadTransaction()) {
            throw new StorageError(StorageError.NOT_IN_TRANSACTION);
        }

        endThreadTransaction(Integer.MAX_VALUE);
    }

    private void endThreadTransaction(final int aMaxDelay) {
        if (myMulticlientSupport) {
            LOGGER.debug(MessageCodes.SB_034);

            if (aMaxDelay != Integer.MAX_VALUE) {
                throw new IllegalArgumentException(LOGGER.getMessage(MessageCodes.SB_029));
            }

            synchronized (myTransactionMonitor) {
                myTransactionLock.unlock();

                if (myNumOfNestedTransactions != 0) {
                    if (myNumOfNestedTransactions == 1) {
                        commit();

                        myPool.flush();
                        myFile.unlock();
                    }

                    myNumOfNestedTransactions -= 1;
                }
            }

            return;
        }

        final ThreadTransactionContext threadTransactionContext = getTransactionContext();

        if (threadTransactionContext.myNested != 0) {
            LOGGER.debug(MessageCodes.SB_033); // serializable transaction

            if (--threadTransactionContext.myNested == 0) {
                final ArrayList modified = threadTransactionContext.myModified;
                final ArrayList deleted = threadTransactionContext.myDeleted;
                final Map locked = threadTransactionContext.myLocked;

                synchronized (myBackgroundGcMonitor) {
                    synchronized (this) {
                        synchronized (myObjectCache) {
                            for (int index = modified.size(); --index >= 0;) {
                                store(modified.get(index));
                            }

                            for (int index = deleted.size(); --index >= 0;) {
                                deallocateObject0(deleted.get(index));
                            }

                            if (modified.size() + deleted.size() > 0) {
                                commit0();
                            }
                        }
                    }
                }

                final Iterator iterator = locked.values().iterator();

                while (iterator.hasNext()) {
                    ((IResource) iterator.next()).reset();
                }

                modified.clear();
                deleted.clear();
                locked.clear();
            }
        } else {
            LOGGER.debug(MessageCodes.SB_035); // exclusive or cooperative transaction

            synchronized (myTransactionMonitor) {
                myTransactionLock.unlock();

                if (myNumOfNestedTransactions != 0) {
                    if (--myNumOfNestedTransactions == 0) {
                        myNumOfCommittedTransactions += 1;
                        commit();
                        myScheduledCommitTime = Long.MAX_VALUE;

                        if (myNumOfBlockedTransactions != 0) {
                            myTransactionMonitor.notifyAll();
                        }
                    } else {
                        if (aMaxDelay != Integer.MAX_VALUE) {
                            final long nextCommit = System.currentTimeMillis() + aMaxDelay;

                            if (nextCommit < myScheduledCommitTime) {
                                myScheduledCommitTime = nextCommit;
                            }

                            if (aMaxDelay == 0) {
                                final int count = myNumOfCommittedTransactions;

                                myNumOfBlockedTransactions += 1;

                                do {
                                    try {
                                        myTransactionMonitor.wait();
                                    } catch (final InterruptedException details) {
                                        LOGGER.warn(details.getMessage(), details);
                                    }
                                } while (myNumOfCommittedTransactions == count);

                                myNumOfBlockedTransactions -= 1;
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public synchronized void exportXML(final Writer aWriter) throws IOException {
        final int rootOid;

        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        myObjectCache.flush();
        rootOid = myHeader.myRoot[1 - myCurrentIndex].myRootObject;

        if (rootOid != 0) {
            new XMLExporter(this, aWriter).exportDatabase(rootOid);
        }
    }

    @Override
    public ClassLoader findClassLoader(final String aClassLoaderName) {
        if (myLoaderMap == null) {
            return null;
        }

        return (ClassLoader) myLoaderMap.get(aClassLoaderName);
    }

    @Override
    public synchronized int gc() {
        return gc0();
    }

    @Override
    public ClassLoader getClassLoader() {
        return myLoader;
    }

    @Override
    public int getDatabaseFormatVersion() {
        return myHeader.myDatabaseFormatVersion;
    }

    @Override
    public long getDatabaseSize() {
        return myHeader.myRoot[1 - myCurrentIndex].mySize;
    }

    @Override
    public StorageListener getListener() {
        return myListener;
    }

    @Override
    public synchronized HashMap getMemoryDump() {
        synchronized (myObjectCache) {
            if (!myOpened) {
                throw new StorageError(StorageError.STORAGE_NOT_OPENED);
            }

            final int size = (int) (myHeader.myRoot[myCurrentIndex].mySize >>> DB_ALLOCATION_QUANTUM_BITS + 5) + 1;

            boolean existsNotMarkedObjects;
            long position;
            int index;
            int jndex;

            // mark
            myGreyBitmap = new int[size];
            myBlackBitmap = new int[size];

            final int rootOid = myHeader.myRoot[myCurrentIndex].myRootObject;
            final HashMap map = new HashMap();

            if (rootOid != 0) {
                final MemoryUsage indexUsage = new MemoryUsage(Index.class);
                final MemoryUsage fieldIndexUsage = new MemoryUsage(FieldIndex.class);
                final MemoryUsage classUsage = new MemoryUsage(Class.class);

                markOid(rootOid);

                do {
                    existsNotMarkedObjects = false;

                    for (index = 0; index < size; index++) {
                        if (myGreyBitmap[index] != 0) {
                            existsNotMarkedObjects = true;

                            for (jndex = 0; jndex < 32; jndex++) {
                                if ((myGreyBitmap[index] & 1 << jndex) != 0) {
                                    position = ((long) index << 5) + jndex << DB_ALLOCATION_QUANTUM_BITS;

                                    myGreyBitmap[index] &= ~(1 << jndex);
                                    myBlackBitmap[index] |= 1 << jndex;

                                    final int offset = (int) position & Page.PAGE_SIZE - 1;
                                    final Page page = myPool.getPage(position - offset);
                                    final int typeOid = ObjectHeader.getType(page.myData, offset);
                                    final int objSize = ObjectHeader.getSize(page.myData, offset);
                                    final int alignedSize = objSize + DB_ALLOCATION_QUANTUM - 1 &
                                            ~(DB_ALLOCATION_QUANTUM - 1);

                                    if (typeOid != 0) {
                                        markOid(typeOid);

                                        final ClassDescriptor descriptor = findClassDescriptor(typeOid);

                                        if (Btree.class.isAssignableFrom(descriptor.myClass)) {
                                            final Btree btree = new Btree(page.myData, ObjectHeader.SIZE_OF + offset);
                                            final int pageCount;

                                            btree.assignOid(this, 0, false);
                                            pageCount = btree.markTree();

                                            if (FieldIndex.class.isAssignableFrom(descriptor.myClass)) {
                                                fieldIndexUsage.myNInstances += 1;
                                                fieldIndexUsage.myTotalSize += (long) pageCount * Page.PAGE_SIZE +
                                                        objSize;
                                                fieldIndexUsage.myAllocatedSize += (long) pageCount * Page.PAGE_SIZE +
                                                        alignedSize;
                                            } else {
                                                indexUsage.myNInstances += 1;
                                                indexUsage.myTotalSize += (long) pageCount * Page.PAGE_SIZE + objSize;
                                                indexUsage.myAllocatedSize += (long) pageCount * Page.PAGE_SIZE +
                                                        alignedSize;
                                            }
                                        } else {
                                            MemoryUsage usage = (MemoryUsage) map.get(descriptor.myClass);

                                            if (usage == null) {
                                                usage = new MemoryUsage(descriptor.myClass);
                                                map.put(descriptor.myClass, usage);
                                            }

                                            usage.myNInstances += 1;
                                            usage.myTotalSize += objSize;
                                            usage.myAllocatedSize += alignedSize;

                                            if (descriptor.hasReferences) {
                                                markObject(myPool.get(position), ObjectHeader.SIZE_OF, descriptor);
                                            }
                                        }
                                    } else {
                                        classUsage.myNInstances += 1;
                                        classUsage.myTotalSize += objSize;
                                        classUsage.myAllocatedSize += alignedSize;
                                    }

                                    myPool.unfix(page);
                                }
                            }
                        }
                    }
                } while (existsNotMarkedObjects);

                if (indexUsage.myNInstances != 0) {
                    map.put(Index.class, indexUsage);
                }

                if (fieldIndexUsage.myNInstances != 0) {
                    map.put(FieldIndex.class, fieldIndexUsage);
                }

                if (classUsage.myNInstances != 0) {
                    map.put(Class.class, classUsage);
                }

                final MemoryUsage system = new MemoryUsage(Storage.class);

                system.myTotalSize += myHeader.myRoot[0].myIndexSize * 8L;
                system.myTotalSize += myHeader.myRoot[1].myIndexSize * 8L;
                system.myTotalSize += (long) (myHeader.myRoot[myCurrentIndex].myBitmapEnd - DB_BITMAP_ID) *
                        Page.PAGE_SIZE;
                system.myTotalSize += Page.PAGE_SIZE; // root page

                if (myHeader.myRoot[myCurrentIndex].myBitmapExtent != 0) {
                    system.myAllocatedSize = getBitmapUsedSpace(DB_BITMAP_ID, DB_BITMAP_ID + DB_BITMAP_PAGES) +
                            getBitmapUsedSpace(myHeader.myRoot[myCurrentIndex].myBitmapExtent + DB_BITMAP_PAGES -
                                    myBitmapExtentBase, myHeader.myRoot[myCurrentIndex].myBitmapExtent +
                                            myHeader.myRoot[myCurrentIndex].myBitmapEnd - DB_BITMAP_ID -
                                            myBitmapExtentBase);
                } else {
                    system.myAllocatedSize = getBitmapUsedSpace(DB_BITMAP_ID,
                            myHeader.myRoot[myCurrentIndex].myBitmapEnd);
                }

                system.myNInstances = myHeader.myRoot[myCurrentIndex].myIndexSize;
                map.put(Storage.class, system);
            }

            return map;
        }
    }

    @Override
    public synchronized Object getObjectByOID(final int aOID) {
        return aOID == 0 ? null : lookupObject(aOID, null);
    }

    @Override
    public int getOid(final Object aObj) {
        return aObj instanceof IPersistent ? ((IPersistent) aObj).getOid() : aObj == null ? 0 : myObjMap.getOid(aObj);
    }

    @Override
    public Properties getProperties() {
        return myProperties;
    }

    @Override
    public Object getProperty(final String aName) {
        return myProperties.get(aName);
    }

    @Override
    public synchronized Object getRoot() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final int rootOid = myHeader.myRoot[1 - myCurrentIndex].myRootObject;
        return rootOid == 0 ? null : lookupObject(rootOid, null);
    }

    /**
     * This method is used internally by Sodbox to get transaction context associated with current thread. But it can
     * be also used by application to get transaction context, store it in some variable and use in another thread. I
     * will make it possible to share one transaction between multiple threads.
     *
     * @return transaction context associated with current thread
     */
    @Override
    public ThreadTransactionContext getTransactionContext() {
        return (ThreadTransactionContext) myTransactionContext.get();
    }

    @Override
    public long getUsedSize() {
        return myUsedSize;
    }

    @Override
    public synchronized void importXML(final Reader aReader) throws XMLImportException {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        new XMLImporter(this, aReader).importDatabase();
    }

    /**
     * Invalidate the supplied object.
     *
     * @param aObj An object to invalidate
     */
    public void invalidate(final Object aObj) {
        if (aObj instanceof IPersistent) {
            ((IPersistent) aObj).invalidate();
        } else {
            synchronized (myObjMap) {
                final ObjectMap.Entry entry = myObjMap.put(aObj);

                entry.myState &= ~Persistent.DIRTY;
                entry.myState |= Persistent.RAW;
            }
        }
    }

    @Override
    public boolean isInsideThreadTransaction() {
        return getTransactionContext().myNested != 0 || myNumOfNestedTransactions != 0;
    }

    @Override
    public boolean isOpened() {
        return myOpened;
    }

    @Override
    public Iterator join(final Iterator[] aIterator) {
        final HashSet result = new HashSet();

        for (int index = 0; index < aIterator.length; index++) {
            final PersistentIterator iterator = (PersistentIterator) aIterator[index];

            int oid;

            while ((oid = iterator.nextOID()) != 0) {
                result.add(Integer.valueOf(oid));
            }
        }

        return new HashIterator(result);
    }

    @Override
    public void load(final Object aObj) {
        if (aObj instanceof IPersistent) {
            ((IPersistent) aObj).load();
        } else {
            synchronized (myObjMap) {
                final ObjectMap.Entry entry = myObjMap.get(aObj);

                if (entry == null || (entry.myState & Persistent.RAW) == 0 || entry.myOID == 0) {
                    return;
                }
            }

            loadObject(aObj);
        }
    }

    @Override
    public synchronized void loadObject(final Object aObject) {
        if (isRaw(aObject)) {
            loadStub(getOid(aObject), aObject, aObject.getClass());
        }
    }

    @Override
    public boolean lockObject(final Object aObj) {
        if (myUseSerializableTransactions) {
            final ThreadTransactionContext threadTransactionContex = getTransactionContext();

            // serializable transaction
            if (threadTransactionContex.myNested != 0) {
                return threadTransactionContex.myLocked.put(aObj, aObj) == null;
            }
        }

        return true;
    }

    @Override
    public synchronized int makePersistent(final Object aObj) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        if (aObj == null) {
            return 0;
        }

        int oid = getOid(aObj);

        if (oid != 0) {
            return oid;
        }

        if (myForceStore && (!myUseSerializableTransactions || getTransactionContext().myNested == 0)) {
            synchronized (myObjectCache) {
                storeObject0(aObj, false);
            }

            return getOid(aObj);
        } else {
            synchronized (myObjectCache) {
                oid = allocateId();
                assignOid(aObj, oid, false);
                setPosition(oid, 0);
                myObjectCache.put(oid, aObj);

                modify(aObj);

                return oid;
            }
        }
    }

    @Override
    public Iterator merge(final Iterator[] aIterator) {
        HashSet result = null;

        for (int index = 0; index < aIterator.length; index++) {
            final PersistentIterator iterator = (PersistentIterator) aIterator[index];
            final HashSet newResult = new HashSet();

            int oid;

            while ((oid = iterator.nextOID()) != 0) {
                final Integer oidWrapper = Integer.valueOf(oid);

                if (result == null || result.contains(oidWrapper)) {
                    newResult.add(oidWrapper);
                }
            }

            result = newResult;

            if (result.size() == 0) {
                break;
            }
        }

        if (result == null) {
            result = new HashSet();
        }

        return new HashIterator(result);
    }

    @Override
    public void modify(final Object aObj) {
        if (aObj instanceof IPersistent) {
            ((IPersistent) aObj).modify();
        } else {
            if (myUseSerializableTransactions) {
                final ThreadTransactionContext threadTransactionContex = getTransactionContext();

                // serializable transaction
                if (threadTransactionContex.myNested != 0) {
                    threadTransactionContex.myModified.add(aObj);
                    return;
                }
            }

            synchronized (this) {
                synchronized (myObjectCache) {
                    synchronized (myObjMap) {
                        final ObjectMap.Entry entry = myObjMap.put(aObj);

                        if ((entry.myState & Persistent.DIRTY) == 0 && entry.myOID != 0) {
                            if ((entry.myState & Persistent.RAW) != 0) {
                                throw new StorageError(StorageError.ACCESS_TO_STUB);
                            }

                            Assert.that((entry.myState & Persistent.DELETED) == 0);

                            storeObject(aObj);

                            entry.myState &= ~Persistent.DIRTY;
                        }
                    }
                }
            }
        }
    }

    @Override
    public synchronized void modifyObject(final Object aObject) {
        synchronized (myObjectCache) {
            if (!isModified(aObject)) {
                myModified = true;

                if (myUseSerializableTransactions) {
                    final ThreadTransactionContext ctx = getTransactionContext();

                    // If a serializable transaction, marked modified
                    if (ctx.myNested != 0) {
                        ctx.myModified.add(aObject);
                        return;
                    }
                }

                myObjectCache.setDirty(aObject);
            }
        }
    }

    @Override
    public void open(final IFile aFile) {
        open(aFile, DEFAULT_PAGE_POOL_SIZE);
    }

    @Override
    public synchronized void open(final IFile aFile, final long aPagePoolSize) {
        Page page;
        int index;

        if (myOpened) {
            throw new StorageError(StorageError.STORAGE_ALREADY_OPENED);
        }

        initialize(aFile, aPagePoolSize);

        if (myMulticlientSupport) {
            beginThreadTransaction(myReadOnly ? COOPERATIVE_TRANSACTION : EXCLUSIVE_TRANSACTION);
        }

        final byte[] buffer = new byte[Header.SIZE_OF];
        final int readCount = aFile.read(0, buffer);
        final int corruptionError = aFile instanceof Rc4File || aFile instanceof CompressedReadWriteFile
                ? StorageError.WRONG_CIPHER_KEY : StorageError.DATABASE_CORRUPTED;

        if (readCount > 0 && readCount < Header.SIZE_OF) {
            throw new StorageError(corruptionError);
        }

        myHeader.unpack(buffer);

        if (myHeader.myCurrentRoot < 0 || myHeader.myCurrentRoot > 1) {
            throw new StorageError(corruptionError);
        }

        myTransactionId = myHeader.myTransactionId;

        if (myHeader.myDatabaseFormatVersion == 0) { // database not initialized
            if (myReadOnly) {
                throw new StorageError(StorageError.READ_ONLY_DATABASE);
            }

            final int bitmapPages;
            final long bitmapSize;
            final int bitmapIndexSize;
            final byte[] bitmapIndex;

            int indexSize = myInitIndexSize;
            int usedBitmapSize;
            long used;

            if (indexSize < DB_FIRST_USER_ID) {
                indexSize = DB_FIRST_USER_ID;
            }

            indexSize = indexSize + DB_HANDLES_PER_PAGE - 1 & ~(DB_HANDLES_PER_PAGE - 1);
            myBitmapExtentBase = DB_BITMAP_PAGES;
            myHeader.myCurrentRoot = myCurrentIndex = 0;
            used = Page.PAGE_SIZE;
            myHeader.myRoot[0].myIndex = used;
            myHeader.myRoot[0].myIndexSize = indexSize;
            myHeader.myRoot[0].myIndexUsed = DB_FIRST_USER_ID;
            myHeader.myRoot[0].myFreeList = 0;
            used += indexSize * 8L;
            myHeader.myRoot[1].myIndex = used;
            myHeader.myRoot[1].myIndexSize = indexSize;
            myHeader.myRoot[1].myIndexUsed = DB_FIRST_USER_ID;
            myHeader.myRoot[1].myFreeList = 0;
            used += indexSize * 8L;
            myHeader.myRoot[0].myShadowIndex = myHeader.myRoot[1].myIndex;
            myHeader.myRoot[1].myShadowIndex = myHeader.myRoot[0].myIndex;
            myHeader.myRoot[0].myShadowIndexSize = indexSize;
            myHeader.myRoot[1].myShadowIndexSize = indexSize;

            bitmapPages = (int) ((used + Page.PAGE_SIZE * (DB_ALLOCATION_QUANTUM * 8 - 1) - 1) / (Page.PAGE_SIZE *
                    (DB_ALLOCATION_QUANTUM * 8 - 1)));
            bitmapSize = (long) bitmapPages * Page.PAGE_SIZE;
            usedBitmapSize = (int) (used + bitmapSize >>> DB_ALLOCATION_QUANTUM_BITS + 3);

            for (index = 0; index < bitmapPages; index++) {
                page = myPool.putPage(used + (long) index * Page.PAGE_SIZE);

                final byte[] bitmap = page.myData;
                final int count = usedBitmapSize > Page.PAGE_SIZE ? Page.PAGE_SIZE : usedBitmapSize;

                for (int jndex = 0; jndex < count; jndex++) {
                    bitmap[jndex] = (byte) 0xFF;
                }

                usedBitmapSize -= Page.PAGE_SIZE;
                myPool.unfix(page);
            }

            bitmapIndexSize = (DB_BITMAP_ID + DB_BITMAP_PAGES) * 8 + Page.PAGE_SIZE - 1 & ~(Page.PAGE_SIZE - 1);
            bitmapIndex = new byte[bitmapIndexSize];
            Bytes.pack8(bitmapIndex, DB_INVALID_ID * 8, DB_FREE_HANDLE_FLAG);

            for (index = 0; index < bitmapPages; index++) {
                Bytes.pack8(bitmapIndex, (DB_BITMAP_ID + index) * 8, used | DB_PAGE_OBJECT_FLAG);
                used += Page.PAGE_SIZE;
            }

            myHeader.myRoot[0].myBitmapEnd = DB_BITMAP_ID + index;
            myHeader.myRoot[1].myBitmapEnd = DB_BITMAP_ID + index;

            while (index < DB_BITMAP_PAGES) {
                Bytes.pack8(bitmapIndex, (DB_BITMAP_ID + index) * 8, DB_FREE_HANDLE_FLAG);
                index += 1;
            }

            myHeader.myRoot[0].mySize = used;
            myHeader.myRoot[1].mySize = used;
            myUsedSize = used;
            myCommittedIndexSize = myCurrentIndexSize = DB_FIRST_USER_ID;

            myPool.write(myHeader.myRoot[1].myIndex, bitmapIndex);
            myPool.write(myHeader.myRoot[0].myIndex, bitmapIndex);

            myModified = true;
            myHeader.isDirty = true;
            myHeader.myRoot[0].mySize = myHeader.myRoot[1].mySize;
            page = myPool.putPage(0);
            myHeader.pack(page.myData);
            myPool.flush();
            myPool.modify(page);
            myHeader.myDatabaseFormatVersion = DB_DATABASE_FORMAT_VERSION;
            myHeader.pack(page.myData);
            myPool.unfix(page);
            myPool.flush();
        } else {
            final int current = myHeader.myCurrentRoot;

            myCurrentIndex = current;

            if (myHeader.myRoot[current].myIndexSize != myHeader.myRoot[current].myShadowIndexSize) {
                throw new StorageError(corruptionError);
            }

            myBitmapExtentBase = myHeader.myDatabaseFormatVersion < 2 ? 0 : DB_BITMAP_PAGES;

            if (isDirty()) {
                if (myListener != null) {
                    myListener.databaseCorrupted();
                }

                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Database was not normally closed: start recovery");
                }

                myHeader.myRoot[1 - current].mySize = myHeader.myRoot[current].mySize;
                myHeader.myRoot[1 - current].myIndexUsed = myHeader.myRoot[current].myIndexUsed;
                myHeader.myRoot[1 - current].myFreeList = myHeader.myRoot[current].myFreeList;
                myHeader.myRoot[1 - current].myIndex = myHeader.myRoot[current].myShadowIndex;
                myHeader.myRoot[1 - current].myIndexSize = myHeader.myRoot[current].myShadowIndexSize;
                myHeader.myRoot[1 - current].myShadowIndex = myHeader.myRoot[current].myIndex;
                myHeader.myRoot[1 - current].myShadowIndexSize = myHeader.myRoot[current].myIndexSize;
                myHeader.myRoot[1 - current].myBitmapEnd = myHeader.myRoot[current].myBitmapEnd;
                myHeader.myRoot[1 - current].myRootObject = myHeader.myRoot[current].myRootObject;
                myHeader.myRoot[1 - current].myClassDescList = myHeader.myRoot[current].myClassDescList;
                myHeader.myRoot[1 - current].myBitmapExtent = myHeader.myRoot[current].myBitmapExtent;

                myModified = true;
                page = myPool.putPage(0);
                myHeader.pack(page.myData);
                myPool.unfix(page);

                myPool.copy(myHeader.myRoot[1 - current].myIndex, myHeader.myRoot[current].myIndex,
                        myHeader.myRoot[current].myIndexUsed * 8L + Page.PAGE_SIZE - 1 & ~(Page.PAGE_SIZE - 1));

                if (myListener != null) {
                    myListener.recoveryCompleted();
                }

                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Database recovery completed");
                }
            }

            myCurrentIndexSize = myHeader.myRoot[1 - current].myIndexUsed;
            myCommittedIndexSize = myCurrentIndexSize;
            myUsedSize = myHeader.myRoot[current].mySize;
        }

        final int bitmapSize = myHeader.myRoot[1 - myCurrentIndex].myBitmapExtent == 0 ? DB_BITMAP_PAGES
                : DB_LARGE_BITMAP_PAGES;
        myBitmapPageAvailableSpace = new int[bitmapSize];

        for (index = 0; index < myBitmapPageAvailableSpace.length; index++) {
            myBitmapPageAvailableSpace[index] = Integer.MAX_VALUE;
        }

        myCurrRBitmapPage = myCurrPBitmapPage = 0;
        myCurrRBitmapOffs = myCurrPBitmapOffs = 0;

        myOpened = true;
        reloadScheme();

        if (myMulticlientSupport) {
            // modified = true; ??? Why it is needed here?
            endThreadTransaction(Integer.MAX_VALUE);
        } else {
            commit(); // commit scheme changes
        }
    }

    @Override
    public void open(final String aFilePath) {
        open(aFilePath, DEFAULT_PAGE_POOL_SIZE);
    }

    @Override
    public synchronized void open(final String aFilePath, final long aPagePoolSize) {
        final IFile file = aFilePath.startsWith("@") ? (IFile) new MultiFile(aFilePath.substring(1), myReadOnly,
                myNoFlush) : (IFile) new OSFile(aFilePath, myReadOnly, myNoFlush);

        try {
            open(file, aPagePoolSize);
        } catch (final StorageError ex) {
            file.close();
            throw ex;
        }
    }

    @Override
    public synchronized void open(final String aFilePath, final long aPagePoolSize, final String aCryptKey) {
        final Rc4File file = new Rc4File(aFilePath, myReadOnly, myNoFlush, aCryptKey);

        try {
            open(file, aPagePoolSize);
        } catch (final StorageError ex) {
            file.close();
            throw ex;
        }
    }

    @Override
    public void registerClassLoader(final INamedClassLoader aClassLoader) {
        if (myLoaderMap == null) {
            myLoaderMap = new HashMap();
        }

        myLoaderMap.put(aClassLoader.getName(), aClassLoader);
    }

    @Override
    public synchronized void registerCustomAllocator(final Class aClass, final CustomAllocator aAllocator) {
        synchronized (myObjectCache) {
            final ClassDescriptor classDescriptor = getClassDescriptor(aClass);

            classDescriptor.myAllocator = aAllocator;
            storeObject0(classDescriptor, false);

            if (myCustomAllocatorMap == null) {
                myCustomAllocatorMap = new HashMap();
                myCustomAllocatorList = new ArrayList();
            }

            myCustomAllocatorMap.put(aClass, aAllocator);
            myCustomAllocatorList.add(aAllocator);
            reserveLocation(aAllocator.getSegmentBase(), aAllocator.getSegmentSize());
        }
    }

    @Override
    public synchronized void rollback() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        if (myUseSerializableTransactions && getTransactionContext().myNested != 0) {
            // Store should not be used in serializable transaction mode
            throw new StorageError(StorageError.INVALID_OPERATION, "rollback");
        }

        myObjectCache.invalidate();

        synchronized (myObjectCache) {
            if (!myModified) {
                return;
            }

            rollback0();
            myModified = false;

            if (myReloadObjectsOnRollback) {
                myObjectCache.reload();
            } else {
                myObjectCache.clear();
            }
        }
    }

    @Override
    public void rollbackSerializableTransaction() {
        if (!isInsideThreadTransaction()) {
            throw new StorageError(StorageError.NOT_IN_TRANSACTION);
        }

        rollbackThreadTransaction();
    }

    @Override
    public void rollbackExclusiveTransaction() {
        rollbackThreadTransaction();
    }

    @Override
    public void rollbackCooperativeTransaction() {
        rollbackThreadTransaction();
    }

    @Override
    public ClassLoader setClassLoader(final ClassLoader aClassLoader) {
        final ClassLoader prev = aClassLoader;
        myLoader = aClassLoader;
        return prev;
    }

    @Override
    public void setCustomSerializer(final CustomSerializer aSerializer) {
        mySerializer = aSerializer;
    }

    @Override
    public void setGcThreshold(final long aMaxAllocatedDelta) {
        myGcThreshold = aMaxAllocatedDelta;
    }

    @Override
    public StorageListener setListener(final StorageListener aStorageListener) {
        final StorageListener prevListener = myListener;

        myListener = aStorageListener;

        return prevListener;
    }

    @Override
    public void setProperties(final Properties aProperties) {
        String value;

        myProperties.putAll(aProperties);

        if ((value = aProperties.getProperty(Constants.IMPLICIT_VALUES)) != null) {
            ClassDescriptor.TREAT_ANY_NONPERSISTENT_OBJ_AS_VALUE = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.SERIALIZE_TRANSIENT_OBJECTS)) != null) {
            ClassDescriptor.SERIALIZE_NONPERSISTENT_OBJS = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.OBJECT_CACHE_INIT_SIZE)) != null) {
            myObjectCacheInitSize = (int) getIntegerValue(value);

            if (myObjectCacheInitSize <= 0) {
                throw new IllegalArgumentException(LOGGER.getMessage(MessageCodes.SB_030));
            }
        }

        if ((value = aProperties.getProperty(Constants.OBJECT_CACHE_KIND)) != null) {
            myCacheKind = value;
        }

        if ((value = aProperties.getProperty(Constants.OBJECT_INDEX_INIT_SIZE)) != null) {
            myInitIndexSize = (int) getIntegerValue(value);
        }

        if ((value = aProperties.getProperty(Constants.EXTENSION_QUANTUM)) != null) {
            myExtensionQuantum = getIntegerValue(value);
        }

        if ((value = aProperties.getProperty(Constants.GC_THRESHOLD)) != null) {
            myGcThreshold = getIntegerValue(value);
        }

        if ((value = aProperties.getProperty(Constants.FILE_READONLY)) != null) {
            myReadOnly = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.FILE_NOFLUSH)) != null) {
            myNoFlush = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.ALT_BTREE)) != null) {
            myAlternativeBtree = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.BACKGROUND_GC)) != null) {
            myBackgroundGc = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.STRING_ENCODING)) != null) {
            myEncoding = value;
        }

        if ((value = aProperties.getProperty(Constants.LOCK_FILE)) != null) {
            myLockFile = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.REPLICATION_ACK)) != null) {
            myReplicationAck = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.CONCURRENT_ITERATOR)) != null) {
            myConcurrentIterator = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.SLAVE_CONNECTION_TIMEOUT)) != null) {
            mySlaveConnectionTimeout = (int) getIntegerValue(value);
        }

        if ((value = aProperties.getProperty(Constants.FORCE_STORE)) != null) {
            myForceStore = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.PAGE_POOL_LRU_LIMIT)) != null) {
            myPagePoolLruLimit = getIntegerValue(value);
        }

        if ((value = aProperties.getProperty(Constants.MULTICLIENT_SUPPORT)) != null) {
            myMulticlientSupport = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.RELOAD_OBJECTS_ON_ROLLBACK)) != null) {
            myReloadObjectsOnRollback = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.REUSE_OID)) != null) {
            myReuseOid = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.SERIALIZE_SYSTEM_COLLECTIONS)) != null) {
            mySerializeSystemCollections = getBooleanValue(value);
        }

        if ((value = aProperties.getProperty(Constants.COMPATIBILITY_MODE)) != null) {
            myCompatibilityMode = (int) getIntegerValue(value);
        }

        if (myMulticlientSupport && myBackgroundGc) {
            throw new IllegalArgumentException("In mutliclient access mode bachround GC is not supported");
        }
    }

    @Override
    public void setProperty(final String aName, final Object aValue) {
        myProperties.put(aName, aValue);

        if (aName.equals(Constants.IMPLICIT_VALUES)) {
            ClassDescriptor.TREAT_ANY_NONPERSISTENT_OBJ_AS_VALUE = getBooleanValue(aValue);
        } else if (aName.equals(Constants.SERIALIZE_TRANSIENT_OBJECTS)) {
            ClassDescriptor.SERIALIZE_NONPERSISTENT_OBJS = getBooleanValue(aValue);
        } else if (aName.equals(Constants.OBJECT_CACHE_INIT_SIZE)) {
            myObjectCacheInitSize = (int) getIntegerValue(aValue);

            if (myObjectCacheInitSize <= 0) {
                throw new IllegalArgumentException("Initial object cache size should be positive");
            }
        } else if (aName.equals(Constants.OBJECT_CACHE_KIND)) {
            myCacheKind = (String) aValue;
        } else if (aName.equals(Constants.OBJECT_INDEX_INIT_SIZE)) {
            myInitIndexSize = (int) getIntegerValue(aValue);
        } else if (aName.equals(Constants.EXTENSION_QUANTUM)) {
            myExtensionQuantum = getIntegerValue(aValue);
        } else if (aName.equals(Constants.GC_THRESHOLD)) {
            myGcThreshold = getIntegerValue(aValue);
        } else if (aName.equals(Constants.FILE_READONLY)) {
            myReadOnly = getBooleanValue(aValue);
        } else if (aName.equals(Constants.FILE_NOFLUSH)) {
            myNoFlush = getBooleanValue(aValue);
        } else if (aName.equals(Constants.ALT_BTREE)) {
            myAlternativeBtree = getBooleanValue(aValue);
        } else if (aName.equals(Constants.BACKGROUND_GC)) {
            myBackgroundGc = getBooleanValue(aValue);
        } else if (aName.equals(Constants.STRING_ENCODING)) {
            myEncoding = aValue == null ? null : aValue.toString();
        } else if (aName.equals(Constants.LOCK_FILE)) {
            myLockFile = getBooleanValue(aValue);
        } else if (aName.equals(Constants.REPLICATION_ACK)) {
            myReplicationAck = getBooleanValue(aValue);
        } else if (aName.equals(Constants.CONCURRENT_ITERATOR)) {
            myConcurrentIterator = getBooleanValue(aValue);
        } else if (aName.equals(Constants.SLAVE_CONNECTION_TIMEOUT)) {
            mySlaveConnectionTimeout = (int) getIntegerValue(aValue);
        } else if (aName.equals(Constants.FORCE_STORE)) {
            myForceStore = getBooleanValue(aValue);
        } else if (aName.equals(Constants.PAGE_POOL_LRU_LIMIT)) {
            myPagePoolLruLimit = getIntegerValue(aValue);
        } else if (aName.equals(Constants.MULTICLIENT_SUPPORT)) {
            myMulticlientSupport = getBooleanValue(aValue);
        } else if (aName.equals(Constants.RELOAD_OBJECTS_ON_ROLLBACK)) {
            myReloadObjectsOnRollback = getBooleanValue(aValue);
        } else if (aName.equals(Constants.REUSE_OID)) {
            myReuseOid = getBooleanValue(aValue);
        } else if (aName.equals(Constants.COMPATIBILITY_MODE)) {
            myCompatibilityMode = (int) getIntegerValue(aValue);
        } else if (aName.equals(Constants.SERIALIZE_SYSTEM_COLLECTIONS)) {
            mySerializeSystemCollections = getBooleanValue(aValue);
        }

        if (myMulticlientSupport && myBackgroundGc) {
            throw new IllegalArgumentException("In mutliclient access mode, background GC is not supported");
        }
    }

    @Override
    public boolean setRecursiveLoading(final Class aType, final boolean aRecursuveLoadFlag) {
        synchronized (myRecursiveLoadingPolicy) {
            final Object prevValue = myRecursiveLoadingPolicy.put(aType, aRecursuveLoadFlag);

            myRecursiveLoadingPolicyDefined = true;

            return prevValue == null ? true : ((Boolean) prevValue).booleanValue();
        }
    }

    @Override
    public synchronized void setRoot(final Object aRoot) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        if (aRoot == null) {
            myHeader.myRoot[1 - myCurrentIndex].myRootObject = 0;
        } else {
            if (!isPersistent(aRoot)) {
                storeObject0(aRoot, false);
            }

            myHeader.myRoot[1 - myCurrentIndex].myRootObject = getOid(aRoot);
        }

        myModified = true;
    }

    /**
     * Associate transaction context with the thread This method can be used by application to share the same
     * transaction between multiple threads
     *
     * @param aContext new transaction context
     * @return transaction context previously associated with this thread
     */
    @Override
    public ThreadTransactionContext setTransactionContext(final ThreadTransactionContext aContext) {
        final ThreadTransactionContext oldContext = (ThreadTransactionContext) myTransactionContext.get();

        myTransactionContext.set(aContext);

        return oldContext;
    }

    @Override
    public void store(final Object aObject) {
        if (aObject instanceof IPersistent) {
            ((IPersistent) aObject).store();
        } else {
            synchronized (this) {
                synchronized (myObjectCache) {
                    synchronized (myObjMap) {
                        final ObjectMap.Entry entry = myObjMap.put(aObject);

                        if ((entry.myState & Persistent.RAW) != 0) {
                            throw new StorageError(StorageError.ACCESS_TO_STUB);
                        }

                        storeObject(aObject);
                        entry.myState &= ~Persistent.DIRTY;
                    }
                }
            }
        }
    }

    @Override
    public void storeFinalizedObject(final Object aObject) {
        if (myOpened) {
            synchronized (myObjectCache) {
                if (getOid(aObject) != 0) {
                    storeObject0(aObject, true);
                }
            }
        }
    }

    @Override
    public synchronized void storeObject(final Object aObject) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        if (myUseSerializableTransactions && getTransactionContext().myNested != 0) {
            // Store should not be used in serializable transaction mode
            throw new StorageError(StorageError.INVALID_OPERATION, "store object");
        }

        synchronized (myObjectCache) {
            storeObject0(aObject, false);
        }
    }

    @Override
    public void decache(final Object aObject) {
        myObjectCache.remove(getOid(aObject));
    }

    protected OidHashTable createObjectCache(final String aKind, final long aPagePoolSize,
            final int aObjectCacheSize) {
        if ("strong".equals(aKind)) {
            return new StrongHashTable(this, aObjectCacheSize);
        }

        if ("soft".equals(aKind)) {
            return new SoftHashTable(this, aObjectCacheSize);
        }

        if ("weak".equals(aKind)) {
            return new WeakHashTable(this, aObjectCacheSize);
        }

        if ("pinned".equals(aKind)) {
            return new PinWeakHashTable(this, aObjectCacheSize);
        }

        if ("lru".equals(aKind)) {
            return new LruObjectCache(this, aObjectCacheSize);
        }

        return aPagePoolSize == INFINITE_PAGE_POOL ? (OidHashTable) new StrongHashTable(this, aObjectCacheSize)
                : (OidHashTable) new LruObjectCache(this, aObjectCacheSize);
    }

    protected void initialize(final IFile aFile, final long aPagePoolSize) {
        myFile = aFile;

        if (myLockFile && !myMulticlientSupport) {
            if (!aFile.tryLock(myReadOnly)) {
                throw new StorageError(StorageError.STORAGE_IS_USED);
            }
        }

        myDirtyPagesMap = new int[DB_DIRTY_PAGE_BITMAP_SIZE / 4 + 1];
        myGcThreshold = Long.MAX_VALUE;
        myBackgroundGcMonitor = new Object();
        myBackgroundGcStartMonitor = new Object();
        myGcThread = null;
        myGcActive = false;
        myGcDone = false;
        myAllocatedDelta = 0;

        myReservedChain = null;
        myCloneList = null;
        myInsideCloneBitmap = false;

        myNumOfNestedTransactions = 0;
        myNumOfBlockedTransactions = 0;
        myNumOfCommittedTransactions = 0;
        myScheduledCommitTime = Long.MAX_VALUE;
        myTransactionMonitor = new Object();
        myTransactionLock = new PersistentResource();

        myModified = false;

        myObjectCache = createObjectCache(myCacheKind, aPagePoolSize, myObjectCacheInitSize);
        myObjMap = new ObjectMap(myObjectCacheInitSize);

        myClassDescriptorMap = new HashMap();
        myClassDescriptor = null;

        myRecursiveLoadingPolicy = new HashMap();
        myRecursiveLoadingPolicyDefined = false;

        myHeader = new Header();
        myPool = new PagePool((int) (aPagePoolSize / Page.PAGE_SIZE), myPagePoolLruLimit);
        myPool.open(aFile);
    }

    protected boolean isDirty() {
        return myHeader.isDirty;
    }

    protected int swizzle(final Object aObject, final boolean aFinalized) {
        int oid = 0;

        if (aObject != null) {
            if (!isPersistent(aObject)) {
                storeObject0(aObject, aFinalized);
            }

            oid = getOid(aObject);
        }

        return oid;
    }

    protected Object unswizzle(final int aOID, final Class aClass, final boolean aRecursiveLoading) {
        if (aOID == 0) {
            return null;
        }

        if (aRecursiveLoading) {
            return lookupObject(aOID, aClass);
        }

        Object stub = myObjectCache.get(aOID);

        if (stub != null) {
            return stub;
        }

        ClassDescriptor desc;

        if (aClass == Object.class || (desc = findClassDescriptor(aClass)) == null || desc.hasSubclasses) {
            final long pos = getPosition(aOID);
            final int offs = (int) pos & Page.PAGE_SIZE - 1;

            if ((offs & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
                throw new StorageError(StorageError.DELETED_OBJECT);
            }

            final Page pg = myPool.getPage(pos - offs);
            final int typeOid = ObjectHeader.getType(pg.myData, offs & ~DB_FLAGS_MASK);

            myPool.unfix(pg);
            desc = findClassDescriptor(typeOid);
        }

        stub = desc.newInstance();
        assignOid(stub, aOID, true);
        myObjectCache.put(aOID, stub);

        return stub;
    }

    final long allocate(final long aSize, final int aOID) {
        long size = aSize;

        synchronized (myObjectCache) {
            setDirty();
            size = size + DB_ALLOCATION_QUANTUM - 1 & ~(DB_ALLOCATION_QUANTUM - 1);
            Assert.that(size != 0);
            myAllocatedDelta += size;

            if (myAllocatedDelta > myGcThreshold && !myInsideCloneBitmap) {
                gc0();
            }

            int objBitSize = (int) (size >> DB_ALLOCATION_QUANTUM_BITS);
            Assert.that(objBitSize == size >> DB_ALLOCATION_QUANTUM_BITS);
            long pos;
            int holeBitSize = 0;
            final int alignment = (int) size & Page.PAGE_SIZE - 1;
            int offset;
            int firstPage;
            int lastPage;
            int index;
            int j;
            int holeBeforeFreePage = 0;
            int freeBitmapPage = 0;
            final int current = 1 - myCurrentIndex;
            Page pg;

            lastPage = myHeader.myRoot[current].myBitmapEnd - DB_BITMAP_ID;
            myUsedSize += size;

            if (alignment == 0) {
                firstPage = myCurrPBitmapPage;
                offset = myCurrPBitmapOffs + INC - 1 & ~(INC - 1);
            } else {
                firstPage = myCurrRBitmapPage;
                offset = myCurrRBitmapOffs;
            }

            while (true) {
                if (alignment == 0) {
                    // allocate page object
                    for (index = firstPage; index < lastPage; index++) {
                        final int spaceNeeded = objBitSize - holeBitSize < PAGE_BITS ? objBitSize - holeBitSize
                                : PAGE_BITS;

                        if (myBitmapPageAvailableSpace[index] <= spaceNeeded) {
                            holeBitSize = 0;
                            offset = 0;
                            continue;
                        }

                        pg = getBitmapPage(index);
                        int startOffs = offset;

                        while (offset < Page.PAGE_SIZE) {
                            if (pg.myData[offset++] != 0) {
                                offset = offset + INC - 1 & ~(INC - 1);
                                holeBitSize = 0;
                            } else if ((holeBitSize += 8) == objBitSize) {
                                pos = ((long) index * Page.PAGE_SIZE + offset) * 8 -
                                        holeBitSize << DB_ALLOCATION_QUANTUM_BITS;

                                if (wasReserved(pos, size)) {
                                    startOffs = offset = offset + INC - 1 & ~(INC - 1);
                                    holeBitSize = 0;
                                    continue;
                                }

                                reserveLocation(pos, size);
                                myCurrPBitmapPage = index;
                                myCurrPBitmapOffs = offset;
                                extend(pos + size);

                                if (aOID != 0) {
                                    final long prev = getPosition(aOID);
                                    final int marker = (int) prev & DB_FLAGS_MASK;
                                    myPool.copy(pos, prev - marker, size);
                                    setPosition(aOID, pos | marker | DB_MODIFIED_FLAG);
                                }

                                myPool.unfix(pg);
                                pg = putBitmapPage(index);

                                int holeBytes = holeBitSize >> 3;

                                if (holeBytes > offset) {
                                    memset(pg, 0, 0xFF, offset);
                                    holeBytes -= offset;
                                    myPool.unfix(pg);
                                    pg = putBitmapPage(--index);
                                    offset = Page.PAGE_SIZE;
                                }

                                while (holeBytes > Page.PAGE_SIZE) {
                                    memset(pg, 0, 0xFF, Page.PAGE_SIZE);
                                    holeBytes -= Page.PAGE_SIZE;
                                    myBitmapPageAvailableSpace[index] = 0;
                                    myPool.unfix(pg);
                                    pg = putBitmapPage(--index);
                                }

                                memset(pg, offset - holeBytes, 0xFF, holeBytes);
                                commitLocation();
                                myPool.unfix(pg);

                                return pos;
                            }
                        }

                        if (startOffs == 0 && holeBitSize == 0 && spaceNeeded < myBitmapPageAvailableSpace[index]) {
                            myBitmapPageAvailableSpace[index] = spaceNeeded;
                        }

                        offset = 0;
                        myPool.unfix(pg);
                    }
                } else {
                    for (index = firstPage; index < lastPage; index++) {
                        final int spaceNeeded = objBitSize - holeBitSize < PAGE_BITS ? objBitSize - holeBitSize
                                : PAGE_BITS;

                        if (myBitmapPageAvailableSpace[index] <= spaceNeeded) {
                            holeBitSize = 0;
                            offset = 0;
                            continue;
                        }

                        pg = getBitmapPage(index);

                        int startOffs = offset;

                        while (offset < Page.PAGE_SIZE) {
                            final int mask = pg.myData[offset] & 0xFF;

                            if (holeBitSize + Bitmap.FIRST_HOLE_SIZE[mask] >= objBitSize) {
                                pos = ((long) index * Page.PAGE_SIZE + offset) * 8 -
                                        holeBitSize << DB_ALLOCATION_QUANTUM_BITS;

                                if (wasReserved(pos, size)) {
                                    startOffs = offset += 1;
                                    holeBitSize = 0;
                                    continue;
                                }

                                reserveLocation(pos, size);
                                myCurrRBitmapPage = index;
                                myCurrRBitmapOffs = offset;
                                extend(pos + size);

                                if (aOID != 0) {
                                    final long prev = getPosition(aOID);
                                    final int marker = (int) prev & DB_FLAGS_MASK;

                                    myPool.copy(pos, prev - marker, size);
                                    setPosition(aOID, pos | marker | DB_MODIFIED_FLAG);
                                }

                                myPool.unfix(pg);
                                pg = putBitmapPage(index);
                                pg.myData[offset] |= (byte) ((1 << objBitSize - holeBitSize) - 1);

                                if (holeBitSize != 0) {
                                    if (holeBitSize > offset * 8) {
                                        memset(pg, 0, 0xFF, offset);
                                        holeBitSize -= offset * 8;
                                        myPool.unfix(pg);
                                        pg = putBitmapPage(--index);
                                        offset = Page.PAGE_SIZE;
                                    }

                                    while (holeBitSize > PAGE_BITS) {
                                        memset(pg, 0, 0xFF, Page.PAGE_SIZE);
                                        holeBitSize -= PAGE_BITS;
                                        myBitmapPageAvailableSpace[index] = 0;
                                        myPool.unfix(pg);
                                        pg = putBitmapPage(--index);
                                    }

                                    while ((holeBitSize -= 8) > 0) {
                                        pg.myData[--offset] = (byte) 0xFF;
                                    }

                                    pg.myData[offset - 1] |= (byte) ~((1 << -holeBitSize) - 1);
                                }

                                myPool.unfix(pg);
                                commitLocation();

                                return pos;
                            } else if (Bitmap.MAX_HOLE_SIZE[mask] >= objBitSize) {
                                final int holeBitOffset = Bitmap.MAX_HOLE_OFFSET[mask];

                                pos = ((long) index * Page.PAGE_SIZE + offset) * 8 +
                                        holeBitOffset << DB_ALLOCATION_QUANTUM_BITS;

                                if (wasReserved(pos, size)) {
                                    startOffs = offset += 1;
                                    holeBitSize = 0;
                                    continue;
                                }

                                reserveLocation(pos, size);
                                myCurrRBitmapPage = index;
                                myCurrRBitmapOffs = offset;
                                extend(pos + size);

                                if (aOID != 0) {
                                    final long prev = getPosition(aOID);
                                    final int marker = (int) prev & DB_FLAGS_MASK;

                                    myPool.copy(pos, prev - marker, size);
                                    setPosition(aOID, pos | marker | DB_MODIFIED_FLAG);
                                }

                                myPool.unfix(pg);
                                pg = putBitmapPage(index);
                                pg.myData[offset] |= (byte) ((1 << objBitSize) - 1 << holeBitOffset);
                                myPool.unfix(pg);
                                commitLocation();

                                return pos;
                            }

                            offset += 1;

                            if (Bitmap.LAST_HOLE_SIZE[mask] == 8) {
                                holeBitSize += 8;
                            } else {
                                holeBitSize = Bitmap.LAST_HOLE_SIZE[mask];
                            }
                        }

                        if (startOffs == 0 && holeBitSize == 0 && spaceNeeded < myBitmapPageAvailableSpace[index]) {
                            myBitmapPageAvailableSpace[index] = spaceNeeded;
                        }

                        offset = 0;
                        myPool.unfix(pg);
                    }
                }

                if (firstPage == 0) {
                    if (freeBitmapPage > index) {
                        index = freeBitmapPage;
                        holeBitSize = holeBeforeFreePage;
                    }

                    objBitSize -= holeBitSize;
                    // number of bits reserved for the object and aligned on page boundary
                    final int skip = objBitSize + Page.PAGE_SIZE / DB_ALLOCATION_QUANTUM - 1 & ~(Page.PAGE_SIZE /
                            DB_ALLOCATION_QUANTUM - 1);
                    // page aligned position after allocated object
                    pos = ((long) index << DB_BITMAP_SEGMENT_BITS) + ((long) skip << DB_ALLOCATION_QUANTUM_BITS);

                    long extension = size > myExtensionQuantum ? size : myExtensionQuantum;
                    int oldIndexSize = 0;
                    long oldIndex = 0;
                    int morePages = (int) ((extension + Page.PAGE_SIZE * (DB_ALLOCATION_QUANTUM * 8 - 1) - 1) /
                            (Page.PAGE_SIZE * (DB_ALLOCATION_QUANTUM * 8 - 1)));

                    if (index + morePages > DB_LARGE_BITMAP_PAGES) {
                        throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
                    }

                    if (index <= DB_BITMAP_PAGES && index + morePages > DB_BITMAP_PAGES) {
                        // We are out of space mapped by memory default allocation bitmap
                        oldIndexSize = myHeader.myRoot[current].myIndexSize;

                        if (oldIndexSize <= myCurrentIndexSize + DB_LARGE_BITMAP_PAGES - DB_BITMAP_PAGES) {
                            int newIndexSize = oldIndexSize;

                            oldIndex = myHeader.myRoot[current].myIndex;

                            do {
                                newIndexSize <<= 1;

                                if (newIndexSize < 0) {
                                    newIndexSize = Integer.MAX_VALUE & ~(DB_HANDLES_PER_PAGE - 1);

                                    if (newIndexSize < myCurrentIndexSize + DB_LARGE_BITMAP_PAGES - DB_BITMAP_PAGES) {
                                        throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
                                    }

                                    break;
                                }
                            } while (newIndexSize <= myCurrentIndexSize + DB_LARGE_BITMAP_PAGES - DB_BITMAP_PAGES);

                            if (size + newIndexSize * 8L > myExtensionQuantum) {
                                extension = size + newIndexSize * 8L;
                                morePages = (int) ((extension + Page.PAGE_SIZE * (DB_ALLOCATION_QUANTUM * 8 - 1) -
                                        1) / (Page.PAGE_SIZE * (DB_ALLOCATION_QUANTUM * 8 - 1)));
                            }

                            extend(pos + (long) morePages * Page.PAGE_SIZE + newIndexSize * 8L);
                            final long newIndex = pos + (long) morePages * Page.PAGE_SIZE;

                            fillBitmap(pos + (skip >> 3) + (long) morePages * (Page.PAGE_SIZE /
                                    DB_ALLOCATION_QUANTUM / 8), newIndexSize >>> DB_ALLOCATION_QUANTUM_BITS);

                            myPool.copy(newIndex, oldIndex, oldIndexSize * 8L);
                            myHeader.myRoot[current].myIndex = newIndex;
                            myHeader.myRoot[current].myIndexSize = newIndexSize;
                        }

                        final int[] newBitmapPageAvailableSpace = new int[DB_LARGE_BITMAP_PAGES];

                        System.arraycopy(myBitmapPageAvailableSpace, 0, newBitmapPageAvailableSpace, 0,
                                DB_BITMAP_PAGES);

                        for (j = DB_BITMAP_PAGES; j < DB_LARGE_BITMAP_PAGES; j++) {
                            newBitmapPageAvailableSpace[j] = Integer.MAX_VALUE;
                        }

                        myBitmapPageAvailableSpace = newBitmapPageAvailableSpace;

                        for (j = 0; j < DB_LARGE_BITMAP_PAGES - DB_BITMAP_PAGES; j++) {
                            setPosition(myCurrentIndexSize + j, DB_FREE_HANDLE_FLAG);
                        }

                        myHeader.myRoot[current].myBitmapExtent = myCurrentIndexSize;
                        myHeader.myRoot[current].myIndexUsed = myCurrentIndexSize += DB_LARGE_BITMAP_PAGES -
                                DB_BITMAP_PAGES;
                    }

                    extend(pos + (long) morePages * Page.PAGE_SIZE);

                    long adr = pos;
                    int len = objBitSize >> 3;

                    // fill bitmap pages used for allocation of object space with 0xFF
                    while (len >= Page.PAGE_SIZE) {
                        pg = myPool.putPage(adr);
                        memset(pg, 0, 0xFF, Page.PAGE_SIZE);
                        myPool.unfix(pg);
                        adr += Page.PAGE_SIZE;
                        len -= Page.PAGE_SIZE;
                    }

                    // fill part of last page responsible for allocation of object space
                    pg = myPool.putPage(adr);
                    memset(pg, 0, 0xFF, len);
                    pg.myData[len] = (byte) ((1 << (objBitSize & 7)) - 1);
                    myPool.unfix(pg);

                    // mark in bitmap newly allocated object
                    fillBitmap(pos + (skip >> 3), morePages * (Page.PAGE_SIZE / DB_ALLOCATION_QUANTUM / 8));

                    j = index;

                    while (--morePages >= 0) {
                        setPosition(getBitmapPageId(j++), pos | DB_PAGE_OBJECT_FLAG | DB_MODIFIED_FLAG);
                        pos += Page.PAGE_SIZE;
                    }

                    myHeader.myRoot[current].myBitmapEnd = j + DB_BITMAP_ID;
                    j = index + objBitSize / PAGE_BITS;

                    if (alignment != 0) {
                        myCurrRBitmapPage = j;
                        myCurrRBitmapOffs = 0;
                    } else {
                        myCurrPBitmapPage = j;
                        myCurrPBitmapOffs = 0;
                    }

                    while (j > index) {
                        myBitmapPageAvailableSpace[--j] = 0;
                    }

                    pos = (long) index * Page.PAGE_SIZE * 8 - holeBitSize << DB_ALLOCATION_QUANTUM_BITS;

                    if (aOID != 0) {
                        final long prev = getPosition(aOID);
                        final int marker = (int) prev & DB_FLAGS_MASK;

                        myPool.copy(pos, prev - marker, size);
                        setPosition(aOID, pos | marker | DB_MODIFIED_FLAG);
                    }

                    if (holeBitSize != 0) {
                        reserveLocation(pos, size);

                        while (holeBitSize > PAGE_BITS) {
                            holeBitSize -= PAGE_BITS;
                            pg = putBitmapPage(--index);
                            memset(pg, 0, 0xFF, Page.PAGE_SIZE);
                            myBitmapPageAvailableSpace[index] = 0;
                            myPool.unfix(pg);
                        }

                        pg = putBitmapPage(--index);
                        offset = Page.PAGE_SIZE;

                        while ((holeBitSize -= 8) > 0) {
                            pg.myData[--offset] = (byte) 0xFF;
                        }

                        pg.myData[offset - 1] |= (byte) ~((1 << -holeBitSize) - 1);
                        myPool.unfix(pg);
                        commitLocation();
                    }

                    if (oldIndex != 0) {
                        free(oldIndex, oldIndexSize * 8L);
                    }

                    return pos;
                }

                if (myGcThreshold != Long.MAX_VALUE && !myGcDone && !myGcActive && !myInsideCloneBitmap) {
                    myAllocatedDelta -= size;
                    myUsedSize -= size;
                    gc0();
                    myCurrRBitmapPage = myCurrPBitmapPage = 0;
                    myCurrRBitmapOffs = myCurrPBitmapOffs = 0;
                    return allocate(size, aOID);
                }

                freeBitmapPage = index;
                holeBeforeFreePage = holeBitSize;
                holeBitSize = 0;
                lastPage = firstPage + 1;
                firstPage = 0;
                offset = 0;
            }
        }
    }

    int allocateId() {
        synchronized (myObjectCache) {
            final int curr = 1 - myCurrentIndex;
            int oid;

            setDirty();

            if (myReuseOid && (oid = myHeader.myRoot[curr].myFreeList) != 0) {
                myHeader.myRoot[curr].myFreeList = (int) (getPosition(oid) >> DB_FLAGS_BITS);
                Assert.that(myHeader.myRoot[curr].myFreeList >= 0);
                myDirtyPagesMap[oid >>> DB_HANDLES_PER_PAGE_BITS + 5] |= 1 << (oid >>> DB_HANDLES_PER_PAGE_BITS & 31);
                return oid;
            }

            if (myCurrentIndexSize > DB_MAX_OBJECT_OID) {
                throw new StorageError(StorageError.TOO_MUCH_OBJECTS);
            }

            if (myCurrentIndexSize >= myHeader.myRoot[curr].myIndexSize) {
                final int oldIndexSize = myHeader.myRoot[curr].myIndexSize;
                int newIndexSize = oldIndexSize << 1;

                if (newIndexSize < oldIndexSize) {
                    newIndexSize = Integer.MAX_VALUE & ~(DB_HANDLES_PER_PAGE - 1);

                    if (newIndexSize <= oldIndexSize) {
                        throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
                    }
                }

                final long newIndex = allocate(newIndexSize * 8L, 0);

                if (myCurrentIndexSize >= myHeader.myRoot[curr].myIndexSize) {
                    final long oldIndex = myHeader.myRoot[curr].myIndex;
                    myPool.copy(newIndex, oldIndex, myCurrentIndexSize * 8L);
                    myHeader.myRoot[curr].myIndex = newIndex;
                    myHeader.myRoot[curr].myIndexSize = newIndexSize;
                    free(oldIndex, oldIndexSize * 8L);
                } else {
                    // index was already reallocated
                    free(newIndex, newIndexSize * 8L);
                }
            }

            oid = myCurrentIndexSize;
            myHeader.myRoot[curr].myIndexUsed = ++myCurrentIndexSize;

            return oid;
        }
    }

    int allocatePage() {
        final int oid = allocateId();

        setPosition(oid, allocate(Page.PAGE_SIZE, 0) | DB_PAGE_OBJECT_FLAG | DB_MODIFIED_FLAG);

        return oid;
    }

    void assignOid(final Object aObj, final int aOid, final boolean aRaw) {
        if (aObj instanceof IPersistent) {
            ((IPersistent) aObj).assignOid(this, aOid, aRaw);
        } else {
            synchronized (myObjMap) {
                final ObjectMap.Entry entry = myObjMap.put(aObj);

                entry.myOID = aOid;

                if (aRaw) {
                    entry.myState = Persistent.RAW;
                }
            }
        }

        if (myListener != null) {
            myListener.onObjectAssignOid(aObj);
        }
    }

    final void cloneBitmap(final long aPosition, final long aSize) {
        long position = aPosition;
        long size = aSize;

        synchronized (myObjectCache) {
            if (myInsideCloneBitmap) {
                Assert.that(size == Page.PAGE_SIZE);
                myCloneList = new CloneNode(position, myCloneList);
            } else {
                myInsideCloneBitmap = true;

                while (true) {
                    final long quantumNum = position >>> DB_ALLOCATION_QUANTUM_BITS;

                    int objBitSize = (int) (size + DB_ALLOCATION_QUANTUM - 1 >>> DB_ALLOCATION_QUANTUM_BITS);
                    int pageId = (int) (quantumNum >>> Page.PAGE_SIZE_LOG + 3);
                    int offset = (int) (quantumNum & Page.PAGE_SIZE * 8 - 1) >> 3;
                    final int bitOffset = (int) quantumNum & 7;
                    int oid = getBitmapPageId(pageId);

                    position = getPosition(oid);

                    if ((position & DB_MODIFIED_FLAG) == 0) {
                        myDirtyPagesMap[oid >>> DB_HANDLES_PER_PAGE_BITS + 5] |=
                                1 << (oid >>> DB_HANDLES_PER_PAGE_BITS & 31);
                        allocate(Page.PAGE_SIZE, oid);
                        cloneBitmap(position & ~DB_FLAGS_MASK, Page.PAGE_SIZE);
                    }

                    if (objBitSize > 8 - bitOffset) {
                        objBitSize -= 8 - bitOffset;
                        offset += 1;

                        while (objBitSize + offset * 8 > Page.PAGE_SIZE * 8) {
                            oid = getBitmapPageId(++pageId);
                            position = getPosition(oid);

                            if ((position & DB_MODIFIED_FLAG) == 0) {
                                myDirtyPagesMap[oid >>> DB_HANDLES_PER_PAGE_BITS + 5] |=
                                        1 << (oid >>> DB_HANDLES_PER_PAGE_BITS & 31);
                                allocate(Page.PAGE_SIZE, oid);
                                cloneBitmap(position & ~DB_FLAGS_MASK, Page.PAGE_SIZE);
                            }

                            objBitSize -= (Page.PAGE_SIZE - offset) * 8;
                            offset = 0;
                        }
                    }

                    if (myCloneList == null) {
                        break;
                    }

                    position = myCloneList.myPosition;
                    size = Page.PAGE_SIZE;
                    myCloneList = myCloneList.myNext;
                }

                myInsideCloneBitmap = false;
            }
        }
    }

    final void commitLocation() {
        myReservedChain = myReservedChain.myNext;
    }

    final void extend(final long aSize) {
        if (aSize > myHeader.myRoot[1 - myCurrentIndex].mySize) {
            myHeader.myRoot[1 - myCurrentIndex].mySize = aSize;
        }
    }

    final void fillBitmap(final long aAddress, final int aLength) {
        long address = aAddress;
        int length = aLength;

        while (true) {
            final int offset = (int) address & Page.PAGE_SIZE - 1;
            final Page page = myPool.putPage(address - offset);

            if (Page.PAGE_SIZE - offset >= length) {
                memset(page, offset, 0xFF, length);
                myPool.unfix(page);

                break;
            } else {
                memset(page, offset, 0xFF, Page.PAGE_SIZE - offset);
                myPool.unfix(page);
                address += Page.PAGE_SIZE - offset;
                length -= Page.PAGE_SIZE - offset;
            }
        }
    }

    final ClassDescriptor findClassDescriptor(final Class aClass) {
        return (ClassDescriptor) myClassDescriptorMap.get(aClass);
    }

    final ClassDescriptor findClassDescriptor(final int aOID) {
        return (ClassDescriptor) lookupObject(aOID, ClassDescriptor.class);
    }

    final void free(final long aPosition, final long aSize) {
        synchronized (myObjectCache) {
            Assert.that(aPosition != 0 && (aPosition & DB_ALLOCATION_QUANTUM - 1) == 0);

            final long quantumNum = aPosition >>> DB_ALLOCATION_QUANTUM_BITS;
            int objBitSize = (int) (aSize + DB_ALLOCATION_QUANTUM - 1 >>> DB_ALLOCATION_QUANTUM_BITS);
            int pageId = (int) (quantumNum >>> Page.PAGE_SIZE_LOG + 3);
            int offset = (int) (quantumNum & Page.PAGE_SIZE * 8 - 1) >> 3;
            Page page = putBitmapPage(pageId);
            final int bitOffs = (int) quantumNum & 7;

            myAllocatedDelta -= (long) objBitSize << DB_ALLOCATION_QUANTUM_BITS;
            myUsedSize -= (long) objBitSize << DB_ALLOCATION_QUANTUM_BITS;

            if ((aPosition & Page.PAGE_SIZE - 1) == 0 && aSize >= Page.PAGE_SIZE) {
                if (pageId == myCurrPBitmapPage && offset < myCurrPBitmapOffs) {
                    myCurrPBitmapOffs = offset;
                }
            }

            if (pageId == myCurrRBitmapPage && offset < myCurrRBitmapOffs) {
                myCurrRBitmapOffs = offset;
            }

            myBitmapPageAvailableSpace[pageId] = Integer.MAX_VALUE;

            if (objBitSize > 8 - bitOffs) {
                objBitSize -= 8 - bitOffs;
                page.myData[offset++] &= (1 << bitOffs) - 1;

                while (objBitSize + offset * 8 > Page.PAGE_SIZE * 8) {
                    memset(page, offset, 0, Page.PAGE_SIZE - offset);
                    myPool.unfix(page);
                    page = putBitmapPage(++pageId);
                    myBitmapPageAvailableSpace[pageId] = Integer.MAX_VALUE;
                    objBitSize -= (Page.PAGE_SIZE - offset) * 8;
                    offset = 0;
                }

                while ((objBitSize -= 8) > 0) {
                    page.myData[offset++] = (byte) 0;
                }

                page.myData[offset] &= (byte) ~((1 << objBitSize + 8) - 1);
            } else {
                page.myData[offset] &= (byte) ~((1 << objBitSize) - 1 << bitOffs);
            }

            myPool.unfix(page);
        }
    }

    void freeId(final int aOid) {
        synchronized (myObjectCache) {
            setPosition(aOid, (long) myHeader.myRoot[1 - myCurrentIndex].myFreeList << DB_FLAGS_BITS |
                    DB_FREE_HANDLE_FLAG);
            myHeader.myRoot[1 - myCurrentIndex].myFreeList = aOid;
        }
    }

    final void freePage(final int aOid) {
        final long position = getPosition(aOid);

        Assert.that((position & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) == DB_PAGE_OBJECT_FLAG);

        if ((position & DB_MODIFIED_FLAG) != 0) {
            free(position & ~DB_FLAGS_MASK, Page.PAGE_SIZE);
        } else {
            cloneBitmap(position & ~DB_FLAGS_MASK, Page.PAGE_SIZE);
        }

        freeId(aOid);
    }

    final byte[] get(final int aOid) {
        final long position = getPosition(aOid);

        if ((position & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
            throw new StorageError(StorageError.INVALID_OID);
        }

        return myPool.get(position & ~DB_FLAGS_MASK);
    }

    final Page getBitmapPage(final int aIndex) {
        return getPage(getBitmapPageId(aIndex));
    }

    final int getBitmapPageId(final int aIndex) {
        return aIndex < DB_BITMAP_PAGES ? DB_BITMAP_ID + aIndex : myHeader.myRoot[1 - myCurrentIndex].myBitmapExtent +
                aIndex - myBitmapExtentBase;
    }

    final long getBitmapUsedSpace(final int aFromIndex, final int aToIndex) {
        int from = aFromIndex;
        long allocated = 0;

        while (from < aToIndex) {
            final Page page = getGCPage(from);

            for (int index = 0; index < Page.PAGE_SIZE; index++) {
                int mask = page.myData[index] & 0xFF;

                while (mask != 0) {
                    if ((mask & 1) != 0) {
                        allocated += DB_ALLOCATION_QUANTUM;
                    }

                    mask >>= 1;
                }
            }

            myPool.unfix(page);
            from += 1;
        }

        return allocated;
    }

    final ClassDescriptor getClassDescriptor(final Class aClass) {
        ClassDescriptor classDescriptor = findClassDescriptor(aClass);

        if (classDescriptor == null) {
            classDescriptor = new ClassDescriptor(this, aClass);
            registerClassDescriptor(classDescriptor);
        }

        return classDescriptor;
    }

    final Page getGCPage(final int aOid) {
        return myPool.getPage(getGCPos(aOid) & ~DB_FLAGS_MASK);
    }

    final long getGCPos(final int aOid) {
        final Page page = myPool.getPage(myHeader.myRoot[myCurrentIndex].myIndex +
                ((long) (aOid >>> DB_HANDLES_PER_PAGE_BITS) << Page.PAGE_SIZE_LOG));
        final long position = Bytes.unpack8(page.myData, (aOid & DB_HANDLES_PER_PAGE - 1) << 3);

        myPool.unfix(page);
        return position;
    }

    final Page getPage(final int aOid) {
        final long position = getPosition(aOid);

        if ((position & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != DB_PAGE_OBJECT_FLAG) {
            throw new StorageError(StorageError.DELETED_OBJECT);
        }

        return myPool.getPage(position & ~DB_FLAGS_MASK);
    }

    final long getPosition(final int aOid) {
        synchronized (myObjectCache) {
            if (aOid == 0 || aOid >= myCurrentIndexSize) {
                throw new StorageError(StorageError.INVALID_OID);
            }

            if (myMulticlientSupport && !isInsideThreadTransaction()) {
                throw new StorageError(StorageError.NOT_IN_TRANSACTION);
            }

            final Page page = myPool.getPage(myHeader.myRoot[1 - myCurrentIndex].myIndex +
                    ((long) (aOid >>> DB_HANDLES_PER_PAGE_BITS) << Page.PAGE_SIZE_LOG));
            final long position = Bytes.unpack8(page.myData, (aOid & DB_HANDLES_PER_PAGE - 1) << 3);

            myPool.unfix(page);

            return position;
        }
    }

    boolean isDeleted(final Object aObj) {
        return aObj instanceof IPersistent ? ((IPersistent) aObj).isDeleted() : aObj == null ? false : (myObjMap
                .getState(aObj) & Persistent.DELETED) != 0;
    }

    boolean isLoaded(final Object aObj) {
        if (aObj instanceof IPersistent) {
            final IPersistent persistentObj = (IPersistent) aObj;
            return !persistentObj.isRaw() && persistentObj.isPersistent();
        } else {
            synchronized (myObjMap) {
                final ObjectMap.Entry entry = myObjMap.get(aObj);
                return entry != null && (entry.myState & Persistent.RAW) == 0 && entry.myOID != 0;
            }
        }
    }

    boolean isModified(final Object aObj) {
        return aObj instanceof IPersistent ? ((IPersistent) aObj).isModified() : aObj == null ? false : (myObjMap
                .getState(aObj) & Persistent.DIRTY) != 0;
    }

    boolean isPersistent(final Object aObj) {
        return getOid(aObj) != 0;
    }

    boolean isRaw(final Object aObj) {
        return aObj instanceof IPersistent ? ((IPersistent) aObj).isRaw() : aObj == null ? false : (myObjMap.getState(
                aObj) & Persistent.RAW) != 0;
    }

    final Object loadStub(final int aOID, final Object aObject, final Class aClass) {
        final long position = getPosition(aOID);

        Object obj = aObject;

        if ((position & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
            throw new StorageError(StorageError.DELETED_OBJECT);
        }

        final byte[] body = myPool.get(position & ~DB_FLAGS_MASK);
        final ClassDescriptor classDescriptor;
        final int typeOid = ObjectHeader.getType(body, 0);

        if (typeOid == 0) {
            classDescriptor = findClassDescriptor(aClass);
        } else {
            classDescriptor = findClassDescriptor(typeOid);
        }

        // synchronized (objectCache)
        {
            if (obj == null) {
                obj = classDescriptor.isCustomSerializable ? mySerializer.create(classDescriptor.myClass)
                        : classDescriptor.newInstance();
                myObjectCache.put(aOID, obj);
            }

            assignOid(obj, aOID, false);

            try {
                if (obj instanceof SelfSerializable) {
                    ((SelfSerializable) obj).unpack(new ByteArrayObjectInputStream(body, ObjectHeader.SIZE_OF, obj,
                            recursiveLoading(obj), false));
                } else if (classDescriptor.isCustomSerializable) {
                    mySerializer.unpack(obj, new ByteArrayObjectInputStream(body, ObjectHeader.SIZE_OF, obj,
                            recursiveLoading(obj), false));
                } else {
                    unpackObject(obj, classDescriptor, recursiveLoading(obj), body, ObjectHeader.SIZE_OF, obj);
                }
            } catch (final Exception details) {
                throw new StorageError(StorageError.ACCESS_VIOLATION, details);
            }
        }

        if (obj instanceof ILoadable) {
            ((IPersistent) obj).onLoad();
        }

        if (myListener != null) {
            myListener.onObjectLoad(obj);
        }

        return obj;
    }

    final synchronized Object lookupObject(final int aOID, final Class aClass) {
        Object obj = myObjectCache.get(aOID);

        if (obj == null || isRaw(obj)) {
            obj = loadStub(aOID, obj, aClass);
        }

        return obj;
    }

    final int markObject(final byte[] aObject, final int aOffset, final ClassDescriptor aDescriptor) {
        final ClassDescriptor.FieldDescriptor[] all = aDescriptor.myFields;

        int offset = aOffset;

        for (int index = 0, n = all.length; index < n; index++) {
            final ClassDescriptor.FieldDescriptor fd = all[index];

            switch (fd.myType) {
                case ClassDescriptor.TP_BOOLEAN:
                case ClassDescriptor.TP_BYTE:
                    offset += 1;
                    continue;
                case ClassDescriptor.TP_CHAR:
                case ClassDescriptor.TP_SHORT:
                    offset += 2;
                    continue;
                case ClassDescriptor.TP_INT:
                case ClassDescriptor.TP_ENUM:
                case ClassDescriptor.TP_FLOAT:
                    offset += 4;
                    continue;
                case ClassDescriptor.TP_LONG:
                case ClassDescriptor.TP_DOUBLE:
                case ClassDescriptor.TP_DATE:
                    offset += 8;
                    continue;
                case ClassDescriptor.TP_STRING:
                case ClassDescriptor.TP_CLASS:
                    offset = Bytes.skipString(aObject, offset);
                    continue;
                case ClassDescriptor.TP_OBJECT:
                    offset = markObjectReference(aObject, offset);
                    continue;
                case ClassDescriptor.TP_VALUE:
                    offset = markObject(aObject, offset, fd.myClassDescriptor);
                    continue;
                case ClassDescriptor.TP_RAW: {
                    final int len = Bytes.unpack4(aObject, offset);

                    offset += 4;

                    if (len > 0) {
                        offset += len;
                    } else if (len == -2 - ClassDescriptor.TP_OBJECT) {
                        markOid(Bytes.unpack4(aObject, offset));
                        offset += 4;
                    } else if (len < -1) {
                        offset += ClassDescriptor.SIZE_OF[-2 - len];
                    }

                    continue;
                }
                case ClassDescriptor.TP_CUSTOM:
                    try {
                        final ByteArrayObjectInputStream inStream = new ByteArrayObjectInputStream(aObject, offset,
                                null, false, true);

                        mySerializer.unpack(inStream);
                        offset = inStream.getPosition();
                    } catch (final IOException details) {
                        throw new StorageError(StorageError.ACCESS_VIOLATION, details);
                    }
                    continue;
                case ClassDescriptor.TP_ARRAY_OF_BYTES:
                case ClassDescriptor.TP_ARRAY_OF_BOOLEANS: {
                    final int length = Bytes.unpack4(aObject, offset);

                    offset += 4;

                    if (length > 0) {
                        offset += length;
                    } else if (length < -1) {
                        offset += ClassDescriptor.SIZE_OF[-2 - length];
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_SHORTS:
                case ClassDescriptor.TP_ARRAY_OF_CHARS: {
                    final int length = Bytes.unpack4(aObject, offset);

                    offset += 4;

                    if (length > 0) {
                        offset += length * 2;
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_INTS:
                case ClassDescriptor.TP_ARRAY_OF_ENUMS:
                case ClassDescriptor.TO_ARRAY_OF_FLOATS: {
                    final int length = Bytes.unpack4(aObject, offset);

                    offset += 4;

                    if (length > 0) {
                        offset += length * 4;
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_LONGS:
                case ClassDescriptor.TP_ARRAY_OF_DOUBLES:
                case ClassDescriptor.TP_ARRAY_OF_DATES: {
                    final int length = Bytes.unpack4(aObject, offset);

                    offset += 4;

                    if (length > 0) {
                        offset += length * 8;
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_STRINGS: {
                    int length = Bytes.unpack4(aObject, offset);

                    offset += 4;

                    while (--length >= 0) {
                        offset = Bytes.skipString(aObject, offset);
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_OBJECTS: {
                    int length = Bytes.unpack4(aObject, offset);

                    offset += 4;

                    while (--length >= 0) {
                        offset = markObjectReference(aObject, offset);
                    }

                    continue;
                }
                case ClassDescriptor.TP_LINK: {
                    int length = Bytes.unpack4(aObject, offset);

                    offset += 4;

                    while (--length >= 0) {
                        markOid(Bytes.unpack4(aObject, offset));
                        offset += 4;
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_VALUES: {
                    int length = Bytes.unpack4(aObject, offset);

                    offset += 4;

                    final ClassDescriptor valueDesc = fd.myClassDescriptor;

                    while (--length >= 0) {
                        offset = markObject(aObject, offset, valueDesc);
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_RAWS: {
                    int length = Bytes.unpack4(aObject, offset);

                    offset += 8;

                    while (--length >= 0) {
                        offset = markObjectReference(aObject, offset);
                    }

                    continue;
                }
                default:
                    break;
            }
        }

        return offset;
    }

    final int markObjectReference(final byte[] aObject, final int aOffset) {
        final int oid = Bytes.unpack4(aObject, aOffset);

        int offset = aOffset;

        offset += 4;

        if (oid < 0) {
            final int tid = -1 - oid;
            final int length;

            switch (tid) {
                case ClassDescriptor.TP_STRING:
                case ClassDescriptor.TP_CLASS:
                    offset = Bytes.skipString(aObject, offset);
                    break;
                case ClassDescriptor.TP_ARRAY_OF_BYTES:
                    length = Bytes.unpack4(aObject, offset);
                    offset += length + 4;
                    break;
                case ClassDescriptor.TP_ARRAY_OF_OBJECTS:
                    length = Bytes.unpack4(aObject, offset);
                    offset += 4;

                    for (int i = 0; i < length; i++) {
                        offset = markObjectReference(aObject, offset);
                    }
                    break;
                case ClassDescriptor.TP_ARRAY_OF_RAWS:
                    length = Bytes.unpack4(aObject, offset);
                    offset += 8;

                    for (int i = 0; i < length; i++) {
                        offset = markObjectReference(aObject, offset);
                    }
                    break;
                case ClassDescriptor.TP_CUSTOM:
                    try {
                        final ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(aObject, offset, null,
                                false, true);
                        mySerializer.unpack(in);
                        offset = in.getPosition();
                        break;
                    } catch (final IOException details) {
                        throw new StorageError(StorageError.ACCESS_VIOLATION, details);
                    }
                default:
                    if (tid >= ClassDescriptor.TP_VALUE_TYPE_BIAS) {
                        final int typeOid = -ClassDescriptor.TP_VALUE_TYPE_BIAS - oid;
                        final ClassDescriptor desc = findClassDescriptor(typeOid);

                        if (desc.isCollection) {
                            length = Bytes.unpack4(aObject, offset);
                            offset += 4;

                            for (int i = 0; i < length; i++) {
                                offset = markObjectReference(aObject, offset);
                            }
                        } else if (desc.isMap) {
                            length = Bytes.unpack4(aObject, offset);
                            offset += 4;

                            for (int i = 0; i < length; i++) {
                                offset = markObjectReference(aObject, offset);
                                offset = markObjectReference(aObject, offset);
                            }
                        } else {
                            offset = markObject(aObject, offset, desc);
                        }
                    } else {
                        offset += ClassDescriptor.SIZE_OF[tid];
                    }
            }
        } else {
            markOid(oid);
        }

        return offset;
    }

    final void markOid(final int aOID) {
        if (aOID != 0) {
            final long pos = getGCPos(aOID);

            if ((pos & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
                throw new StorageError(StorageError.INVALID_OID);
            }

            if (pos < myHeader.myRoot[myCurrentIndex].mySize) {
                // object was not allocated by custom allocator
                final int bit = (int) (pos >>> DB_ALLOCATION_QUANTUM_BITS);

                if ((myBlackBitmap[bit >>> 5] & 1 << (bit & 31)) == 0) {
                    myGreyBitmap[bit >>> 5] |= 1 << (bit & 31);
                }
            }
        }
    }

    final byte[] packObject(final Object aObj, final boolean aFinalizedObj) {
        final ByteBuffer buffer = new ByteBuffer(this, aObj, aFinalizedObj);
        int offset = ObjectHeader.SIZE_OF;

        buffer.extend(offset);

        final ClassDescriptor classDescriptor = getClassDescriptor(aObj.getClass());

        try {
            if (aObj instanceof SelfSerializable) {
                ((SelfSerializable) aObj).pack(buffer.getOutputStream());
                offset = buffer.myUsed;
            } else if (classDescriptor.isCustomSerializable) {
                mySerializer.pack(aObj, buffer.getOutputStream());
                offset = buffer.myUsed;
            } else {
                offset = packObject(aObj, classDescriptor, offset, buffer);
            }
        } catch (final Exception details) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, details);
        }

        ObjectHeader.setSize(buffer.myByteArray, 0, offset);
        ObjectHeader.setType(buffer.myByteArray, 0, classDescriptor.getOid());

        return buffer.myByteArray;
    }

    final int packObject(final Object aObject, final ClassDescriptor aDescriptor, final int aOffset,
            final ByteBuffer aByteBuffer) throws Exception {
        final ClassDescriptor.FieldDescriptor[] fields = aDescriptor.myFields;

        int offset = aOffset;

        for (int index = 0, fieldCount = fields.length; index < fieldCount; index++) {
            final ClassDescriptor.FieldDescriptor fieldDescriptor = fields[index];
            final Field field = fieldDescriptor.myField;

            switch (fieldDescriptor.myType) {
                case ClassDescriptor.TP_BYTE:
                    aByteBuffer.extend(offset + 1);
                    aByteBuffer.myByteArray[offset++] = field.getByte(aObject);
                    continue;
                case ClassDescriptor.TP_BOOLEAN:
                    aByteBuffer.extend(offset + 1);
                    aByteBuffer.myByteArray[offset++] = (byte) (field.getBoolean(aObject) ? 1 : 0);
                    continue;
                case ClassDescriptor.TP_SHORT:
                    aByteBuffer.extend(offset + 2);
                    Bytes.pack2(aByteBuffer.myByteArray, offset, field.getShort(aObject));
                    offset += 2;
                    continue;
                case ClassDescriptor.TP_CHAR:
                    aByteBuffer.extend(offset + 2);
                    Bytes.pack2(aByteBuffer.myByteArray, offset, (short) field.getChar(aObject));
                    offset += 2;
                    continue;
                case ClassDescriptor.TP_INT:
                    aByteBuffer.extend(offset + 4);
                    Bytes.pack4(aByteBuffer.myByteArray, offset, field.getInt(aObject));
                    offset += 4;
                    continue;
                case ClassDescriptor.TP_LONG:
                    aByteBuffer.extend(offset + 8);
                    Bytes.pack8(aByteBuffer.myByteArray, offset, field.getLong(aObject));
                    offset += 8;
                    continue;
                case ClassDescriptor.TP_FLOAT:
                    aByteBuffer.extend(offset + 4);
                    Bytes.packF4(aByteBuffer.myByteArray, offset, field.getFloat(aObject));
                    offset += 4;
                    continue;
                case ClassDescriptor.TP_DOUBLE:
                    aByteBuffer.extend(offset + 8);
                    Bytes.packF8(aByteBuffer.myByteArray, offset, field.getDouble(aObject));
                    offset += 8;
                    continue;
                case ClassDescriptor.TP_ENUM: {
                    final Enum enumeration = (Enum) field.get(aObject);

                    aByteBuffer.extend(offset + 4);

                    if (enumeration == null) {
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                    } else {
                        Bytes.pack4(aByteBuffer.myByteArray, offset, enumeration.ordinal());
                    }

                    offset += 4;

                    continue;
                }
                case ClassDescriptor.TP_DATE: {
                    aByteBuffer.extend(offset + 8);

                    final Date date = (Date) field.get(aObject);
                    final long msec = date == null ? -1 : date.getTime();

                    Bytes.pack8(aByteBuffer.myByteArray, offset, msec);
                    offset += 8;

                    continue;
                }
                case ClassDescriptor.TP_STRING:
                    offset = aByteBuffer.packString(offset, (String) field.get(aObject));
                    continue;
                case ClassDescriptor.TP_CLASS:
                    offset = aByteBuffer.packString(offset, ClassDescriptor.getClassName((Class) field.get(aObject)));
                    continue;
                case ClassDescriptor.TP_OBJECT:
                    offset = swizzle(aByteBuffer, offset, field.get(aObject));
                    continue;
                case ClassDescriptor.TP_VALUE: {
                    final Object value = field.get(aObject);

                    if (value == null) {
                        throw new StorageError(StorageError.NULL_VALUE, fieldDescriptor.myFieldName);
                    } else if (value instanceof IPersistent) {
                        throw new StorageError(StorageError.SERIALIZE_PERSISTENT);
                    }

                    offset = packObject(value, fieldDescriptor.myClassDescriptor, offset, aByteBuffer);
                    continue;
                }
                case ClassDescriptor.TP_RAW:
                    offset = packValue(field.get(aObject), offset, aByteBuffer);
                    continue;
                case ClassDescriptor.TP_CUSTOM: {
                    mySerializer.pack(field.get(aObject), aByteBuffer.getOutputStream());
                    offset = aByteBuffer.size();
                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_BYTES: {
                    final byte[] array = (byte[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4 + length);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;
                        System.arraycopy(array, 0, aByteBuffer.myByteArray, offset, length);
                        offset += length;
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_BOOLEANS: {
                    final boolean[] array = (boolean[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int len = array.length;

                        aByteBuffer.extend(offset + 4 + len);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, len);
                        offset += 4;

                        for (int j = 0; j < len; j++, offset++) {
                            aByteBuffer.myByteArray[offset] = (byte) (array[j] ? 1 : 0);
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_SHORTS: {
                    final short[] array = (short[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4 + length * 2);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        for (int j = 0; j < length; j++) {
                            Bytes.pack2(aByteBuffer.myByteArray, offset, array[j]);
                            offset += 2;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_CHARS: {
                    final char[] array = (char[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4 + length * 2);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        for (int j = 0; j < length; j++) {
                            Bytes.pack2(aByteBuffer.myByteArray, offset, (short) array[j]);
                            offset += 2;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_INTS: {
                    final int[] array = (int[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4 + length * 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        for (int j = 0; j < length; j++) {
                            Bytes.pack4(aByteBuffer.myByteArray, offset, array[j]);
                            offset += 4;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_ENUMS: {
                    final Enum[] array = (Enum[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4 + length * 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        for (int jndex = 0; jndex < length; jndex++) {
                            if (array[jndex] == null) {
                                Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                            } else {
                                Bytes.pack4(aByteBuffer.myByteArray, offset, array[jndex].ordinal());
                            }

                            offset += 4;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_LONGS: {
                    final long[] array = (long[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4 + length * 8);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        for (int jndex = 0; jndex < length; jndex++) {
                            Bytes.pack8(aByteBuffer.myByteArray, offset, array[jndex]);
                            offset += 8;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TO_ARRAY_OF_FLOATS: {
                    final float[] array = (float[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4 + length * 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        for (int jndex = 0; jndex < length; jndex++) {
                            Bytes.packF4(aByteBuffer.myByteArray, offset, array[jndex]);
                            offset += 4;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_DOUBLES: {
                    final double[] array = (double[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4 + length * 8);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        for (int jndex = 0; jndex < length; jndex++) {
                            Bytes.packF8(aByteBuffer.myByteArray, offset, array[jndex]);
                            offset += 8;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_DATES: {
                    final Date[] array = (Date[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4 + length * 8);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        for (int jndex = 0; jndex < length; jndex++) {
                            final Date date = array[jndex];
                            final long msec = date == null ? -1 : date.getTime();

                            Bytes.pack8(aByteBuffer.myByteArray, offset, msec);
                            offset += 8;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_STRINGS: {
                    final String[] array = (String[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        for (int jndex = 0; jndex < length; jndex++) {
                            offset = aByteBuffer.packString(offset, array[jndex]);
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_OBJECTS: {
                    final Object[] array = (Object[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4 + length * 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        for (int jndex = 0; jndex < length; jndex++) {
                            offset = swizzle(aByteBuffer, offset, array[jndex]);
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TP_ARRAY_OF_VALUES: {
                    final Object[] array = (Object[]) field.get(aObject);

                    if (array == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        final int length = array.length;

                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        final ClassDescriptor classDescriptor = fieldDescriptor.myClassDescriptor;

                        for (int jndex = 0; jndex < length; jndex++) {
                            final Object value = array[jndex];

                            if (value == null) {
                                throw new StorageError(StorageError.NULL_VALUE, fieldDescriptor.myFieldName);
                            }

                            offset = packObject(value, classDescriptor, offset, aByteBuffer);
                        }
                    }

                    continue;
                }
                case ClassDescriptor.TP_LINK: {
                    final LinkImpl link = (LinkImpl) field.get(aObject);

                    if (link == null) {
                        aByteBuffer.extend(offset + 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
                        offset += 4;
                    } else {
                        link.myOwner = aByteBuffer.myParent;

                        final int length = link.size();

                        aByteBuffer.extend(offset + 4 + length * 4);
                        Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                        offset += 4;

                        for (int jndex = 0; jndex < length; jndex++) {
                            Bytes.pack4(aByteBuffer.myByteArray, offset, swizzle(link.getRaw(jndex),
                                    aByteBuffer.isFinalized));
                            offset += 4;
                        }

                        if (!aByteBuffer.isFinalized) {
                            link.unpin();
                        }
                    }

                    continue;
                }
                default:
                    break;
            }
        }

        return offset;
    }

    final int packValue(final Object aValue, final int aOffset, final ByteBuffer aByteBuffer) throws Exception {
        int offset = aOffset;

        if (aValue == null) {
            aByteBuffer.extend(offset + 4);
            Bytes.pack4(aByteBuffer.myByteArray, offset, -1);
            offset += 4;
        } else if (aValue instanceof IPersistent) {
            aByteBuffer.extend(offset + 8);
            Bytes.pack4(aByteBuffer.myByteArray, offset, -2 - ClassDescriptor.TP_OBJECT);
            Bytes.pack4(aByteBuffer.myByteArray, offset + 4, swizzle(aValue, aByteBuffer.isFinalized));
            offset += 8;
        } else {
            final Class clazz = aValue.getClass();

            if (clazz == Boolean.class) {
                aByteBuffer.extend(offset + 5);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -2 - ClassDescriptor.TP_BOOLEAN);
                aByteBuffer.myByteArray[offset + 4] = (byte) (((Boolean) aValue).booleanValue() ? 1 : 0);
                offset += 5;
            } else if (clazz == Character.class) {
                aByteBuffer.extend(offset + 6);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -2 - ClassDescriptor.TP_CHAR);
                Bytes.pack2(aByteBuffer.myByteArray, offset + 4, (short) ((Character) aValue).charValue());
                offset += 6;
            } else if (clazz == Byte.class) {
                aByteBuffer.extend(offset + 5);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -2 - ClassDescriptor.TP_BYTE);
                aByteBuffer.myByteArray[offset + 4] = ((Byte) aValue).byteValue();
                offset += 5;
            } else if (clazz == Short.class) {
                aByteBuffer.extend(offset + 6);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -2 - ClassDescriptor.TP_SHORT);
                Bytes.pack2(aByteBuffer.myByteArray, offset + 4, ((Short) aValue).shortValue());
                offset += 6;
            } else if (clazz == Integer.class) {
                aByteBuffer.extend(offset + 8);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -2 - ClassDescriptor.TP_INT);
                Bytes.pack4(aByteBuffer.myByteArray, offset + 4, ((Integer) aValue).intValue());
                offset += 8;
            } else if (clazz == Long.class) {
                aByteBuffer.extend(offset + 12);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -2 - ClassDescriptor.TP_LONG);
                Bytes.pack8(aByteBuffer.myByteArray, offset + 4, ((Long) aValue).longValue());
                offset += 12;
            } else if (clazz == Float.class) {
                aByteBuffer.extend(offset + 8);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -2 - ClassDescriptor.TP_FLOAT);
                Bytes.pack4(aByteBuffer.myByteArray, offset + 4, Float.floatToIntBits(((Float) aValue).floatValue()));
                offset += 8;
            } else if (clazz == Double.class) {
                aByteBuffer.extend(offset + 12);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -2 - ClassDescriptor.TP_DOUBLE);
                Bytes.pack8(aByteBuffer.myByteArray, offset + 4, Double.doubleToLongBits(((Double) aValue)
                        .doubleValue()));
                offset += 12;
            } else if (clazz == Date.class) {
                aByteBuffer.extend(offset + 12);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -2 - ClassDescriptor.TP_DATE);
                Bytes.pack8(aByteBuffer.myByteArray, offset + 4, ((Date) aValue).getTime());
                offset += 12;
            } else {
                final ByteArrayOutputStream bout = new ByteArrayOutputStream();
                final ObjectOutputStream outStream = myLoaderMap != null && (myCompatibilityMode &
                        CLASS_LOADER_SERIALIZATION_COMPATIBILITY_MODE) == 0
                                ? (ObjectOutputStream) new AnnotatedPersistentObjectOutputStream(bout)
                                : (ObjectOutputStream) new PersistentObjectOutputStream(bout);

                outStream.writeObject(aValue);
                outStream.close();

                final byte[] array = bout.toByteArray();
                final int length = array.length;

                aByteBuffer.extend(offset + 4 + length);
                Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                offset += 4;
                System.arraycopy(array, 0, aByteBuffer.myByteArray, offset, length);
                offset += length;
            }
        }

        return offset;
    }

    final Page putBitmapPage(final int aIndex) {
        return putPage(getBitmapPageId(aIndex));
    }

    final Page putPage(final int aOid) {
        synchronized (myObjectCache) {
            long position = getPosition(aOid);

            if ((position & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != DB_PAGE_OBJECT_FLAG) {
                throw new StorageError(StorageError.DELETED_OBJECT);
            }

            if ((position & DB_MODIFIED_FLAG) == 0) {
                myDirtyPagesMap[aOid >>> DB_HANDLES_PER_PAGE_BITS + 5] |= 1 << (aOid >>> DB_HANDLES_PER_PAGE_BITS &
                        31);
                allocate(Page.PAGE_SIZE, aOid);
                cloneBitmap(position & ~DB_FLAGS_MASK, Page.PAGE_SIZE);
                position = getPosition(aOid);
            }

            myModified = true;

            return myPool.putPage(position & ~DB_FLAGS_MASK);
        }
    }

    boolean recursiveLoading(final Object aObj) {
        if (myRecursiveLoadingPolicyDefined) {
            synchronized (myRecursiveLoadingPolicy) {
                Class type = aObj.getClass();

                do {
                    final Object enabled = myRecursiveLoadingPolicy.get(type);

                    if (enabled != null) {
                        return ((Boolean) enabled).booleanValue();
                    }
                } while ((type = type.getSuperclass()) != null);
            }
        }

        return aObj instanceof IPersistent ? ((IPersistent) aObj).recursiveLoading() : true;
    }

    final void registerClassDescriptor(final ClassDescriptor aDescriptor) {
        myClassDescriptorMap.put(aDescriptor.myClass, aDescriptor);
        aDescriptor.myNextCD = myClassDescriptor;
        myClassDescriptor = aDescriptor;
        checkIfFinal(aDescriptor);
        storeObject0(aDescriptor, false);
        myHeader.myRoot[1 - myCurrentIndex].myClassDescList = aDescriptor.getOid();
        myModified = true;
    }

    void reloadScheme() {
        myClassDescriptorMap.clear();
        myCustomAllocatorMap = null;
        myCustomAllocatorList = null;
        myDefaultAllocator = new DefaultAllocator(this);

        final int descriptorListOid = myHeader.myRoot[1 - myCurrentIndex].myClassDescList;
        final ClassDescriptor metaclass = new ClassDescriptor(this, ClassDescriptor.class);
        final ClassDescriptor metafield = new ClassDescriptor(this, ClassDescriptor.FieldDescriptor.class);

        myClassDescriptorMap.put(ClassDescriptor.class, metaclass);
        myClassDescriptorMap.put(ClassDescriptor.FieldDescriptor.class, metafield);

        // TODO: Remove
        if ((myCompatibilityMode & IBM_JAVA5_COMPATIBILITY_MODE) != 0 && getDatabaseFormatVersion() == 1) {
            ClassDescriptor.FieldDescriptor tmp = metaclass.myFields[2];
            metaclass.myFields[2] = metaclass.myFields[3];
            metaclass.myFields[3] = metaclass.myFields[4];
            metaclass.myFields[4] = tmp;
            tmp = metafield.myFields[2];
            metafield.myFields[2] = metafield.myFields[3];
            metafield.myFields[3] = tmp;
        }

        if (descriptorListOid != 0) {
            ClassDescriptor classDescriptor;
            myClassDescriptor = findClassDescriptor(descriptorListOid);

            for (classDescriptor = myClassDescriptor; classDescriptor != null; classDescriptor =
                    classDescriptor.myNextCD) {
                classDescriptor.load();
            }

            for (classDescriptor = myClassDescriptor; classDescriptor != null; classDescriptor =
                    classDescriptor.myNextCD) {
                if (findClassDescriptor(classDescriptor.myClass) == classDescriptor) {
                    classDescriptor.resolve();
                }

                if (classDescriptor.myAllocator != null) {
                    if (myCustomAllocatorMap == null) {
                        myCustomAllocatorMap = new HashMap();
                        myCustomAllocatorList = new ArrayList();
                    }

                    final CustomAllocator allocator = classDescriptor.myAllocator;

                    allocator.load();

                    myCustomAllocatorMap.put(classDescriptor.myClass, allocator);
                    myCustomAllocatorList.add(allocator);

                    reserveLocation(allocator.getSegmentBase(), allocator.getSegmentSize());
                }

                checkIfFinal(classDescriptor);
            }
        } else {
            myClassDescriptor = null;
        }
    }

    final void reserveLocation(final long aPosition, final long aSize) {
        final Location location = new Location();

        location.myPosition = aPosition;
        location.mySize = aSize;
        location.myNext = myReservedChain;
        myReservedChain = location;
    }

    final void setDirty() {
        myModified = true;

        if (!myHeader.isDirty) {
            final Page page = myPool.putPage(0);

            myHeader.isDirty = true;
            myHeader.pack(page.myData);
            myPool.flush();
            myPool.unfix(page);
        }
    }

    final void setPosition(final int aOid, final long aPosition) {
        synchronized (myObjectCache) {
            myDirtyPagesMap[aOid >>> DB_HANDLES_PER_PAGE_BITS + 5] |= 1 << (aOid >>> DB_HANDLES_PER_PAGE_BITS & 31);

            final Page page = myPool.putPage(myHeader.myRoot[1 - myCurrentIndex].myIndex +
                    ((long) (aOid >>> DB_HANDLES_PER_PAGE_BITS) << Page.PAGE_SIZE_LOG));

            Bytes.pack8(page.myData, (aOid & DB_HANDLES_PER_PAGE - 1) << 3, aPosition);
            myPool.unfix(page);
        }
    }

    final int skipObjectReference(final byte[] aObject, final int aOffset) throws Exception {
        final int oid = Bytes.unpack4(aObject, aOffset);
        final int length;

        int offset = aOffset;

        offset += 4;

        if (oid < 0) {
            final int typeId = -1 - oid;

            switch (typeId) {
                case ClassDescriptor.TP_STRING:
                case ClassDescriptor.TP_CLASS:
                    offset = Bytes.skipString(aObject, offset);
                    break;
                case ClassDescriptor.TP_ARRAY_OF_BYTES:
                    length = Bytes.unpack4(aObject, offset);
                    offset += length + 4;
                    break;
                case ClassDescriptor.TP_ARRAY_OF_OBJECTS:
                    length = Bytes.unpack4(aObject, offset);
                    offset += 4;

                    for (int index = 0; index < length; index++) {
                        offset = skipObjectReference(aObject, offset);
                    }

                    break;
                case ClassDescriptor.TP_ARRAY_OF_RAWS:
                    length = Bytes.unpack4(aObject, offset);
                    offset += 8;

                    for (int index = 0; index < length; index++) {
                        offset = skipObjectReference(aObject, offset);
                    }

                    break;
                default:
                    if (typeId >= ClassDescriptor.TP_VALUE_TYPE_BIAS) {
                        final int typeOid = -ClassDescriptor.TP_VALUE_TYPE_BIAS - oid;
                        final ClassDescriptor classDescriptor = findClassDescriptor(typeOid);

                        if (classDescriptor.isCollection) {
                            length = Bytes.unpack4(aObject, offset);
                            offset += 4;

                            for (int index = 0; index < length; index++) {
                                offset = skipObjectReference(aObject, offset);
                            }
                        } else if (classDescriptor.isMap) {
                            length = Bytes.unpack4(aObject, offset);
                            offset += 4;

                            for (int index = 0; index < length; index++) {
                                offset = skipObjectReference(aObject, offset);
                                offset = skipObjectReference(aObject, offset);
                            }
                        } else {
                            offset = unpackObject(null, findClassDescriptor(typeOid), false, aObject, offset, null);
                        }
                    } else {
                        offset += ClassDescriptor.SIZE_OF[typeId];
                    }
            }
        }

        return offset;
    }

    final int swizzle(final ByteBuffer aByteBuffer, final int aOffset, final Object aObject) throws Exception {
        int offset = aOffset;

        if (aObject instanceof IPersistent || aObject == null) {
            offset = aByteBuffer.packI4(offset, swizzle(aObject, aByteBuffer.isFinalized));
        } else {
            final Class type = aObject.getClass();

            if (type == Boolean.class) {
                aByteBuffer.extend(offset + 5);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_BOOLEAN);
                aByteBuffer.myByteArray[offset + 4] = (byte) (((Boolean) aObject).booleanValue() ? 1 : 0);
                offset += 5;
            } else if (type == Character.class) {
                aByteBuffer.extend(offset + 6);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_CHAR);
                Bytes.pack2(aByteBuffer.myByteArray, offset + 4, (short) ((Character) aObject).charValue());
                offset += 6;
            } else if (type == Byte.class) {
                aByteBuffer.extend(offset + 5);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_BYTE);
                aByteBuffer.myByteArray[offset + 4] = ((Byte) aObject).byteValue();
                offset += 5;
            } else if (type == Short.class) {
                aByteBuffer.extend(offset + 6);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_SHORT);
                Bytes.pack2(aByteBuffer.myByteArray, offset + 4, ((Short) aObject).shortValue());
                offset += 6;
            } else if (type == Integer.class) {
                aByteBuffer.extend(offset + 8);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_INT);
                Bytes.pack4(aByteBuffer.myByteArray, offset + 4, ((Integer) aObject).intValue());
                offset += 8;
            } else if (type == Long.class) {
                aByteBuffer.extend(offset + 12);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_LONG);
                Bytes.pack8(aByteBuffer.myByteArray, offset + 4, ((Long) aObject).longValue());
                offset += 12;
            } else if (type == Float.class) {
                aByteBuffer.extend(offset + 8);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_FLOAT);
                Bytes.packF4(aByteBuffer.myByteArray, offset + 4, ((Float) aObject).floatValue());
                offset += 8;
            } else if (type == Double.class) {
                aByteBuffer.extend(offset + 12);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_DOUBLE);
                Bytes.packF8(aByteBuffer.myByteArray, offset + 4, ((Double) aObject).doubleValue());
                offset += 12;
            } else if (type == Date.class) {
                aByteBuffer.extend(offset + 12);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_DATE);
                Bytes.pack8(aByteBuffer.myByteArray, offset + 4, ((Date) aObject).getTime());
                offset += 12;
            } else if (type == String.class) {
                offset = aByteBuffer.packI4(offset, -1 - ClassDescriptor.TP_STRING);
                offset = aByteBuffer.packString(offset, (String) aObject);
            } else if (type == Class.class) {
                offset = aByteBuffer.packI4(offset, -1 - ClassDescriptor.TP_CLASS);
                offset = aByteBuffer.packString(offset, ClassDescriptor.getClassName((Class) aObject));
            } else if (aObject instanceof LinkImpl) {
                final LinkImpl link = (LinkImpl) aObject;
                link.myOwner = aByteBuffer.myParent;
                final int length = link.size();
                aByteBuffer.extend(offset + 8 + length * 4);
                Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_LINK);
                offset += 4;
                Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                offset += 4;

                for (int index = 0; index < length; index++) {
                    Bytes.pack4(aByteBuffer.myByteArray, offset, swizzle(link.getRaw(index),
                            aByteBuffer.isFinalized));
                    offset += 4;
                }
            } else if (aObject instanceof Collection && (!mySerializeSystemCollections || type.getName().startsWith(
                    UTIL_PACKAGE))) {
                final ClassDescriptor classDescriptor = getClassDescriptor(aObject.getClass());
                offset = aByteBuffer.packI4(offset, -ClassDescriptor.TP_VALUE_TYPE_BIAS - classDescriptor.getOid());
                final Collection collection = (Collection) aObject;
                offset = aByteBuffer.packI4(offset, collection.size());

                for (final Object elem : collection) {
                    offset = swizzle(aByteBuffer, offset, elem);
                }
            } else if (aObject instanceof Map && (!mySerializeSystemCollections || type.getName().startsWith(
                    UTIL_PACKAGE))) {
                final ClassDescriptor valueDesc = getClassDescriptor(aObject.getClass());
                offset = aByteBuffer.packI4(offset, -ClassDescriptor.TP_VALUE_TYPE_BIAS - valueDesc.getOid());
                final Map map = (Map) aObject;
                offset = aByteBuffer.packI4(offset, map.size());

                for (final Object entry : map.entrySet()) {
                    final Map.Entry e = (Map.Entry) entry;
                    offset = swizzle(aByteBuffer, offset, e.getKey());
                    offset = swizzle(aByteBuffer, offset, e.getValue());
                }
            } else if (aObject instanceof IValue) {
                final ClassDescriptor valueDesc = getClassDescriptor(aObject.getClass());
                offset = aByteBuffer.packI4(offset, -ClassDescriptor.TP_VALUE_TYPE_BIAS - valueDesc.getOid());
                offset = packObject(aObject, valueDesc, offset, aByteBuffer);
            } else if (type.isArray()) {
                final Class componentType = type.getComponentType();

                if (componentType == byte.class) {
                    final byte[] array = (byte[]) aObject;
                    final int length = array.length;

                    aByteBuffer.extend(offset + length + 8);
                    Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_ARRAY_OF_BYTES);
                    Bytes.pack4(aByteBuffer.myByteArray, offset + 4, length);
                    System.arraycopy(array, 0, aByteBuffer.myByteArray, offset + 8, length);
                    offset += 8 + length;
                } else if (componentType == boolean.class) {
                    final boolean[] array = (boolean[]) aObject;
                    final int length = array.length;

                    aByteBuffer.extend(offset + length + 8);
                    Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_ARRAY_OF_BOOLEANS);
                    offset += 4;
                    Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                    offset += 4;

                    for (int index = 0; index < length; index++) {
                        aByteBuffer.myByteArray[offset++] = (byte) (array[index] ? 1 : 0);
                    }
                } else if (componentType == char.class) {
                    final char[] array = (char[]) aObject;
                    final int length = array.length;

                    aByteBuffer.extend(offset + length * 2 + 8);
                    Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_ARRAY_OF_CHARS);
                    offset += 4;
                    Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                    offset += 4;

                    for (int index = 0; index < length; index++) {
                        Bytes.pack2(aByteBuffer.myByteArray, offset, (short) array[index]);
                        offset += 2;
                    }
                } else if (componentType == short.class) {
                    final short[] array = (short[]) aObject;
                    final int length = array.length;

                    aByteBuffer.extend(offset + length * 2 + 8);
                    Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_ARRAY_OF_SHORTS);
                    offset += 4;
                    Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                    offset += 4;

                    for (int index = 0; index < length; index++) {
                        Bytes.pack2(aByteBuffer.myByteArray, offset, array[index]);
                        offset += 2;
                    }
                } else if (componentType == int.class) {
                    final int[] array = (int[]) aObject;
                    final int length = array.length;

                    aByteBuffer.extend(offset + length * 4 + 8);
                    Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_ARRAY_OF_INTS);
                    offset += 4;
                    Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                    offset += 4;

                    for (int index = 0; index < length; index++) {
                        Bytes.pack4(aByteBuffer.myByteArray, offset, array[index]);
                        offset += 4;
                    }
                } else if (componentType == long.class) {
                    final long[] array = (long[]) aObject;
                    final int length = array.length;

                    aByteBuffer.extend(offset + length * 8 + 8);
                    Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_ARRAY_OF_LONGS);
                    offset += 4;
                    Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                    offset += 4;

                    for (int index = 0; index < length; index++) {
                        Bytes.pack8(aByteBuffer.myByteArray, offset, array[index]);
                        offset += 8;
                    }
                } else if (componentType == float.class) {
                    final float[] array = (float[]) aObject;
                    final int length = array.length;

                    aByteBuffer.extend(offset + length * 4 + 8);
                    Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TO_ARRAY_OF_FLOATS);
                    offset += 4;
                    Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                    offset += 4;

                    for (int index = 0; index < length; index++) {
                        Bytes.packF4(aByteBuffer.myByteArray, offset, array[index]);
                        offset += 4;
                    }
                } else if (componentType == double.class) {
                    final double[] array = (double[]) aObject;
                    final int length = array.length;

                    aByteBuffer.extend(offset + length * 8 + 8);
                    Bytes.pack4(aByteBuffer.myByteArray, offset, -1 - ClassDescriptor.TP_ARRAY_OF_LONGS);
                    offset += 4;
                    Bytes.pack4(aByteBuffer.myByteArray, offset, length);
                    offset += 4;

                    for (int index = 0; index < length; index++) {
                        Bytes.packF8(aByteBuffer.myByteArray, offset, array[index]);
                        offset += 8;
                    }
                } else if (componentType == Object.class) {
                    final Object[] array = (Object[]) aObject;
                    final int length = array.length;

                    offset = aByteBuffer.packI4(offset, -1 - ClassDescriptor.TP_ARRAY_OF_OBJECTS);
                    offset = aByteBuffer.packI4(offset, length);

                    for (int index = 0; index < length; index++) {
                        offset = swizzle(aByteBuffer, offset, array[index]);
                    }
                } else {
                    offset = aByteBuffer.packI4(offset, -1 - ClassDescriptor.TP_ARRAY_OF_RAWS);
                    final int length = Array.getLength(aObject);
                    offset = aByteBuffer.packI4(offset, length);

                    if (componentType.equals(Comparable.class)) {
                        offset = aByteBuffer.packI4(offset, -1);
                    } else {
                        final ClassDescriptor classDescriptor = getClassDescriptor(componentType);

                        offset = aByteBuffer.packI4(offset, classDescriptor.getOid());
                    }

                    for (int index = 0; index < length; index++) {
                        offset = swizzle(aByteBuffer, offset, Array.get(aObject, index));
                    }
                }
            } else if (mySerializer != null && mySerializer.isEmbedded(aObject)) {
                aByteBuffer.packI4(offset, -1 - ClassDescriptor.TP_CUSTOM);
                mySerializer.pack(aObject, aByteBuffer.getOutputStream());
                offset = aByteBuffer.myUsed;
            } else {
                offset = aByteBuffer.packI4(offset, swizzle(aObject, aByteBuffer.isFinalized));
            }
        }

        return offset;
    }

    void unassignOid(final Object aObj) {
        if (aObj instanceof IPersistent) {
            ((IPersistent) aObj).unassignOid();
        } else {
            myObjMap.remove(aObj);
        }
    }

    final int unpackObject(final Object aObject, final ClassDescriptor aDescriptor, final boolean aRecursiveLoading,
            final byte[] aBody, final int aOffset, final Object aParent) throws Exception {
        final ClassDescriptor.FieldDescriptor[] all = aDescriptor.myFields;
        final ReflectionProvider provider = ClassDescriptor.getReflectionProvider();
        int offset = aOffset;
        int len;

        for (int i = 0, n = all.length; i < n; i++) {
            final ClassDescriptor.FieldDescriptor fd = all[i];
            final Field f = fd.myField;

            if (f == null || aObject == null) {
                switch (fd.myType) {
                    case ClassDescriptor.TP_BOOLEAN:
                    case ClassDescriptor.TP_BYTE:
                        offset += 1;
                        continue;
                    case ClassDescriptor.TP_CHAR:
                    case ClassDescriptor.TP_SHORT:
                        offset += 2;
                        continue;
                    case ClassDescriptor.TP_INT:
                    case ClassDescriptor.TP_FLOAT:
                    case ClassDescriptor.TP_ENUM:
                        offset += 4;
                        continue;
                    case ClassDescriptor.TP_OBJECT:
                        offset = skipObjectReference(aBody, offset);
                        continue;
                    case ClassDescriptor.TP_LONG:
                    case ClassDescriptor.TP_DOUBLE:
                    case ClassDescriptor.TP_DATE:
                        offset += 8;
                        continue;
                    case ClassDescriptor.TP_STRING:
                    case ClassDescriptor.TP_CLASS:
                        offset = Bytes.skipString(aBody, offset);
                        continue;
                    case ClassDescriptor.TP_VALUE:
                        offset = unpackObject(null, fd.myClassDescriptor, aRecursiveLoading, aBody, offset, aParent);
                        continue;
                    case ClassDescriptor.TP_RAW:
                    case ClassDescriptor.TP_ARRAY_OF_BYTES:
                    case ClassDescriptor.TP_ARRAY_OF_BOOLEANS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len > 0) {
                            offset += len;
                        } else if (len < -1) {
                            offset += ClassDescriptor.SIZE_OF[-2 - len];
                        }

                        continue;
                    case ClassDescriptor.TP_CUSTOM: {
                        final ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(aBody, offset, aParent,
                                aRecursiveLoading, false);
                        mySerializer.unpack(in);
                        offset = in.getPosition();
                        continue;
                    }
                    case ClassDescriptor.TP_ARRAY_OF_SHORTS:
                    case ClassDescriptor.TP_ARRAY_OF_CHARS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len > 0) {
                            offset += len * 2;
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_OBJECTS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        for (int j = 0; j < len; j++) {
                            offset = skipObjectReference(aBody, offset);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_INTS:
                    case ClassDescriptor.TP_ARRAY_OF_ENUMS:
                    case ClassDescriptor.TO_ARRAY_OF_FLOATS:
                    case ClassDescriptor.TP_LINK:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len > 0) {
                            offset += len * 4;
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_LONGS:
                    case ClassDescriptor.TP_ARRAY_OF_DOUBLES:
                    case ClassDescriptor.TP_ARRAY_OF_DATES:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len > 0) {
                            offset += len * 8;
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_STRINGS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len > 0) {
                            for (int j = 0; j < len; j++) {
                                offset = Bytes.skipString(aBody, offset);
                            }
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_VALUES:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len > 0) {
                            final ClassDescriptor valueDesc = fd.myClassDescriptor;

                            for (int j = 0; j < len; j++) {
                                offset = unpackObject(null, valueDesc, aRecursiveLoading, aBody, offset, aParent);
                            }
                        }

                        continue;
                    default:
                        break;
                }
            } else if (offset < aBody.length) {
                switch (fd.myType) {
                    case ClassDescriptor.TP_BOOLEAN:
                        provider.setBoolean(f, aObject, aBody[offset++] != 0);
                        continue;
                    case ClassDescriptor.TP_BYTE:
                        provider.setByte(f, aObject, aBody[offset++]);
                        continue;
                    case ClassDescriptor.TP_CHAR:
                        provider.setChar(f, aObject, (char) Bytes.unpack2(aBody, offset));
                        offset += 2;
                        continue;
                    case ClassDescriptor.TP_SHORT:
                        provider.setShort(f, aObject, Bytes.unpack2(aBody, offset));
                        offset += 2;
                        continue;
                    case ClassDescriptor.TP_INT:
                        provider.setInt(f, aObject, Bytes.unpack4(aBody, offset));
                        offset += 4;
                        continue;
                    case ClassDescriptor.TP_LONG:
                        provider.setLong(f, aObject, Bytes.unpack8(aBody, offset));
                        offset += 8;
                        continue;
                    case ClassDescriptor.TP_FLOAT:
                        provider.setFloat(f, aObject, Bytes.unpackF4(aBody, offset));
                        offset += 4;
                        continue;
                    case ClassDescriptor.TP_DOUBLE:
                        provider.setDouble(f, aObject, Bytes.unpackF8(aBody, offset));
                        offset += 8;
                        continue;
                    case ClassDescriptor.TP_ENUM: {
                        final int index = Bytes.unpack4(aBody, offset);

                        if (index >= 0) {
                            provider.set(f, aObject, fd.myField.getType().getEnumConstants()[index]);
                        } else {
                            provider.set(f, aObject, null);
                        }

                        offset += 4;
                        continue;
                    }
                    case ClassDescriptor.TP_STRING: {
                        final ArrayPos pos = new ArrayPos(aBody, offset);
                        provider.set(f, aObject, Bytes.unpackString(pos, myEncoding));
                        offset = pos.myOffset;
                        continue;
                    }
                    case ClassDescriptor.TP_CLASS: {
                        final ArrayPos pos = new ArrayPos(aBody, offset);
                        final Class cls = ClassDescriptor.loadClass(this, Bytes.unpackString(pos, myEncoding));
                        provider.set(f, aObject, cls);
                        offset = pos.myOffset;
                        continue;
                    }
                    case ClassDescriptor.TP_DATE: {
                        final long msec = Bytes.unpack8(aBody, offset);
                        offset += 8;
                        Date date = null;

                        if (msec >= 0) {
                            date = new Date(msec);
                        }

                        provider.set(f, aObject, date);
                        continue;
                    }
                    case ClassDescriptor.TP_OBJECT: {
                        final ArrayPos pos = new ArrayPos(aBody, offset);
                        provider.set(f, aObject, unswizzle(pos, f.getType(), aParent, aRecursiveLoading));
                        offset = pos.myOffset;
                        continue;
                    }
                    case ClassDescriptor.TP_VALUE: {
                        final Object value = fd.myClassDescriptor.newInstance();
                        offset = unpackObject(value, fd.myClassDescriptor, aRecursiveLoading, aBody, offset, aParent);
                        provider.set(f, aObject, value);
                        continue;
                    }
                    case ClassDescriptor.TP_RAW:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len >= 0) {
                            final ByteArrayInputStream bin = new ByteArrayInputStream(aBody, offset, len);
                            final ObjectInputStream in = new PersistentObjectInputStream(bin);
                            provider.set(f, aObject, in.readObject());
                            in.close();
                            offset += len;
                        } else if (len < 0) {
                            Object value = null;

                            switch (-2 - len) {
                                case ClassDescriptor.TP_BOOLEAN:
                                    value = Boolean.valueOf(aBody[offset++] != 0);
                                    break;
                                case ClassDescriptor.TP_BYTE:
                                    value = Byte.valueOf(aBody[offset++]);
                                    break;
                                case ClassDescriptor.TP_CHAR:
                                    value = Character.valueOf((char) Bytes.unpack2(aBody, offset));
                                    offset += 2;
                                    break;
                                case ClassDescriptor.TP_SHORT:
                                    value = Short.valueOf(Bytes.unpack2(aBody, offset));
                                    offset += 2;
                                    break;
                                case ClassDescriptor.TP_INT:
                                    value = Integer.valueOf(Bytes.unpack4(aBody, offset));
                                    offset += 4;
                                    break;
                                case ClassDescriptor.TP_LONG:
                                    value = Long.valueOf(Bytes.unpack8(aBody, offset));
                                    offset += 8;
                                    break;
                                case ClassDescriptor.TP_FLOAT:
                                    value = Float.valueOf(Float.intBitsToFloat(Bytes.unpack4(aBody, offset)));
                                    offset += 4;
                                    break;
                                case ClassDescriptor.TP_DOUBLE:
                                    value = Double.valueOf(Double.longBitsToDouble(Bytes.unpack8(aBody, offset)));
                                    offset += 8;
                                    break;
                                case ClassDescriptor.TP_DATE:
                                    value = new Date(Bytes.unpack8(aBody, offset));
                                    offset += 8;
                                    break;
                                case ClassDescriptor.TP_OBJECT:
                                    value = unswizzle(Bytes.unpack4(aBody, offset), Persistent.class,
                                            aRecursiveLoading);
                                    offset += 4;
                                default:
                                    break;
                            }

                            provider.set(f, aObject, value);
                        }

                        continue;
                    case ClassDescriptor.TP_CUSTOM: {
                        final ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(aBody, offset, aParent,
                                aRecursiveLoading, false);
                        mySerializer.unpack(in);
                        provider.set(f, aObject, mySerializer.unpack(in));
                        offset = in.getPosition();
                        continue;
                    }
                    case ClassDescriptor.TP_ARRAY_OF_BYTES:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final byte[] arr = new byte[len];
                            System.arraycopy(aBody, offset, arr, 0, len);
                            offset += len;
                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_BOOLEANS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final boolean[] arr = new boolean[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = aBody[offset++] != 0;
                            }

                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_SHORTS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final short[] arr = new short[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpack2(aBody, offset);
                                offset += 2;
                            }

                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_CHARS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final char[] arr = new char[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = (char) Bytes.unpack2(aBody, offset);
                                offset += 2;
                            }

                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_INTS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final int[] arr = new int[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpack4(aBody, offset);
                                offset += 4;
                            }

                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_ENUMS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            f.set(aObject, null);
                        } else {
                            final Class elemType = f.getType().getComponentType();
                            final Enum[] enumConstants = (Enum[]) elemType.getEnumConstants();
                            final Enum[] arr = (Enum[]) Array.newInstance(elemType, len);

                            for (int j = 0; j < len; j++) {
                                final int index = Bytes.unpack4(aBody, offset);

                                if (index >= 0) {
                                    arr[j] = enumConstants[index];
                                }

                                offset += 4;
                            }

                            f.set(aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_LONGS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final long[] arr = new long[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpack8(aBody, offset);
                                offset += 8;
                            }

                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TO_ARRAY_OF_FLOATS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final float[] arr = new float[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpackF4(aBody, offset);
                                offset += 4;
                            }

                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_DOUBLES:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final double[] arr = new double[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpackF8(aBody, offset);
                                offset += 8;
                            }

                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_DATES:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final Date[] arr = new Date[len];

                            for (int j = 0; j < len; j++) {
                                final long msec = Bytes.unpack8(aBody, offset);
                                offset += 8;

                                if (msec >= 0) {
                                    arr[j] = new Date(msec);
                                }
                            }

                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_STRINGS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final String[] arr = new String[len];
                            final ArrayPos pos = new ArrayPos(aBody, offset);

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpackString(pos, myEncoding);
                            }

                            offset = pos.myOffset;
                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_OBJECTS:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final Class elemType = f.getType().getComponentType();
                            final Object[] arr = (Object[]) Array.newInstance(elemType, len);
                            final ArrayPos pos = new ArrayPos(aBody, offset);

                            for (int j = 0; j < len; j++) {
                                arr[j] = unswizzle(pos, elemType, aParent, aRecursiveLoading);
                            }

                            offset = pos.myOffset;
                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_ARRAY_OF_VALUES:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final Class elemType = f.getType().getComponentType();
                            final Object[] arr = (Object[]) Array.newInstance(elemType, len);
                            final ClassDescriptor valueDesc = fd.myClassDescriptor;

                            for (int j = 0; j < len; j++) {
                                final Object value = valueDesc.newInstance();
                                offset = unpackObject(value, valueDesc, aRecursiveLoading, aBody, offset, aParent);
                                arr[j] = value;
                            }

                            provider.set(f, aObject, arr);
                        }

                        continue;
                    case ClassDescriptor.TP_LINK:
                        len = Bytes.unpack4(aBody, offset);
                        offset += 4;

                        if (len < 0) {
                            provider.set(f, aObject, null);
                        } else {
                            final Object[] arr = new Object[len];

                            for (int j = 0; j < len; j++) {
                                final int elemOid = Bytes.unpack4(aBody, offset);
                                offset += 4;

                                if (elemOid != 0) {
                                    arr[j] = new PersistentStub(this, elemOid);
                                }
                            }

                            provider.set(f, aObject, new LinkImpl(this, arr, aParent));
                        }
                    default:
                        break;
                }
            }
        }

        return offset;
    }

    final Object unswizzle(final ArrayPos aObj, final Class aClass, final Object aParent,
            final boolean aRecursiveLoad) throws Exception {
        final byte[] body = aObj.myBody;
        int offs = aObj.myOffset;
        final int oid = Bytes.unpack4(body, offs);
        final Object val;

        offs += 4;

        if (oid < 0) {
            switch (-1 - oid) {
                case ClassDescriptor.TP_BOOLEAN:
                    val = body[offs++] != 0;
                    break;
                case ClassDescriptor.TP_BYTE:
                    val = body[offs++];
                    break;
                case ClassDescriptor.TP_CHAR:
                    val = (char) Bytes.unpack2(body, offs);
                    offs += 2;
                    break;
                case ClassDescriptor.TP_SHORT:
                    val = Bytes.unpack2(body, offs);
                    offs += 2;
                    break;
                case ClassDescriptor.TP_INT:
                    val = Bytes.unpack4(body, offs);
                    offs += 4;
                    break;
                case ClassDescriptor.TP_LONG:
                    val = Bytes.unpack8(body, offs);
                    offs += 8;
                    break;
                case ClassDescriptor.TP_FLOAT:
                    val = Bytes.unpackF4(body, offs);
                    offs += 4;
                    break;
                case ClassDescriptor.TP_DOUBLE:
                    val = Bytes.unpackF8(body, offs);
                    offs += 8;
                    break;
                case ClassDescriptor.TP_DATE:
                    val = new Date(Bytes.unpack8(body, offs));
                    offs += 8;
                    break;
                case ClassDescriptor.TP_STRING:
                    aObj.myOffset = offs;
                    return Bytes.unpackString(aObj, myEncoding);
                case ClassDescriptor.TP_CLASS:
                    aObj.myOffset = offs;
                    return ClassDescriptor.loadClass(this, Bytes.unpackString(aObj, myEncoding));
                case ClassDescriptor.TP_LINK: {
                    final int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    final Object[] arr = new Object[len];

                    for (int j = 0; j < len; j++) {
                        final int elemOid = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (elemOid != 0) {
                            arr[j] = new PersistentStub(this, elemOid);
                        }
                    }

                    val = new LinkImpl(this, arr, aParent);
                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_BYTES: {
                    final int len = Bytes.unpack4(body, offs);
                    final byte[] arr = new byte[len];

                    offs += 4;

                    System.arraycopy(body, offs, arr, 0, len);

                    offs += len;
                    val = arr;

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_BOOLEANS: {
                    final int len = Bytes.unpack4(body, offs);
                    final boolean[] arr = new boolean[len];

                    offs += 4;

                    for (int j = 0; j < len; j++) {
                        arr[j] = body[offs++] != 0;
                    }

                    val = arr;

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_SHORTS: {
                    final int len = Bytes.unpack4(body, offs);
                    final short[] arr = new short[len];

                    offs += 4;

                    for (int j = 0; j < len; j++) {
                        arr[j] = Bytes.unpack2(body, offs);
                        offs += 2;
                    }

                    val = arr;

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_CHARS: {
                    final int len = Bytes.unpack4(body, offs);
                    final char[] arr = new char[len];

                    offs += 4;

                    for (int j = 0; j < len; j++) {
                        arr[j] = (char) Bytes.unpack2(body, offs);
                        offs += 2;
                    }

                    val = arr;

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_INTS: {
                    final int len = Bytes.unpack4(body, offs);
                    final int[] arr = new int[len];

                    offs += 4;

                    for (int j = 0; j < len; j++) {
                        arr[j] = Bytes.unpack4(body, offs);
                        offs += 4;
                    }

                    val = arr;

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_LONGS: {
                    final int len = Bytes.unpack4(body, offs);
                    final long[] arr = new long[len];

                    offs += 4;

                    for (int j = 0; j < len; j++) {
                        arr[j] = Bytes.unpack8(body, offs);
                        offs += 8;
                    }

                    val = arr;

                    break;
                }
                case ClassDescriptor.TO_ARRAY_OF_FLOATS: {
                    final int len = Bytes.unpack4(body, offs);
                    final float[] arr = new float[len];

                    offs += 4;

                    for (int j = 0; j < len; j++) {
                        arr[j] = Bytes.unpackF4(body, offs);
                        offs += 4;
                    }

                    val = arr;

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_DOUBLES: {
                    final int len = Bytes.unpack4(body, offs);
                    final double[] arr = new double[len];

                    offs += 4;

                    for (int j = 0; j < len; j++) {
                        arr[j] = Bytes.unpackF8(body, offs);
                        offs += 8;
                    }

                    val = arr;

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_OBJECTS: {
                    final int len = Bytes.unpack4(body, offs);
                    final Object[] arr = new Object[len];

                    aObj.myOffset = offs + 4;

                    for (int j = 0; j < len; j++) {
                        arr[j] = unswizzle(aObj, Object.class, aParent, aRecursiveLoad);
                    }

                    return arr;
                }
                case ClassDescriptor.TP_ARRAY_OF_RAWS: {
                    final int len = Bytes.unpack4(body, offs);
                    final int typeOid = Bytes.unpack4(body, offs + 4);
                    final Class elemType;

                    aObj.myOffset = offs + 8;

                    if (typeOid == -1) {
                        elemType = Comparable.class;
                    } else {
                        final ClassDescriptor desc = findClassDescriptor(typeOid);
                        elemType = desc.myClass;
                    }

                    final Object arr = Array.newInstance(elemType, len);

                    for (int j = 0; j < len; j++) {
                        Array.set(arr, j, unswizzle(aObj, elemType, aParent, aRecursiveLoad));
                    }

                    return arr;
                }
                case ClassDescriptor.TP_CUSTOM: {
                    final ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(body, offs, aParent,
                            aRecursiveLoad, false);

                    val = mySerializer.unpack(in);
                    offs = in.getPosition();

                    break;
                }
                default:
                    if (oid < -ClassDescriptor.TP_VALUE_TYPE_BIAS) {
                        final int typeOid = -ClassDescriptor.TP_VALUE_TYPE_BIAS - oid;
                        final ClassDescriptor desc = findClassDescriptor(typeOid);

                        val = desc.newInstance();

                        if (desc.isCollection) {
                            final int len = Bytes.unpack4(body, offs);
                            final Collection collection = (Collection) val;

                            aObj.myOffset = offs + 4;

                            for (int i = 0; i < len; i++) {
                                collection.add(unswizzle(aObj, Object.class, aParent, aRecursiveLoad));
                            }

                            return collection;
                        } else if (desc.isMap) {
                            final int len = Bytes.unpack4(body, offs);
                            final Map map = (Map) val;

                            aObj.myOffset = offs + 4;

                            for (int i = 0; i < len; i++) {
                                final Object key = unswizzle(aObj, Object.class, aParent, aRecursiveLoad);
                                final Object value = unswizzle(aObj, Object.class, aParent, aRecursiveLoad);

                                map.put(key, value);
                            }

                            return map;
                        } else {
                            offs = unpackObject(val, desc, aRecursiveLoad, body, offs, aParent);
                        }
                    } else {
                        throw new StorageError(StorageError.UNSUPPORTED_TYPE);
                    }
            }
        } else {
            val = unswizzle(oid, aClass, aRecursiveLoad);
        }

        aObj.myOffset = offs;

        return val;
    }

    final boolean wasReserved(final long aPosition, final long aSize) {
        for (Location location = myReservedChain; location != null; location = location.myNext) {
            if (aPosition >= location.myPosition && aPosition - location.myPosition < location.mySize ||
                    aPosition <= location.myPosition && location.myPosition - aPosition < aSize) {
                return true;
            }
        }

        return false;
    }

    private void beginThreadTransaction(final int aMode) {
        switch (aMode) {
            case REPLICATION_SLAVE_TRANSACTION:
                throw new IllegalArgumentException(LOGGER.getMessage(MessageCodes.SB_044));
            case SERIALIZABLE_TRANSACTION:
                if (myMulticlientSupport) {
                    throw new IllegalArgumentException(LOGGER.getMessage(MessageCodes.SB_032));
                }

                myUseSerializableTransactions = true;
                getTransactionContext().myNested += 1;

                LOGGER.debug(MessageCodes.SB_037, getTransactionContext().myNested);

                break;
            case EXCLUSIVE_TRANSACTION:
            case COOPERATIVE_TRANSACTION:
                if (myMulticlientSupport) {
                    if (aMode == EXCLUSIVE_TRANSACTION) {
                        LOGGER.debug(MessageCodes.SB_038);
                        myTransactionLock.exclusiveLock();
                    } else {
                        LOGGER.debug(MessageCodes.SB_039);
                        myTransactionLock.sharedLock();
                    }

                    synchronized (myTransactionMonitor) {
                        if (myNumOfNestedTransactions++ == 0) {
                            myFile.lock(aMode == COOPERATIVE_TRANSACTION);

                            final byte[] buffer = new byte[Header.SIZE_OF];
                            final int readCount = myFile.read(0, buffer);
                            final int current;

                            if (readCount > 0 && readCount < Header.SIZE_OF) {
                                throw new StorageError(StorageError.DATABASE_CORRUPTED);
                            }

                            myHeader.unpack(buffer);
                            current = myHeader.myCurrentRoot;
                            myCurrentIndex = current;
                            myCurrentIndexSize = myHeader.myRoot[1 - current].myIndexUsed;
                            myCommittedIndexSize = myCurrentIndexSize;
                            myUsedSize = myHeader.myRoot[current].mySize;

                            if (myHeader.myTransactionId != myTransactionId) {
                                if (myBitmapPageAvailableSpace != null) {
                                    for (int index = 0; index < myBitmapPageAvailableSpace.length; index++) {
                                        myBitmapPageAvailableSpace[index] = Integer.MAX_VALUE;
                                    }
                                }

                                myObjectCache.clear();
                                myPool.clear();
                                myTransactionId = myHeader.myTransactionId;
                            }
                        }
                    }
                } else {
                    synchronized (myTransactionMonitor) {
                        if (myScheduledCommitTime != Long.MAX_VALUE) {
                            myNumOfBlockedTransactions += 1;

                            while (System.currentTimeMillis() >= myScheduledCommitTime) {
                                try {
                                    myTransactionMonitor.wait();
                                } catch (final InterruptedException details) {
                                    LOGGER.warn(details.getMessage(), details);
                                }
                            }

                            myNumOfBlockedTransactions -= 1;
                        }

                        myNumOfNestedTransactions += 1;
                    }

                    if (aMode == EXCLUSIVE_TRANSACTION) {
                        LOGGER.debug(MessageCodes.SB_040);
                        myTransactionLock.exclusiveLock();
                    } else {
                        LOGGER.debug(MessageCodes.SB_041);
                        myTransactionLock.sharedLock();
                    }
                }

                break;
            default:
                throw new IllegalArgumentException(LOGGER.getMessage(MessageCodes.SB_028));
        }
    }

    private void rollbackThreadTransaction() {
        if (myMulticlientSupport) {
            LOGGER.debug(MessageCodes.SB_042); // multi-client rollback

            synchronized (myTransactionMonitor) {
                myTransactionLock.reset();
                rollback();
                myFile.unlock();
                myNumOfNestedTransactions = 0;
            }

            return;
        }

        final ThreadTransactionContext threadTransactionContex = getTransactionContext();

        if (threadTransactionContex.myNested != 0) { // serializable transaction rollback
            final ArrayList modified = threadTransactionContex.myModified;
            final Map locked = threadTransactionContex.myLocked;

            LOGGER.debug(MessageCodes.SB_036, locked.size(), modified.size());

            synchronized (this) {
                synchronized (myObjectCache) {
                    int index = modified.size();

                    if (index == 0) {
                        LOGGER.warn(MessageCodes.SB_043);
                    }

                    while (--index >= 0) {
                        final Object obj = modified.get(index);
                        final int oid = getOid(obj);

                        Assert.that(oid != 0);
                        invalidate(obj);

                        if (getPosition(oid) == 0) {
                            freeId(oid);
                            myObjectCache.remove(oid);
                        } else {
                            loadStub(oid, obj, obj.getClass());
                            myObjectCache.clearDirty(obj);
                        }
                    }
                }
            }

            final Iterator iterator = locked.values().iterator();

            while (iterator.hasNext()) {
                ((IResource) iterator.next()).reset();
            }

            threadTransactionContex.myNested = 0;
            modified.clear();
            threadTransactionContex.myDeleted.clear();
            locked.clear();

            if (myListener != null) {
                myListener.onTransactionRollback();
            }
        } else { // single-client exclusive or cooperative transaction rollback
            synchronized (myTransactionMonitor) {
                myTransactionLock.reset();
                myNumOfNestedTransactions = 0;

                if (myNumOfBlockedTransactions != 0) {
                    myTransactionMonitor.notifyAll();
                }

                rollback();
            }
        }
    }

    private void commit0() {
        int index;
        int jndex;
        int count;
        int current = myCurrentIndex;

        final int[] map = myDirtyPagesMap;
        final int oldIndexSize = myHeader.myRoot[current].myIndexSize;
        int newIndexSize = myHeader.myRoot[1 - current].myIndexSize;
        final int pageCount = myCommittedIndexSize >>> DB_HANDLES_PER_PAGE_BITS;
        Page pg;

        if (newIndexSize > oldIndexSize) {
            long newIndex;

            cloneBitmap(myHeader.myRoot[current].myIndex, oldIndexSize * 8L);

            while (true) {
                newIndex = allocate(newIndexSize * 8L, 0);

                if (newIndexSize == myHeader.myRoot[1 - current].myIndexSize) {
                    break;
                }

                free(newIndex, newIndexSize * 8L);
                newIndexSize = myHeader.myRoot[1 - current].myIndexSize;
            }

            myHeader.myRoot[1 - current].myShadowIndex = newIndex;
            myHeader.myRoot[1 - current].myShadowIndexSize = newIndexSize;

            free(myHeader.myRoot[current].myIndex, oldIndexSize * 8L);
        }

        final long currentSize = myHeader.myRoot[1 - current].mySize;

        for (index = 0; index < pageCount; index++) {
            if ((map[index >> 5] & 1 << (index & 31)) != 0) {
                final Page srcIndex = myPool.getPage(myHeader.myRoot[1 - current].myIndex + (long) index *
                        Page.PAGE_SIZE);
                final Page dstIndex = myPool.getPage(myHeader.myRoot[current].myIndex + (long) index *
                        Page.PAGE_SIZE);

                for (jndex = 0; jndex < Page.PAGE_SIZE; jndex += 8) {
                    final long position = Bytes.unpack8(dstIndex.myData, jndex);

                    if (Bytes.unpack8(srcIndex.myData, jndex) != position && position < currentSize) {
                        if ((position & DB_FREE_HANDLE_FLAG) == 0) {
                            if ((position & DB_PAGE_OBJECT_FLAG) != 0) {
                                free(position & ~DB_FLAGS_MASK, Page.PAGE_SIZE);
                            } else if (position != 0) {
                                final int offset = (int) position & Page.PAGE_SIZE - 1;

                                pg = myPool.getPage(position - offset);
                                free(position, ObjectHeader.getSize(pg.myData, offset));
                                myPool.unfix(pg);
                            }
                        }
                    }
                }

                myPool.unfix(srcIndex);
                myPool.unfix(dstIndex);
            }
        }

        count = myCommittedIndexSize & DB_HANDLES_PER_PAGE - 1;

        if (count != 0 && (map[index >> 5] & 1 << (index & 31)) != 0) {
            final Page srcIndex = myPool.getPage(myHeader.myRoot[1 - current].myIndex + (long) index *
                    Page.PAGE_SIZE);
            final Page dstIndex = myPool.getPage(myHeader.myRoot[current].myIndex + (long) index * Page.PAGE_SIZE);

            jndex = 0;

            do {
                final long position = Bytes.unpack8(dstIndex.myData, jndex);

                if (Bytes.unpack8(srcIndex.myData, jndex) != position && position < currentSize) {
                    if ((position & DB_FREE_HANDLE_FLAG) == 0) {
                        if ((position & DB_PAGE_OBJECT_FLAG) != 0) {
                            free(position & ~DB_FLAGS_MASK, Page.PAGE_SIZE);
                        } else if (position != 0) {
                            final int offset = (int) position & Page.PAGE_SIZE - 1;

                            pg = myPool.getPage(position - offset);
                            free(position, ObjectHeader.getSize(pg.myData, offset));
                            myPool.unfix(pg);
                        }
                    }
                }

                jndex += 8;
            } while (--count != 0);

            myPool.unfix(srcIndex);
            myPool.unfix(dstIndex);
        }

        for (index = 0; index <= pageCount; index++) {
            if ((map[index >> 5] & 1 << (index & 31)) != 0) {
                pg = myPool.putPage(myHeader.myRoot[1 - current].myIndex + (long) index * Page.PAGE_SIZE);

                for (jndex = 0; jndex < Page.PAGE_SIZE; jndex += 8) {
                    Bytes.pack8(pg.myData, jndex, Bytes.unpack8(pg.myData, jndex) & ~DB_MODIFIED_FLAG);
                }

                myPool.unfix(pg);
            }
        }

        if (myCurrentIndexSize > myCommittedIndexSize) {
            long page = myHeader.myRoot[1 - current].myIndex + myCommittedIndexSize * 8L & ~(Page.PAGE_SIZE - 1);
            final long end = myHeader.myRoot[1 - current].myIndex + Page.PAGE_SIZE - 1 + myCurrentIndexSize * 8L &
                    ~(Page.PAGE_SIZE - 1);

            while (page < end) {
                pg = myPool.putPage(page);

                for (jndex = 0; jndex < Page.PAGE_SIZE; jndex += 8) {
                    Bytes.pack8(pg.myData, jndex, Bytes.unpack8(pg.myData, jndex) & ~DB_MODIFIED_FLAG);
                }

                myPool.unfix(pg);
                page += Page.PAGE_SIZE;
            }
        }

        myHeader.myRoot[1 - current].myUsedSize = myUsedSize;
        pg = myPool.putPage(0);
        myHeader.pack(pg.myData);
        myPool.flush();
        myPool.modify(pg);
        Assert.that(myHeader.myTransactionId == myTransactionId);
        myHeader.myTransactionId = ++myTransactionId;
        myHeader.myCurrentRoot = current ^= 1;
        myHeader.isDirty = true;
        myHeader.pack(pg.myData);
        myPool.unfix(pg);
        myPool.flush();
        myHeader.myRoot[1 - current].mySize = myHeader.myRoot[current].mySize;
        myHeader.myRoot[1 - current].myIndexUsed = myCurrentIndexSize;
        myHeader.myRoot[1 - current].myFreeList = myHeader.myRoot[current].myFreeList;
        myHeader.myRoot[1 - current].myBitmapEnd = myHeader.myRoot[current].myBitmapEnd;
        myHeader.myRoot[1 - current].myRootObject = myHeader.myRoot[current].myRootObject;
        myHeader.myRoot[1 - current].myClassDescList = myHeader.myRoot[current].myClassDescList;
        myHeader.myRoot[1 - current].myBitmapExtent = myHeader.myRoot[current].myBitmapExtent;

        if (myCurrentIndexSize == 0 || newIndexSize != oldIndexSize) {
            myHeader.myRoot[1 - current].myIndex = myHeader.myRoot[current].myShadowIndex;
            myHeader.myRoot[1 - current].myIndexSize = myHeader.myRoot[current].myShadowIndexSize;
            myHeader.myRoot[1 - current].myShadowIndex = myHeader.myRoot[current].myIndex;
            myHeader.myRoot[1 - current].myShadowIndexSize = myHeader.myRoot[current].myIndexSize;
            myPool.copy(myHeader.myRoot[1 - current].myIndex, myHeader.myRoot[current].myIndex, myCurrentIndexSize *
                    8L);
            index = myCurrentIndexSize + DB_HANDLES_PER_PAGE * 32 - 1 >>> DB_HANDLES_PER_PAGE_BITS + 5;

            while (--index >= 0) {
                map[index] = 0;
            }
        } else {
            for (index = 0; index < pageCount; index++) {
                if ((map[index >> 5] & 1 << (index & 31)) != 0) {
                    map[index >> 5] -= 1 << (index & 31);
                    myPool.copy(myHeader.myRoot[1 - current].myIndex + (long) index * Page.PAGE_SIZE,
                            myHeader.myRoot[current].myIndex + (long) index * Page.PAGE_SIZE, Page.PAGE_SIZE);
                }
            }

            if (myCurrentIndexSize > index * DB_HANDLES_PER_PAGE && ((map[index >> 5] & 1 << (index & 31)) != 0 ||
                    myCurrentIndexSize != myCommittedIndexSize)) {
                myPool.copy(myHeader.myRoot[1 - current].myIndex + (long) index * Page.PAGE_SIZE,
                        myHeader.myRoot[current].myIndex + (long) index * Page.PAGE_SIZE, 8L * myCurrentIndexSize -
                                (long) index * Page.PAGE_SIZE);
                jndex = index >>> 5;
                count = myCurrentIndexSize + DB_HANDLES_PER_PAGE * 32 - 1 >>> DB_HANDLES_PER_PAGE_BITS + 5;

                while (jndex < count) {
                    map[jndex++] = 0;
                }
            }
        }

        myGcDone = false;
        myCurrentIndex = current;
        myCommittedIndexSize = myCurrentIndexSize;

        if (myMulticlientSupport) {
            myPool.flush();
            pg = myPool.putPage(0);
            myHeader.isDirty = false;
            myHeader.pack(pg.myData);
            myPool.unfix(pg);
            myPool.flush();
        }

        if (myListener != null) {
            myListener.onTransactionCommit();
        }
    }

    private void deallocateObject0(final Object aObj) {
        if (myListener != null) {
            myListener.onObjectDelete(aObj);
        }

        final int oid = getOid(aObj);
        final long pos = getPosition(oid);

        myObjectCache.remove(oid);

        int offset = (int) pos & Page.PAGE_SIZE - 1;

        if ((offset & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
            throw new StorageError(StorageError.DELETED_OBJECT);
        }

        final Page page = myPool.getPage(pos - offset);

        offset &= ~DB_FLAGS_MASK;

        final int size = ObjectHeader.getSize(page.myData, offset);

        myPool.unfix(page);
        freeId(oid);

        final CustomAllocator allocator = myCustomAllocatorMap != null ? getCustomAllocator(aObj.getClass()) : null;

        if (allocator != null) {
            allocator.free(pos & ~DB_FLAGS_MASK, size);
        } else {
            if ((pos & DB_MODIFIED_FLAG) != 0) {
                free(pos & ~DB_FLAGS_MASK, size);
            } else {
                cloneBitmap(pos, size);
            }
        }

        unassignOid(aObj);
    }

    private int gc0() {
        synchronized (myObjectCache) {
            if (!myOpened) {
                throw new StorageError(StorageError.STORAGE_NOT_OPENED);
            }

            if (myGcDone || myGcActive) {
                return 0;
            }

            myGcActive = true;

            if (myBackgroundGc) {
                if (myGcThread == null) {
                    myGcThread = new GcThread();
                }

                myGcThread.activate();
                return 0;
            }

            mark();

            return sweep();
        }
    }

    private boolean getBooleanValue(final Object aValue) {
        if (aValue instanceof Boolean) {
            return ((Boolean) aValue).booleanValue();
        } else if (aValue instanceof String) {
            final String string = (String) aValue;

            if ("true".equalsIgnoreCase(string) || "t".equalsIgnoreCase(string) || "1".equals(string)) {
                return true;
            } else if ("false".equalsIgnoreCase(string) || "f".equalsIgnoreCase(string) || "0".equals(string)) {
                return false;
            }
        }

        throw new StorageError(StorageError.BAD_PROPERTY_VALUE);
    }

    private CustomAllocator getCustomAllocator(final Class aClass) {
        final Object allocator = myCustomAllocatorMap.get(aClass);

        if (allocator != null) {
            return allocator == myDefaultAllocator ? null : (CustomAllocator) allocator;
        }

        final Class superclass = aClass.getSuperclass();

        if (superclass != null) {
            final CustomAllocator alloc = getCustomAllocator(superclass);

            if (alloc != null) {
                myCustomAllocatorMap.put(aClass, alloc);
                return alloc;
            }
        }

        final Class[] interfaces = aClass.getInterfaces();

        for (int index = 0; index < interfaces.length; index++) {
            final CustomAllocator customAllocator = getCustomAllocator(interfaces[index]);

            if (customAllocator != null) {
                myCustomAllocatorMap.put(aClass, customAllocator);
                return customAllocator;
            }
        }

        myCustomAllocatorMap.put(aClass, myDefaultAllocator);

        return null;
    }

    private long getIntegerValue(final Object aValue) {
        if (aValue instanceof Number) {
            return ((Number) aValue).longValue();
        } else if (aValue instanceof String) {
            try {
                return Long.parseLong((String) aValue, 10);
            } catch (final NumberFormatException details) {
                LOGGER.warn(details.getMessage(), details);
            }
        }

        throw new StorageError(StorageError.BAD_PROPERTY_VALUE);
    }

    private void mark() {
        final int bitmapSize = (int) (myHeader.myRoot[myCurrentIndex].mySize >>> DB_ALLOCATION_QUANTUM_BITS + 5) + 1;
        boolean existsNotMarkedObjects;
        long position;
        int index;
        int jndex;

        if (myListener != null) {
            myListener.gcStarted();
        }

        myGreyBitmap = new int[bitmapSize];
        myBlackBitmap = new int[bitmapSize];

        final int rootOid = myHeader.myRoot[myCurrentIndex].myRootObject;

        if (rootOid != 0) {
            markOid(rootOid);

            do {
                existsNotMarkedObjects = false;

                for (index = 0; index < bitmapSize; index++) {
                    if (myGreyBitmap[index] != 0) {
                        existsNotMarkedObjects = true;

                        for (jndex = 0; jndex < 32; jndex++) {
                            if ((myGreyBitmap[index] & 1 << jndex) != 0) {
                                position = ((long) index << 5) + jndex << DB_ALLOCATION_QUANTUM_BITS;
                                myGreyBitmap[index] &= ~(1 << jndex);
                                myBlackBitmap[index] |= 1 << jndex;

                                final int offset = (int) position & Page.PAGE_SIZE - 1;
                                final Page page = myPool.getPage(position - offset);
                                final int typeOid = ObjectHeader.getType(page.myData, offset);

                                if (typeOid != 0) {
                                    final ClassDescriptor desc = findClassDescriptor(typeOid);

                                    if (Btree.class.isAssignableFrom(desc.myClass)) {
                                        final Btree btree = new Btree(page.myData, ObjectHeader.SIZE_OF + offset);

                                        btree.assignOid(this, 0, false);
                                        btree.markTree();
                                    } else if (desc.hasReferences) {
                                        markObject(myPool.get(position), ObjectHeader.SIZE_OF, desc);
                                    }
                                }

                                myPool.unfix(page);
                            }
                        }
                    }
                }
            } while (existsNotMarkedObjects);
        }
    }

    private void rollback0() {
        final int current = myCurrentIndex;
        final int[] map = myDirtyPagesMap;

        if (myHeader.myRoot[1 - current].myIndex != myHeader.myRoot[current].myShadowIndex) {
            myPool.copy(myHeader.myRoot[current].myShadowIndex, myHeader.myRoot[current].myIndex, 8L *
                    myCommittedIndexSize);
        } else {
            final int pageCount = myCommittedIndexSize + DB_HANDLES_PER_PAGE - 1 >>> DB_HANDLES_PER_PAGE_BITS;

            for (int index = 0; index < pageCount; index++) {
                if ((map[index >> 5] & 1 << (index & 31)) != 0) {
                    myPool.copy(myHeader.myRoot[current].myShadowIndex + (long) index * Page.PAGE_SIZE,
                            myHeader.myRoot[current].myIndex + (long) index * Page.PAGE_SIZE, Page.PAGE_SIZE);
                }
            }
        }

        for (int jndex = myCurrentIndexSize + DB_HANDLES_PER_PAGE * 32 - 1 >>> DB_HANDLES_PER_PAGE_BITS +
                5; --jndex >= 0; map[jndex] = 0) {
        }

        myHeader.myRoot[1 - current].myIndex = myHeader.myRoot[current].myShadowIndex;
        myHeader.myRoot[1 - current].myIndexSize = myHeader.myRoot[current].myShadowIndexSize;
        myHeader.myRoot[1 - current].myIndexUsed = myCommittedIndexSize;
        myHeader.myRoot[1 - current].myFreeList = myHeader.myRoot[current].myFreeList;
        myHeader.myRoot[1 - current].myBitmapEnd = myHeader.myRoot[current].myBitmapEnd;
        myHeader.myRoot[1 - current].mySize = myHeader.myRoot[current].mySize;
        myHeader.myRoot[1 - current].myRootObject = myHeader.myRoot[current].myRootObject;
        myHeader.myRoot[1 - current].myClassDescList = myHeader.myRoot[current].myClassDescList;
        myHeader.myRoot[1 - current].myBitmapExtent = myHeader.myRoot[current].myBitmapExtent;
        myUsedSize = myHeader.myRoot[current].mySize;
        myCurrentIndexSize = myCommittedIndexSize;
        myCurrRBitmapPage = myCurrPBitmapPage = 0;
        myCurrRBitmapOffs = myCurrPBitmapOffs = 0;
        reloadScheme();

        if (myListener != null) {
            myListener.onTransactionRollback();
        }
    }

    private void storeObject0(final Object aObj, final boolean aFinalizedObj) {
        if (aObj instanceof IStoreable) {
            ((IPersistent) aObj).onStore();
        }

        if (myListener != null) {
            myListener.onObjectStore(aObj);
        }

        int oid = getOid(aObj);
        boolean newObject = false;

        if (oid == 0) {
            oid = allocateId();

            if (!aFinalizedObj) {
                myObjectCache.put(oid, aObj);
            }

            assignOid(aObj, oid, false);
            newObject = true;
        } else if (isModified(aObj)) {
            myObjectCache.clearDirty(aObj);
        }

        final byte[] data = packObject(aObj, aFinalizedObj);
        final int newSize = ObjectHeader.getSize(data, 0);
        final CustomAllocator allocator = myCustomAllocatorMap != null ? getCustomAllocator(aObj.getClass()) : null;

        long position;

        if (newObject || (position = getPosition(oid)) == 0) {
            position = allocator != null ? allocator.allocate(newSize) : allocate(newSize, 0);
            setPosition(oid, position | DB_MODIFIED_FLAG);
        } else {
            final int offset = (int) position & Page.PAGE_SIZE - 1;

            if ((offset & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
                throw new StorageError(StorageError.DELETED_OBJECT);
            }

            final Page page = myPool.getPage(position - offset);
            final int size = ObjectHeader.getSize(page.myData, offset & ~DB_FLAGS_MASK);

            myPool.unfix(page);

            if ((position & DB_MODIFIED_FLAG) == 0) {
                if (allocator != null) {
                    allocator.free(position & ~DB_FLAGS_MASK, size);
                    position = allocator.allocate(newSize);
                } else {
                    cloneBitmap(position & ~DB_FLAGS_MASK, size);
                    position = allocate(newSize, 0);
                }

                setPosition(oid, position | DB_MODIFIED_FLAG);
            } else {
                position &= ~DB_FLAGS_MASK;

                if (newSize != size) {
                    if (allocator != null) {
                        final long newPosition = allocator.reallocate(position, size, newSize);

                        if (newPosition != position) {
                            position = newPosition;
                            setPosition(oid, position | DB_MODIFIED_FLAG);
                        } else if (newSize < size) {
                            ObjectHeader.setSize(data, 0, size);
                        }
                    } else {
                        if ((newSize + DB_ALLOCATION_QUANTUM - 1 & ~(DB_ALLOCATION_QUANTUM - 1)) > (size +
                                DB_ALLOCATION_QUANTUM - 1 & ~(DB_ALLOCATION_QUANTUM - 1))) {
                            final long newPos = allocate(newSize, 0);

                            cloneBitmap(position, size);
                            free(position, size);
                            position = newPos;
                            setPosition(oid, position | DB_MODIFIED_FLAG);
                        } else if (newSize < size) {
                            ObjectHeader.setSize(data, 0, size);
                        }
                    }
                }
            }
        }

        myModified = true;
        myPool.put(position, data, newSize);
    }

    private int sweep() {
        int deallocatedCount = 0;
        long position;
        myGcDone = true;

        for (int index = DB_FIRST_USER_ID, jndex = myCommittedIndexSize; index < jndex; index++) {
            position = getGCPos(index);

            if (position != 0 && ((int) position & (DB_PAGE_OBJECT_FLAG | DB_FREE_HANDLE_FLAG)) == 0) {
                final int bit = (int) (position >>> DB_ALLOCATION_QUANTUM_BITS);

                if ((myBlackBitmap[bit >>> 5] & 1 << (bit & 31)) == 0) {
                    // object is not accessible
                    if (getPosition(index) == position) {
                        final int offset = (int) position & Page.PAGE_SIZE - 1;
                        final Page page = myPool.getPage(position - offset);
                        final int typeOid = ObjectHeader.getType(page.myData, offset);

                        if (typeOid != 0) {
                            final ClassDescriptor classDescriptor = findClassDescriptor(typeOid);

                            deallocatedCount += 1;

                            if (Btree.class.isAssignableFrom(classDescriptor.myClass)) {
                                final Btree btree = new Btree(page.myData, ObjectHeader.SIZE_OF + offset);

                                myPool.unfix(page);
                                btree.assignOid(this, index, false);
                                btree.deallocate();
                            } else {
                                final int size = ObjectHeader.getSize(page.myData, offset);

                                myPool.unfix(page);
                                freeId(index);
                                myObjectCache.remove(index);
                                cloneBitmap(position, size);
                            }

                            if (myListener != null) {
                                myListener.deallocateObject(classDescriptor.myClass, index);
                            }
                        }
                    }
                }
            }
        }

        myGreyBitmap = null;
        myBlackBitmap = null;
        myAllocatedDelta = 0;
        myGcActive = false;

        if (myListener != null) {
            myListener.gcCompleted(deallocatedCount);
        }

        return deallocatedCount;
    }

    public class AnnotatedPersistentObjectOutputStream extends PersistentObjectOutputStream {

        AnnotatedPersistentObjectOutputStream(final OutputStream aOutputStream) throws IOException {
            super(aOutputStream);
        }

        @Override
        protected void annotateClass(final Class aClass) throws IOException {
            final ClassLoader loader = aClass.getClassLoader();

            writeObject(loader instanceof INamedClassLoader ? ((INamedClassLoader) loader).getName() : null);
        }
    }

    public class PersistentObjectOutputStream extends ObjectOutputStream {

        PersistentObjectOutputStream(final OutputStream aOutputStream) throws IOException {
            super(aOutputStream);
        }

        /**
         * Gets the storage.
         *
         * @return The storage
         */
        public Storage getStorage() {
            return StorageImpl.this;
        }
    }

    class ByteArrayObjectInputStream extends SodboxInputStream {

        private final byte[] myBuffer;

        private final boolean myMarkReferences;

        private final Object myParent;

        private final boolean myRecursiveLoading;

        ByteArrayObjectInputStream(final byte[] aBuffer, final int aOffset, final Object aParent,
                final boolean aRecursiveLoading, final boolean aMarkReferences) {
            super(new ByteArrayInputStream(aBuffer, aOffset, aBuffer.length - aOffset));

            myBuffer = aBuffer;
            myParent = aParent;
            myRecursiveLoading = aRecursiveLoading;
            myMarkReferences = aMarkReferences;
        }

        @Override
        public Object readObject() throws IOException {
            int offset = getPosition();
            Object obj = null;

            if (myMarkReferences) {
                offset = markObjectReference(myBuffer, offset) - offset;
            } else {
                try {
                    final ArrayPos pos = new ArrayPos(myBuffer, offset);

                    obj = unswizzle(pos, Object.class, myParent, myRecursiveLoading);
                    offset = pos.myOffset - offset;
                } catch (final Exception details) {
                    throw new StorageError(StorageError.ACCESS_VIOLATION, details);
                }
            }

            in.skip(offset);

            return obj;
        }

        @Override
        public String readString() throws IOException {
            final int offset = getPosition();
            final ArrayPos position = new ArrayPos(myBuffer, offset);
            final String string = Bytes.unpackString(position, myEncoding);

            in.skip(position.myOffset - offset);

            return string;
        }

        int getPosition() throws IOException {
            return myBuffer.length - in.available();
        }
    }

    static class CloneNode {

        CloneNode myNext;

        long myPosition;

        CloneNode(final long aPosition, final CloneNode aList) {
            myPosition = aPosition;
            myNext = aList;
        }
    }

    class GcThread extends Thread {

        private boolean isOpened;

        GcThread() {
            start();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    synchronized (myBackgroundGcStartMonitor) {
                        while (!isOpened && myOpened) {
                            myBackgroundGcStartMonitor.wait();
                        }

                        if (!myOpened) {
                            return;
                        }

                        isOpened = false;
                    }

                    synchronized (myBackgroundGcMonitor) {
                        if (!myOpened) {
                            return;
                        }

                        mark();

                        synchronized (StorageImpl.this) {
                            synchronized (myObjectCache) {
                                sweep();
                            }
                        }
                    }
                }
            } catch (final InterruptedException details) {
                LOGGER.warn(details.getMessage(), details);
            }
        }

        void activate() {
            synchronized (myBackgroundGcStartMonitor) {
                isOpened = true;
                myBackgroundGcStartMonitor.notify();
            }
        }
    }

    class HashIterator implements PersistentIterator, Iterator {

        Iterator myOids;

        HashIterator(final HashSet aResult) {
            myOids = aResult.iterator();
        }

        @Override
        public boolean hasNext() {
            return myOids.hasNext();
        }

        @Override
        public Object next() {
            final int oid = ((Integer) myOids.next()).intValue();
            return lookupObject(oid, null);
        }

        @Override
        public int nextOID() {
            return myOids.hasNext() ? ((Integer) myOids.next()).intValue() : 0;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static class Location {

        Location myNext;

        long myPosition;

        long mySize;
    }

    class PersistentObjectInputStream extends ObjectInputStream {

        PersistentObjectInputStream(final InputStream aInputStream) throws IOException {
            super(aInputStream);

            enableResolveObject(true);
        }

        @Override
        protected Class resolveClass(final ObjectStreamClass aObjectStreamClass) throws IOException,
                ClassNotFoundException {
            String classLoaderName = null;

            if (myLoaderMap != null && (myCompatibilityMode & CLASS_LOADER_SERIALIZATION_COMPATIBILITY_MODE) == 0) {
                classLoaderName = (String) readObject();
            }

            final ClassLoader classLoader = classLoaderName != null ? findClassLoader(classLoaderName) : myLoader;

            if (classLoader != null) {
                try {
                    return Class.forName(aObjectStreamClass.getName(), false, classLoader);
                } catch (final ClassNotFoundException details) {
                    LOGGER.warn(details.getMessage(), details);
                }
            }

            return super.resolveClass(aObjectStreamClass);
        }

        @Override
        protected Object resolveObject(final Object aObject) throws IOException {
            final int oid = getOid(aObject);

            if (oid != 0) {
                return lookupObject(oid, aObject.getClass());
            }

            return aObject;
        }
    }
}

class Header {

    static final int SIZE_OF = 3 + RootPage.SIZEOF * 2 + 8;

    boolean isDirty; // database was not closed normally

    int myCurrentRoot;

    byte myDatabaseFormatVersion;

    RootPage[] myRoot;

    long myTransactionId;

    final void pack(final byte[] aRecord) {
        int offset = 0;

        aRecord[offset++] = (byte) myCurrentRoot;
        aRecord[offset++] = (byte) (isDirty ? 1 : 0);
        aRecord[offset++] = myDatabaseFormatVersion;

        for (int index = 0; index < 2; index++) {
            Bytes.pack8(aRecord, offset, myRoot[index].mySize);
            offset += 8;
            Bytes.pack8(aRecord, offset, myRoot[index].myIndex);
            offset += 8;
            Bytes.pack8(aRecord, offset, myRoot[index].myShadowIndex);
            offset += 8;
            Bytes.pack8(aRecord, offset, myRoot[index].myUsedSize);
            offset += 8;
            Bytes.pack4(aRecord, offset, myRoot[index].myIndexSize);
            offset += 4;
            Bytes.pack4(aRecord, offset, myRoot[index].myShadowIndexSize);
            offset += 4;
            Bytes.pack4(aRecord, offset, myRoot[index].myIndexUsed);
            offset += 4;
            Bytes.pack4(aRecord, offset, myRoot[index].myFreeList);
            offset += 4;
            Bytes.pack4(aRecord, offset, myRoot[index].myBitmapEnd);
            offset += 4;
            Bytes.pack4(aRecord, offset, myRoot[index].myRootObject);
            offset += 4;
            Bytes.pack4(aRecord, offset, myRoot[index].myClassDescList);
            offset += 4;
            Bytes.pack4(aRecord, offset, myRoot[index].myBitmapExtent);
            offset += 4;
        }

        Bytes.pack8(aRecord, offset, myTransactionId);
        offset += 8;
        Assert.that(offset == SIZE_OF);
    }

    final void unpack(final byte[] aRecord) {
        int offset = 0;

        myCurrentRoot = aRecord[offset++];
        isDirty = aRecord[offset++] != 0;
        myDatabaseFormatVersion = aRecord[offset++];
        myRoot = new RootPage[2];

        for (int index = 0; index < 2; index++) {
            myRoot[index] = new RootPage();
            myRoot[index].mySize = Bytes.unpack8(aRecord, offset);
            offset += 8;
            myRoot[index].myIndex = Bytes.unpack8(aRecord, offset);
            offset += 8;
            myRoot[index].myShadowIndex = Bytes.unpack8(aRecord, offset);
            offset += 8;
            myRoot[index].myUsedSize = Bytes.unpack8(aRecord, offset);
            offset += 8;
            myRoot[index].myIndexSize = Bytes.unpack4(aRecord, offset);
            offset += 4;
            myRoot[index].myShadowIndexSize = Bytes.unpack4(aRecord, offset);
            offset += 4;
            myRoot[index].myIndexUsed = Bytes.unpack4(aRecord, offset);
            offset += 4;
            myRoot[index].myFreeList = Bytes.unpack4(aRecord, offset);
            offset += 4;
            myRoot[index].myBitmapEnd = Bytes.unpack4(aRecord, offset);
            offset += 4;
            myRoot[index].myRootObject = Bytes.unpack4(aRecord, offset);
            offset += 4;
            myRoot[index].myClassDescList = Bytes.unpack4(aRecord, offset);
            offset += 4;
            myRoot[index].myBitmapExtent = Bytes.unpack4(aRecord, offset);
            offset += 4;
        }

        myTransactionId = Bytes.unpack8(aRecord, offset);
        offset += 8;
        Assert.that(offset == SIZE_OF);
    }
}

class RootPage {

    static final int SIZEOF = 64;

    int myBitmapEnd; // index of last allocated bitmap page

    int myBitmapExtent; // Offset of extended bitmap pages in object index

    int myClassDescList; // List of class descriptors

    int myFreeList; // L1 list of free descriptors

    long myIndex; // offset to object index

    int myIndexSize; // size of object index

    int myIndexUsed; // used part of the index

    int myRootObject; // OID of root object

    long myShadowIndex; // offset to shadow index

    int myShadowIndexSize; // size of object index

    long mySize; // database file size

    long myUsedSize; // size used by objects
}
