
package info.freelibrary.sodbox.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Blob;
import info.freelibrary.sodbox.CompressedReadWriteFile;
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
import info.freelibrary.sodbox.MultidimensionalComparator;
import info.freelibrary.sodbox.MultidimensionalIndex;
import info.freelibrary.sodbox.PatriciaTrie;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentComparator;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.PersistentResource;
import info.freelibrary.sodbox.PinnedPersistent;
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
import info.freelibrary.sodbox.impl.ClassDescriptor.FieldDescriptor;

public class StorageImpl implements Storage {

    static final int DB_ALLOCATION_QUANTUM_BITS = 5;

    static final int DB_ALLOCATION_QUANTUM = 1 << DB_ALLOCATION_QUANTUM_BITS;

    static final int DB_BITMAP_ID = 1;

    /**
     * Current version of database format. 0 means that database is not initialized. Used to provide backward
     * compatibility of Sodbox releases.
     */
    static final byte DB_DATABASE_FORMAT_VERSION = (byte) 3;

    static final int DB_DATABASE_OFFSET_BITS = 32; // up to 4 GB

    static final int DB_DATABASE_OID_BITS = 31; // up to 2 billion objects

    static final int DB_BITMAP_SEGMENT_BITS = Page.pageSizeLog + 3 + DB_ALLOCATION_QUANTUM_BITS;

    static final int DB_BITMAP_PAGES = 1 << (DB_DATABASE_OFFSET_BITS - DB_BITMAP_SEGMENT_BITS);

    static final int DB_BITMAP_SEGMENT_SIZE = 1 << DB_BITMAP_SEGMENT_BITS;

    /**
     * Database extension quantum. Memory is allocate by scanning bitmap. If there is no large enough hole, then
     * database is extended by the value of <code>dbDefaultExtensionQuantum</code>. This parameter should not be smaller
     * than <code>dbFirstUserId</code>.
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

    static final int DB_HANDLES_PER_PAGE_BITS = Page.pageSizeLog - 3;

    static final int DB_DIRTY_PAGE_BITMAP_SIZE = 1 << (DB_DATABASE_OID_BITS - DB_HANDLES_PER_PAGE_BITS - 3);

    static final int DB_FIRST_USER_ID = DB_BITMAP_ID + DB_BITMAP_PAGES;

    static final int DB_FLAGS_BITS = 3;

    static final int DB_FLAGS_MASK = 7;

    static final int DB_FREE_HANDLE_FLAG = 4;

    static final int DB_HANDLES_PER_PAGE = 1 << DB_HANDLES_PER_PAGE_BITS;

    static final int DB_INVALID_ID = 0;

    static final int DB_LARGE_DATABASE_OFFSET_BITS = 40; // up to 1 TB

    static final int DB_LARGE_BITMAP_PAGES = 1 << (DB_LARGE_DATABASE_OFFSET_BITS - DB_BITMAP_SEGMENT_BITS);

    static final int DB_MAX_OBJECT_OID = (1 << DB_DATABASE_OID_BITS) - 1;

    static final int DB_MODIFIED_FLAG = 2;

    static final int DB_PAGE_OBJECT_FLAG = 1;

    static final int INC = Page.pageSize / DB_ALLOCATION_QUANTUM / 8;

    static final int PAGE_BITS = Page.pageSize * 8;

    private static Logger LOGGER = LoggerFactory.getLogger(StorageImpl.class);

    protected boolean myAlternativeBtree = false;

    protected boolean myBackgroundGc = false;

    protected String myCacheKind = "default";

    protected int myCompatibilityMode = 0;

    protected long myExtensionQuantum = DB_DEFAULT_EXTENSION_QUANTUM;

    protected boolean myForceStore = false;

    protected int myInitIndexSize = DB_DEFAULT_INIT_INDEX_SIZE;

    protected boolean myLockFile = false;

    protected boolean myMulticlientSupport = false;

    protected boolean myNoFlush = false;

    protected int myObjectCacheInitSize = DB_DEFAULT_OBJECT_CACHE_INIT_SIZE;

    protected long myPagePoolLruLimit = DB_DEFAULT_PAGE_POOL_LRU_LIMIT;

    protected boolean myReadOnly = false;

    protected boolean myReloadObjectsOnRollback = false;

    protected boolean myReuseOid = true;

    protected boolean mySerializeSystemCollections = true;

    long myAllocatedDelta;

    Object myBackgroundGcMonitor;

    Object myBackgroundGcStartMonitor;

    int myBitmapExtentBase;

    int[] myBitmapPageAvailableSpace;

    // bitmap of objects marked during GC
    int[] myBlackBitmap;

    HashMap<Class<?>, ClassDescriptor> myClassDescMap;

    CloneNode myCloneList;

    int myCommittedIndexSize;

    boolean myConcurrentIterator = false;

    // Copy of header.root, used to allow read access to the database during transaction commit
    int myCurrentIndex;

    int myCurrentIndexSize;

    // Offset in current bitmap page for allocating page objects
    int myCurrentPageBitmapOffset;

    // Current bitmap page for allocating page objects
    int myCurrentPageBitmapPage;

    // Offset in current bitmap page for allocating unaligned records
    int myCurrentRecordBitmapOffset;

    // Current bitmap page for allocating records
    int myCurrentRecordBitmapPage;

    ClassDescriptor myDescriptorList;

    // Bitmap of changed pages in current index
    int myDirtyPagesMap[];

    String myEncoding = null;

    IFile myFile;

    boolean myGcActive;

    boolean myGcDone;

    GcThread myGcThread;

    long myGcThreshold;

    // Bitmap of visited during GC but not yet marked object
    int[] myGreyBitmap;

    // Base address of database file mapping
    Header myHeader;

    boolean myInsideCloneBitmap;

    StorageListener myListener;

    ClassLoader myClassLoader;

    HashMap myClassLoaderMap;

    boolean myModified;

    int myBlockedTransactionsCount;

    int myCommittedTransactionsCount;

    int myNestedTransactionsCount;

    OidHashTable myObjectCache;

    boolean myOpened;

    PagePool myPagePool;

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

    PersistentResource transactionLock;

    Object transactionMonitor;

    long usedSize; // total size of allocated objects since the beginning of the
                   // session

    boolean useSerializableTransactions;

    private ArrayList customAllocatorList;

    private HashMap customAllocatorMap;

    private CustomAllocator defaultAllocator;

    private ObjectMap myObjMap;

    static void checkIfFinal(final ClassDescriptor aDescriptor) {
        final Class<?> cls = aDescriptor.cls;

        if (cls != null) {
            for (ClassDescriptor next = aDescriptor.next; next != null; next = next.next) {
                next.load();

                if (cls.isAssignableFrom(next.cls)) {
                    aDescriptor.hasSubclasses = true;
                } else if (next.cls.isAssignableFrom(cls)) {
                    next.hasSubclasses = true;
                }
            }
        }
    }

    static final void memset(final Page aPage, int aOffset, final int aPattern, int aLength) {
        final byte[] array = aPage.data;
        final byte pattern = (byte) aPattern;

        while (--aLength >= 0) {
            array[aOffset++] = pattern;
        }
    }

    @Override
    public/* synchronized */void backup(final OutputStream out) throws java.io.IOException {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }
        synchronized (this) {
            myObjectCache.flush();
        }

        final int curr = 1 - myCurrentIndex;
        final int nObjects = myHeader.myRootPage[curr].myIndexUsed;
        final long indexOffs = myHeader.myRootPage[curr].myIndex;
        int i, j, k;
        final int nUsedIndexPages = ((nObjects + DB_HANDLES_PER_PAGE) - 1) / DB_HANDLES_PER_PAGE;
        final int nIndexPages = ((myHeader.myRootPage[curr].myIndexSize + DB_HANDLES_PER_PAGE) - 1) /
                DB_HANDLES_PER_PAGE;
        long totalRecordsSize = 0;
        long nPagedObjects = 0;
        int bitmapExtent = myHeader.myRootPage[curr].myBitmapExtent;
        final long[] index = new long[nObjects];
        final int[] oids = new int[nObjects];

        if (bitmapExtent == 0) {
            bitmapExtent = Integer.MAX_VALUE;
        }

        for (i = 0, j = 0; i < nUsedIndexPages; i++) {
            final Page pg = myPagePool.getPage(indexOffs + ((long) i * Page.pageSize));

            for (k = 0; (k < DB_HANDLES_PER_PAGE) && (j < nObjects); k++, j++) {
                final long pos = Bytes.unpack8(pg.data, k * 8);
                index[j] = pos;
                oids[j] = j;

                if ((pos & DB_FREE_HANDLE_FLAG) == 0) {
                    if ((pos & DB_PAGE_OBJECT_FLAG) != 0) {
                        nPagedObjects += 1;
                    } else if (pos != 0) {
                        final int offs = (int) pos & (Page.pageSize - 1);
                        final Page op = myPagePool.getPage(pos - offs);
                        int size = ObjectHeader.getSize(op.data, offs & ~DB_FLAGS_MASK);

                        size = ((size + DB_ALLOCATION_QUANTUM) - 1) & ~(DB_ALLOCATION_QUANTUM - 1);
                        totalRecordsSize += size;
                        myPagePool.unfix(op);
                    }
                }
            }

            myPagePool.unfix(pg);
        }

        final Header newHeader = new Header();
        newHeader.myCurrentRoot = 0;
        newHeader.myDbIsDirty = false;
        newHeader.myDatabaseFormatVersion = myHeader.myDatabaseFormatVersion;

        long newFileSize = ((nPagedObjects + (nIndexPages * 2) + 1) * Page.pageSize) + totalRecordsSize;
        newFileSize = ((newFileSize + Page.pageSize) - 1) & ~(Page.pageSize - 1);
        newHeader.myRootPage = new RootPage[2];
        newHeader.myRootPage[0] = new RootPage();
        newHeader.myRootPage[1] = new RootPage();
        newHeader.myRootPage[0].mySize = newHeader.myRootPage[1].mySize = newFileSize;
        newHeader.myRootPage[0].myIndex = newHeader.myRootPage[1].myShadowIndex = Page.pageSize;
        newHeader.myRootPage[0].myShadowIndex = newHeader.myRootPage[1].myIndex = Page.pageSize + ((long) nIndexPages *
                Page.pageSize);
        newHeader.myRootPage[0].myShadowIndexSize = newHeader.myRootPage[0].myIndexSize =
                newHeader.myRootPage[1].myShadowIndexSize = newHeader.myRootPage[1].myIndexSize = nIndexPages *
                        DB_HANDLES_PER_PAGE;
        newHeader.myRootPage[0].myIndexUsed = newHeader.myRootPage[1].myIndexUsed = nObjects;
        newHeader.myRootPage[0].myFreeList = newHeader.myRootPage[1].myFreeList = myHeader.myRootPage[curr].myFreeList;
        newHeader.myRootPage[0].myBitmapEnd = newHeader.myRootPage[1].myBitmapEnd =
                myHeader.myRootPage[curr].myBitmapEnd;

        newHeader.myRootPage[0].myRootObject = newHeader.myRootPage[1].myRootObject =
                myHeader.myRootPage[curr].myRootObject;
        newHeader.myRootPage[0].myClassDescriptorList = newHeader.myRootPage[1].myClassDescriptorList =
                myHeader.myRootPage[curr].myClassDescriptorList;
        newHeader.myRootPage[0].myBitmapExtent = newHeader.myRootPage[1].myBitmapExtent =
                myHeader.myRootPage[curr].myBitmapExtent;

        final byte[] page = new byte[Page.pageSize];
        newHeader.pack(page);
        out.write(page);

        long pageOffs = (long) ((nIndexPages * 2) + 1) * Page.pageSize;
        long recOffs = (nPagedObjects + (nIndexPages * 2) + 1) * Page.pageSize;

        GenericSort.sort(new GenericSortArray() {

            @Override
            public int compare(final int i, final int j) {
                return index[i] < index[j] ? -1 : index[i] == index[j] ? 0 : 1;
            }

            @Override
            public int size() {
                return nObjects;
            }

            @Override
            public void swap(final int i, final int j) {
                final long t1 = index[i];
                index[i] = index[j];
                index[j] = t1;
                final int t2 = oids[i];
                oids[i] = oids[j];
                oids[j] = t2;
            }
        });

        final byte[] newIndex = new byte[nIndexPages * DB_HANDLES_PER_PAGE * 8];

        for (i = 0; i < nObjects; i++) {
            final long pos = index[i];
            final int oid = oids[i];

            if ((pos & DB_FREE_HANDLE_FLAG) == 0) {
                if ((pos & DB_PAGE_OBJECT_FLAG) != 0) {
                    Bytes.pack8(newIndex, oid * 8, pageOffs | DB_PAGE_OBJECT_FLAG);
                    pageOffs += Page.pageSize;
                } else if (pos != 0) {
                    Bytes.pack8(newIndex, oid * 8, recOffs);
                    final int offs = (int) pos & (Page.pageSize - 1);
                    final Page op = myPagePool.getPage(pos - offs);
                    int size = ObjectHeader.getSize(op.data, offs & ~DB_FLAGS_MASK);

                    size = ((size + DB_ALLOCATION_QUANTUM) - 1) & ~(DB_ALLOCATION_QUANTUM - 1);
                    recOffs += size;
                    myPagePool.unfix(op);
                }
            } else {
                Bytes.pack8(newIndex, oid * 8, pos);
            }
        }

        out.write(newIndex);
        out.write(newIndex);

        for (i = 0; i < nObjects; i++) {
            final long pos = index[i];

            if (((int) pos & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) == DB_PAGE_OBJECT_FLAG) {
                if ((oids[i] < (DB_BITMAP_ID + DB_BITMAP_PAGES)) || ((oids[i] >= bitmapExtent) &&
                        (oids[i] < ((bitmapExtent + DB_LARGE_BITMAP_PAGES) - DB_BITMAP_PAGES)))) {
                    final int pageId = oids[i] < (DB_BITMAP_ID + DB_BITMAP_PAGES) ? oids[i] - DB_BITMAP_ID : (oids[i] -
                            bitmapExtent) + myBitmapExtentBase;
                    final long mappedSpace = (long) pageId * Page.pageSize * 8 * DB_ALLOCATION_QUANTUM;

                    if (mappedSpace >= newFileSize) {
                        Arrays.fill(page, (byte) 0);
                    } else if ((mappedSpace + (Page.pageSize * 8 * DB_ALLOCATION_QUANTUM)) <= newFileSize) {
                        Arrays.fill(page, (byte) -1);
                    } else {
                        final int nBits = (int) ((newFileSize - mappedSpace) >> DB_ALLOCATION_QUANTUM_BITS);
                        Arrays.fill(page, 0, nBits >> 3, (byte) -1);
                        page[nBits >> 3] = (byte) ((1 << (nBits & 7)) - 1);
                        Arrays.fill(page, (nBits >> 3) + 1, Page.pageSize, (byte) 0);
                    }

                    out.write(page);
                } else {
                    final Page pg = myPagePool.getPage(pos & ~DB_FLAGS_MASK);
                    out.write(pg.data);
                    myPagePool.unfix(pg);
                }
            }
        }

        for (i = 0; i < nObjects; i++) {
            long pos = index[i];

            if ((pos != 0) && (((int) pos & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) == 0)) {
                pos &= ~DB_FLAGS_MASK;
                int offs = (int) pos & (Page.pageSize - 1);
                Page pg = myPagePool.getPage(pos - offs);
                int size = ObjectHeader.getSize(pg.data, offs);
                size = ((size + DB_ALLOCATION_QUANTUM) - 1) & ~(DB_ALLOCATION_QUANTUM - 1);

                while (true) {
                    if ((Page.pageSize - offs) >= size) {
                        out.write(pg.data, offs, size);
                        break;
                    }

                    out.write(pg.data, offs, Page.pageSize - offs);
                    size -= Page.pageSize - offs;
                    pos += Page.pageSize - offs;
                    offs = 0;
                    myPagePool.unfix(pg);
                    pg = myPagePool.getPage(pos);
                }

                myPagePool.unfix(pg);
            }
        }

        if (recOffs != newFileSize) {
            Assert.that((newFileSize - recOffs) < Page.pageSize);
            final int align = (int) (newFileSize - recOffs);
            Arrays.fill(page, 0, align, (byte) 0);
            out.write(page, 0, align);
        }
    }

    @Override
    public void backup(final String filePath, final String cryptKey) throws java.io.IOException {
        backup(new IFileOutputStream(cryptKey != null ? (IFile) new Rc4File(filePath, false, false, cryptKey)
                : (IFile) new OSFile(filePath, false, false)));
    }

    @Override
    public void beginSerializableTransaction() {
        if (myMulticlientSupport) {
            beginThreadTransaction(READ_WRITE_TRANSACTION);
        } else {
            beginThreadTransaction(SERIALIZABLE_TRANSACTION);
        }
    }

    @Override
    public void beginThreadTransaction(final int mode) {
        switch (mode) {
            case SERIALIZABLE_TRANSACTION:
                if (myMulticlientSupport) {
                    throw new IllegalArgumentException("Illegal transaction mode");
                }

                useSerializableTransactions = true;
                getTransactionContext().nested += 1;;
                break;
            case EXCLUSIVE_TRANSACTION:
            case COOPERATIVE_TRANSACTION:
                if (myMulticlientSupport) {
                    if (mode == EXCLUSIVE_TRANSACTION) {
                        transactionLock.exclusiveLock();
                    } else {
                        transactionLock.sharedLock();
                    }

                    synchronized (transactionMonitor) {
                        if (myNestedTransactionsCount++ == 0) {
                            myFile.lock(mode == READ_ONLY_TRANSACTION);
                            final byte[] buf = new byte[Header.SIZE];
                            final int rc = myFile.read(0, buf);

                            if ((rc > 0) && (rc < Header.SIZE)) {
                                throw new StorageError(StorageError.DATABASE_CORRUPTED);
                            }

                            myHeader.unpack(buf);
                            final int curr = myHeader.myCurrentRoot;
                            myCurrentIndex = curr;
                            myCurrentIndexSize = myHeader.myRootPage[1 - curr].myIndexUsed;
                            myCommittedIndexSize = myCurrentIndexSize;
                            usedSize = myHeader.myRootPage[curr].mySize;

                            if (myHeader.myTransactionId != myTransactionId) {
                                if (myBitmapPageAvailableSpace != null) {
                                    for (int i = 0; i < myBitmapPageAvailableSpace.length; i++) {
                                        myBitmapPageAvailableSpace[i] = Integer.MAX_VALUE;
                                    }
                                }

                                myObjectCache.clear();
                                myPagePool.clear();
                                myTransactionId = myHeader.myTransactionId;
                            }
                        }
                    }
                } else {
                    synchronized (transactionMonitor) {
                        if (myScheduledCommitTime != Long.MAX_VALUE) {
                            myBlockedTransactionsCount += 1;

                            while (System.currentTimeMillis() >= myScheduledCommitTime) {
                                try {
                                    transactionMonitor.wait();
                                } catch (final InterruptedException x) {
                                }
                            }

                            myBlockedTransactionsCount -= 1;
                        }

                        myNestedTransactionsCount += 1;
                    }

                    if (mode == EXCLUSIVE_TRANSACTION) {
                        transactionLock.exclusiveLock();
                    } else {
                        transactionLock.sharedLock();
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("Illegal transaction mode");
        }
    }

    @Override
    public void clearObjectCache() {
        myObjectCache.clear();
    }

    @Override
    public void close() {
        synchronized (myBackgroundGcMonitor) {
            commit();
            myOpened = false;
        }

        if (myGcThread != null) {
            myGcThread.activate();

            try {
                myGcThread.join();
            } catch (final InterruptedException x) {
            }
        }

        if (isDirty()) {
            final Page pg = myPagePool.putPage(0);
            myHeader.pack(pg.data);
            myPagePool.flush();
            myPagePool.modify(pg);
            myHeader.myDbIsDirty = false;
            myHeader.pack(pg.data);
            myPagePool.unfix(pg);
            myPagePool.flush();
        }

        myPagePool.close();
        // make GC easier
        myPagePool = null;
        myObjectCache = null;
        myClassDescMap = null;
        myBitmapPageAvailableSpace = null;
        myDirtyPagesMap = null;
        myDescriptorList = null;
    }

    @Override
    public void commit() {
        if (useSerializableTransactions && (getTransactionContext().nested != 0)) {
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

                if (customAllocatorList != null) {
                    final Iterator iterator = customAllocatorList.iterator();

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
    public void commitSerializableTransaction() {
        if (!isInsideThreadTransaction()) {
            throw new StorageError(StorageError.NOT_IN_TRANSACTION);
        }

        endThreadTransaction(Integer.MAX_VALUE);
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
    public CustomAllocator createBitmapAllocator(final int quantum, final long base, final long extension,
            final long limit) {
        return new BitmapCustomAllocator(this, quantum, base, extension, limit);
    }

    @Override
    public Blob createBlob() {
        return new BlobImpl(this, Page.pageSize - BlobImpl.headerSize);
    }

    @Override
    public <T> FieldIndex<T> createFieldIndex(final Class<?> type, final String fieldName, final boolean unique) {
        return this.<T>createFieldIndex(type, fieldName, unique, false);
    }

    @Override
    public <T> FieldIndex<T> createFieldIndex(final Class<?> type, final String fieldName, final boolean unique,
            final boolean caseInsensitive) {
        return this.<T>createFieldIndex(type, fieldName, unique, caseInsensitive, false);
    }

    @Override
    public synchronized <T> FieldIndex<T> createFieldIndex(final Class<?> aType, final String fieldName,
            final boolean unique, final boolean caseInsensitive, final boolean thick) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final FieldIndex<T> index = thick ? caseInsensitive ? (FieldIndex<T>) new ThickCaseInsensitiveFieldIndex(this,
                aType, fieldName) : (FieldIndex<T>) new ThickFieldIndex(this, aType, fieldName) : caseInsensitive
                        ? myAlternativeBtree ? (FieldIndex<T>) new AltBtreeCaseInsensitiveFieldIndex<T>(aType,
                                fieldName, unique) : (FieldIndex<T>) new BtreeCaseInsensitiveFieldIndex<T>(aType,
                                        fieldName, unique) : myAlternativeBtree
                                                ? (FieldIndex<T>) new AltBtreeFieldIndex<T>(aType, fieldName, unique)
                                                : (FieldIndex<T>) new BtreeFieldIndex<T>(aType, fieldName, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    @Override
    public <T> FieldIndex<T> createFieldIndex(final Class<?> type, final String[] fieldNames, final boolean unique) {
        return this.<T>createFieldIndex(type, fieldNames, unique, false);
    }

    @Override
    public synchronized <T> FieldIndex<T> createFieldIndex(final Class<?> type, final String[] fieldNames,
            final boolean unique, final boolean caseInsensitive) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final FieldIndex<T> index = caseInsensitive ? myAlternativeBtree
                ? (FieldIndex<T>) new AltBtreeCaseInsensitiveMultiFieldIndex(type, fieldNames, unique)
                : (FieldIndex<T>) new BtreeCaseInsensitiveMultiFieldIndex(type, fieldNames, unique) : myAlternativeBtree
                        ? (FieldIndex<T>) new AltBtreeMultiFieldIndex(type, fieldNames, unique)
                        : (FieldIndex<T>) new BtreeMultiFieldIndex(type, fieldNames, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    @Override
    public FullTextIndex createFullTextIndex() {
        return createFullTextIndex(new FullTextSearchHelper(this));
    }

    @Override
    public FullTextIndex createFullTextIndex(final FullTextSearchHelper helper) {
        return new FullTextIndexImpl(this, helper);
    }

    @Override
    public <K, V> IPersistentHash<K, V> createHash() {
        return createHash(101, 2);
    }

    @Override
    public <K, V> IPersistentHash<K, V> createHash(final int pageSize, final int loadFactor) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new PersistentHashImpl<K, V>(this, pageSize, loadFactor);
    }

    @Override
    public synchronized <T> Index<T> createIndex(final Class<?> keyType, final boolean unique) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final Index<T> index = myAlternativeBtree ? (Index<T>) new AltBtree<T>(keyType, unique)
                : (Index<T>) new Btree<T>(keyType, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    @Override
    public synchronized <T> Index<T> createIndex(final Class<?>[] keyTypes, final boolean unique) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final Index<T> index = myAlternativeBtree ? (Index<T>) new AltBtreeCompoundIndex<T>(keyTypes, unique)
                : (Index<T>) new BtreeCompoundIndex<T>(keyTypes, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    @Override
    public <T> Link<T> createLink() {
        return createLink(8);
    }

    @Override
    public <T> Link<T> createLink(final int initialSize) {
        return new LinkImpl<T>(this, initialSize);
    }

    @Override
    public <T> IPersistentList<T> createList() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new PersistentListImpl<T>(this);
    }

    @Override
    public <K extends Comparable<?>, V> IPersistentMap<K, V> createMap(final Class<?> aKeyType) {
        return createMap(aKeyType, 4);
    }

    @Override
    public <K extends Comparable<?>, V> IPersistentMap<K, V> createMap(final Class<?> aKeyType,
            final int aInitialSize) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new PersistentMapImpl<K, V>(this, aKeyType, aInitialSize);
    }

    @Override
    public synchronized <T> MultidimensionalIndex<T> createMultidimensionalIndex(final Class<?> type,
            final String[] fieldNames, final boolean treateZeroAsUndefinedValue) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new KDTree<T>(this, type, fieldNames, treateZeroAsUndefinedValue);
    }

    @Override
    public synchronized <T> MultidimensionalIndex<T> createMultidimensionalIndex(
            final MultidimensionalComparator<T> comparator) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new KDTree<T>(this, comparator);
    }

    @Override
    public <T> PatriciaTrie<T> createPatriciaTrie() {
        return new PTrie<T>();
    }

    @Override
    public Blob createRandomAccessBlob() {
        return new RandomAccessBlobImpl(this);
    }

    @Override
    public <T> FieldIndex<T> createRandomAccessFieldIndex(final Class<?> type, final String fieldName,
            final boolean unique) {
        return this.<T>createRandomAccessFieldIndex(type, fieldName, unique, false);
    }

    @Override
    public synchronized <T> FieldIndex<T> createRandomAccessFieldIndex(final Class<?> type, final String fieldName,
            final boolean unique, final boolean caseInsensitive) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final FieldIndex<T> index = caseInsensitive ? (FieldIndex) new RndBtreeCaseInsensitiveFieldIndex<T>(type,
                fieldName, unique) : (FieldIndex) new RndBtreeFieldIndex<T>(type, fieldName, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    @Override
    public <T> FieldIndex<T> createRandomAccessFieldIndex(final Class<?> type, final String[] fieldNames,
            final boolean unique) {
        return this.<T>createRandomAccessFieldIndex(type, fieldNames, unique, false);
    }

    @Override
    public synchronized <T> FieldIndex<T> createRandomAccessFieldIndex(final Class<?> type, final String[] fieldNames,
            final boolean unique, final boolean caseInsensitive) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final FieldIndex<T> index = caseInsensitive ? (FieldIndex) new RndBtreeCaseInsensitiveMultiFieldIndex(type,
                fieldNames, unique) : (FieldIndex) new RndBtreeMultiFieldIndex(type, fieldNames, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    @Override
    public synchronized <T> Index<T> createRandomAccessIndex(final Class<?> keyType, final boolean unique) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final Index<T> index = new RndBtree<T>(keyType, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    @Override
    public synchronized <T> Index<T> createRandomAccessIndex(final Class<?>[] keyTypes, final boolean unique) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final Index<T> index = new RndBtreeCompoundIndex<T>(keyTypes, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    @Override
    public <M, O> Relation<M, O> createRelation(final O owner) {
        return new RelationImpl<M, O>(this, owner);
    }

    @Override
    public <T> IPersistentList<T> createScalableList() {
        return createScalableList(8);
    }

    @Override
    public <T> IPersistentList<T> createScalableList(final int initialSize) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new ScalableList<T>(this, initialSize);
    }

    @Override
    public synchronized <T> IPersistentSet<T> createScalableSet() {
        return createScalableSet(8);
    }

    @Override
    public synchronized <T> IPersistentSet<T> createScalableSet(final int initialSize) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new ScalableSet(this, initialSize);
    }

    @Override
    public synchronized <T> IPersistentSet<T> createSet() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final IPersistentSet<T> set = myAlternativeBtree ? (IPersistentSet<T>) new AltPersistentSet<T>(true)
                : (IPersistentSet<T>) new PersistentSet<T>(true);
        set.assignOid(this, 0, false);
        return set;
    }

    public <T extends Comparable> SortedCollection<T> createSortedCollection(final boolean unique) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new Ttree<T>(this, new DefaultPersistentComparator<T>(), unique);
    }

    @Override
    public <T> SortedCollection<T> createSortedCollection(final PersistentComparator<T> comparator,
            final boolean unique) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new Ttree<T>(this, comparator, unique);
    }

    @Override
    public synchronized <T> SpatialIndex<T> createSpatialIndex() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new Rtree<T>();
    }

    @Override
    public synchronized <T> SpatialIndexR2<T> createSpatialIndexR2() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new RtreeR2<T>(this);
    }

    @Override
    public synchronized <T> Index<T> createThickIndex(final Class<?> keyType) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        return new ThickIndex<T>(this, keyType);
    }

    @Override
    public <T extends TimeSeries.Tick> TimeSeries<T> createTimeSeries(final Class<?> blockClass,
            final long maxBlockTimeInterval) {
        return new TimeSeriesImpl<T>(this, blockClass, maxBlockTimeInterval);
    }

    @Override
    public void deallocate(final Object obj) {
        deallocateObject(obj);
    }

    @Override
    public/* protected */synchronized void deallocateObject(final Object obj) {
        synchronized (myObjectCache) {
            if (getOid(obj) == 0) {
                return;
            }

            if (useSerializableTransactions) {
                final ThreadTransactionContext ctx = getTransactionContext();

                if (ctx.nested != 0) { // serializable transaction
                    ctx.deleted.add(obj);
                    return;
                }
            }

            deallocateObject0(obj);
        }
    }

    @Override
    public void endThreadTransaction() {
        endThreadTransaction(Integer.MAX_VALUE);
    }

    @Override
    public void endThreadTransaction(final int maxDelay) {
        if (myMulticlientSupport) {
            if (maxDelay != Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Delay is not supported for global transactions");
            }

            synchronized (transactionMonitor) {
                transactionLock.unlock();

                if (myNestedTransactionsCount != 0) { // may be everything is already
                    // aborted
                    if (myNestedTransactionsCount == 1) {
                        commit();
                        myPagePool.flush();
                        myFile.unlock();
                    }

                    myNestedTransactionsCount -= 1;
                }
            }

            return;
        }

        final ThreadTransactionContext ctx = getTransactionContext();

        if (ctx.nested != 0) { // serializable transaction
            if (--ctx.nested == 0) {
                final ArrayList modified = ctx.modified;
                final ArrayList deleted = ctx.deleted;
                final Map locked = ctx.locked;

                synchronized (myBackgroundGcMonitor) {
                    synchronized (this) {
                        synchronized (myObjectCache) {
                            for (int i = modified.size(); --i >= 0;) {
                                store(modified.get(i));
                            }

                            for (int i = deleted.size(); --i >= 0;) {
                                deallocateObject0(deleted.get(i));
                            }

                            if ((modified.size() + deleted.size()) > 0) {
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
        } else { // exclusive or cooperative transaction
            synchronized (transactionMonitor) {
                transactionLock.unlock();

                if (myNestedTransactionsCount != 0) { // may be everything is already
                    // aborted
                    if (--myNestedTransactionsCount == 0) {
                        myCommittedTransactionsCount += 1;
                        commit();
                        myScheduledCommitTime = Long.MAX_VALUE;

                        if (myBlockedTransactionsCount != 0) {
                            transactionMonitor.notifyAll();
                        }
                    } else {
                        if (maxDelay != Integer.MAX_VALUE) {
                            final long nextCommit = System.currentTimeMillis() + maxDelay;
                            if (nextCommit < myScheduledCommitTime) {
                                myScheduledCommitTime = nextCommit;
                            }

                            if (maxDelay == 0) {
                                final int n = myCommittedTransactionsCount;
                                myBlockedTransactionsCount += 1;

                                do {
                                    try {
                                        transactionMonitor.wait();
                                    } catch (final InterruptedException x) {
                                    }
                                } while (myCommittedTransactionsCount == n);

                                myBlockedTransactionsCount -= 1;
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public synchronized void exportXML(final Writer writer) throws IOException {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        myObjectCache.flush();

        final int rootOid = myHeader.myRootPage[1 - myCurrentIndex].myRootObject;

        if (rootOid != 0) {
            final XMLExporter xmlExporter = new XMLExporter(this, writer);
            xmlExporter.exportDatabase(rootOid);
        }
    }

    @Override
    public ClassLoader findClassLoader(final String name) {
        if (myClassLoaderMap == null) {
            return null;
        }

        return (ClassLoader) myClassLoaderMap.get(name);
    }

    @Override
    public synchronized int gc() {
        return gc0();
    }

    @Override
    public ClassLoader getClassLoader() {
        return myClassLoader;
    }

    @Override
    public int getDatabaseFormatVersion() {
        return myHeader.myDatabaseFormatVersion;
    }

    @Override
    public long getDatabaseSize() {
        return myHeader.myRootPage[1 - myCurrentIndex].mySize;
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

            final int bitmapSize = (int) (myHeader.myRootPage[myCurrentIndex].mySize >>> (DB_ALLOCATION_QUANTUM_BITS +
                    5)) + 1;
            boolean existsNotMarkedObjects;
            long pos;
            int i, j;

            // mark
            myGreyBitmap = new int[bitmapSize];
            myBlackBitmap = new int[bitmapSize];
            final int rootOid = myHeader.myRootPage[myCurrentIndex].myRootObject;
            final HashMap map = new HashMap();

            if (rootOid != 0) {
                final MemoryUsage indexUsage = new MemoryUsage(Index.class);
                final MemoryUsage fieldIndexUsage = new MemoryUsage(FieldIndex.class);
                final MemoryUsage classUsage = new MemoryUsage(Class.class);

                markOid(rootOid);

                do {
                    existsNotMarkedObjects = false;

                    for (i = 0; i < bitmapSize; i++) {
                        if (myGreyBitmap[i] != 0) {
                            existsNotMarkedObjects = true;

                            for (j = 0; j < 32; j++) {
                                if ((myGreyBitmap[i] & (1 << j)) != 0) {
                                    pos = (((long) i << 5) + j) << DB_ALLOCATION_QUANTUM_BITS;
                                    myGreyBitmap[i] &= ~(1 << j);
                                    myBlackBitmap[i] |= 1 << j;
                                    final int offs = (int) pos & (Page.pageSize - 1);
                                    final Page pg = myPagePool.getPage(pos - offs);
                                    final int typeOid = ObjectHeader.getType(pg.data, offs);
                                    final int objSize = ObjectHeader.getSize(pg.data, offs);
                                    final int alignedSize = ((objSize + DB_ALLOCATION_QUANTUM) - 1) &
                                            ~(DB_ALLOCATION_QUANTUM - 1);

                                    if (typeOid != 0) {
                                        markOid(typeOid);
                                        final ClassDescriptor desc = findClassDescriptor(typeOid);

                                        if (Btree.class.isAssignableFrom(desc.cls)) {
                                            final Btree btree = new Btree(pg.data, ObjectHeader.sizeof + offs);
                                            btree.assignOid(this, 0, false);
                                            final int nPages = btree.markTree();

                                            if (FieldIndex.class.isAssignableFrom(desc.cls)) {
                                                fieldIndexUsage.nInstances += 1;
                                                fieldIndexUsage.totalSize += ((long) nPages * Page.pageSize) + objSize;
                                                fieldIndexUsage.allocatedSize += ((long) nPages * Page.pageSize) +
                                                        alignedSize;
                                            } else {
                                                indexUsage.nInstances += 1;
                                                indexUsage.totalSize += ((long) nPages * Page.pageSize) + objSize;
                                                indexUsage.allocatedSize += ((long) nPages * Page.pageSize) +
                                                        alignedSize;
                                            }
                                        } else {
                                            MemoryUsage usage = (MemoryUsage) map.get(desc.cls);

                                            if (usage == null) {
                                                usage = new MemoryUsage(desc.cls);
                                                map.put(desc.cls, usage);
                                            }

                                            usage.nInstances += 1;
                                            usage.totalSize += objSize;
                                            usage.allocatedSize += alignedSize;

                                            if (desc.hasReferences) {
                                                markObject(myPagePool.get(pos), ObjectHeader.sizeof, desc);
                                            }
                                        }
                                    } else {
                                        classUsage.nInstances += 1;
                                        classUsage.totalSize += objSize;
                                        classUsage.allocatedSize += alignedSize;
                                    }

                                    myPagePool.unfix(pg);
                                }
                            }
                        }
                    }
                } while (existsNotMarkedObjects);

                if (indexUsage.nInstances != 0) {
                    map.put(Index.class, indexUsage);
                }

                if (fieldIndexUsage.nInstances != 0) {
                    map.put(FieldIndex.class, fieldIndexUsage);
                }

                if (classUsage.nInstances != 0) {
                    map.put(Class.class, classUsage);
                }

                final MemoryUsage system = new MemoryUsage(Storage.class);
                system.totalSize += myHeader.myRootPage[0].myIndexSize * 8L;
                system.totalSize += myHeader.myRootPage[1].myIndexSize * 8L;
                system.totalSize += (long) (myHeader.myRootPage[myCurrentIndex].myBitmapEnd - DB_BITMAP_ID) *
                        Page.pageSize;
                system.totalSize += Page.pageSize; // root page

                if (myHeader.myRootPage[myCurrentIndex].myBitmapExtent != 0) {
                    system.allocatedSize = getBitmapUsedSpace(DB_BITMAP_ID, DB_BITMAP_ID + DB_BITMAP_PAGES) +
                            getBitmapUsedSpace((myHeader.myRootPage[myCurrentIndex].myBitmapExtent + DB_BITMAP_PAGES) -
                                    myBitmapExtentBase, (myHeader.myRootPage[myCurrentIndex].myBitmapExtent +
                                            myHeader.myRootPage[myCurrentIndex].myBitmapEnd) - DB_BITMAP_ID -
                                            myBitmapExtentBase);
                } else {
                    system.allocatedSize = getBitmapUsedSpace(DB_BITMAP_ID,
                            myHeader.myRootPage[myCurrentIndex].myBitmapEnd);
                }

                system.nInstances = myHeader.myRootPage[myCurrentIndex].myIndexSize;
                map.put(Storage.class, system);
            }

            return map;
        }
    }

    @Override
    public synchronized Object getObjectByOID(final int oid) {
        return oid == 0 ? null : lookupObject(oid, null);
    }

    @Override
    public int getOid(final Object obj) {
        return obj instanceof IPersistent ? ((IPersistent) obj).getOid() : obj == null ? 0 : myObjMap.getOid(obj);
    }

    @Override
    public Properties getProperties() {
        return myProperties;
    }

    @Override
    public Object getProperty(final String name) {
        return myProperties.get(name);
    }

    @Override
    public synchronized Object getRoot() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final int rootOid = myHeader.myRootPage[1 - myCurrentIndex].myRootObject;
        return rootOid == 0 ? null : lookupObject(rootOid, null);
    }

    /**
     * This method is used internally by Sodbox to get transaction context associated with current thread. But it can be
     * also used by application to get transaction context, store it in some variable and use in another thread. I will
     * make it possible to share one transaction between multiple threads.
     *
     * @return transaction context associated with current thread
     */
    @Override
    public ThreadTransactionContext getTransactionContext() {
        return (ThreadTransactionContext) myTransactionContext.get();
    }

    @Override
    public long getUsedSize() {
        return usedSize;
    }

    @Override
    public synchronized void importXML(final java.io.Reader reader) throws XMLImportException {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        final XMLImporter xmlImporter = new XMLImporter(this, reader);
        xmlImporter.importDatabase();
    }

    public void invalidate(final Object obj) {
        if (obj instanceof IPersistent) {
            ((IPersistent) obj).invalidate();
        } else {
            synchronized (myObjMap) {
                final ObjectMap.Entry e = myObjMap.put(obj);
                e.state &= ~PinnedPersistent.DIRTY;
                e.state |= PinnedPersistent.RAW;
            }
        }
    }

    @Override
    public boolean isInsideThreadTransaction() {
        return (getTransactionContext().nested != 0) || (myNestedTransactionsCount != 0);
    }

    @Override
    public boolean isOpened() {
        return myOpened;
    }

    @Override
    public Iterator join(final Iterator[] selections) {
        final HashSet result = new HashSet();

        for (final Iterator selection : selections) {
            final PersistentIterator iterator = (PersistentIterator) selection;
            int oid;

            while ((oid = iterator.nextOid()) != 0) {
                result.add(new Integer(oid));
            }
        }

        return new HashIterator(result);
    }

    @Override
    public void load(final Object obj) {
        if (obj instanceof IPersistent) {
            ((IPersistent) obj).load();
        } else {
            synchronized (myObjMap) {
                final ObjectMap.Entry e = myObjMap.get(obj);

                if ((e == null) || ((e.state & PinnedPersistent.RAW) == 0) || (e.oid == 0)) {
                    return;
                }
            }

            loadObject(obj);
        }
    }

    @Override
    public/* protected */synchronized void loadObject(final Object obj) {
        if (isRaw(obj)) {
            loadStub(getOid(obj), obj, obj.getClass());
        }
    }

    @Override
    public/* protected */boolean lockObject(final Object obj) {
        if (useSerializableTransactions) {
            final ThreadTransactionContext ctx = getTransactionContext();

            if (ctx.nested != 0) { // serializable transaction
                return ctx.locked.put(obj, obj) == null;
            }
        }

        return true;
    }

    @Override
    public synchronized int makePersistent(final Object obj) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        if (obj == null) {
            return 0;
        }

        int oid = getOid(obj);

        if (oid != 0) {
            return oid;
        }

        if (myForceStore && (!useSerializableTransactions || (getTransactionContext().nested == 0))) {
            synchronized (myObjectCache) {
                storeObject0(obj, false);
            }

            return getOid(obj);
        } else {
            synchronized (myObjectCache) {
                oid = allocateId();
                assignOid(obj, oid, false);
                setPos(oid, 0);
                myObjectCache.put(oid, obj);
                modify(obj);
                return oid;
            }
        }
    }

    @Override
    public Iterator merge(final Iterator[] selections) {
        HashSet result = null;

        for (final Iterator selection : selections) {
            final PersistentIterator iterator = (PersistentIterator) selection;
            final HashSet newResult = new HashSet();
            int oid;

            while ((oid = iterator.nextOid()) != 0) {
                final Integer oidWrapper = new Integer(oid);
                if ((result == null) || result.contains(oidWrapper)) {
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
    public void modify(final Object obj) {
        if (obj instanceof IPersistent) {
            ((IPersistent) obj).modify();
        } else {
            if (useSerializableTransactions) {
                final ThreadTransactionContext ctx = getTransactionContext();

                if (ctx.nested != 0) { // serializable transaction
                    ctx.modified.add(obj);
                    return;
                }
            }

            synchronized (this) {
                synchronized (myObjectCache) {
                    synchronized (myObjMap) {
                        final ObjectMap.Entry e = myObjMap.put(obj);

                        if (((e.state & PinnedPersistent.DIRTY) == 0) && (e.oid != 0)) {
                            if ((e.state & PinnedPersistent.RAW) != 0) {
                                throw new StorageError(StorageError.ACCESS_TO_STUB);
                            }

                            Assert.that((e.state & PinnedPersistent.DELETED) == 0);
                            storeObject(obj);
                            e.state &= ~PinnedPersistent.DIRTY;
                        }
                    }
                }
            }
        }
    }

    @Override
    public/* protected */synchronized void modifyObject(final Object obj) {
        synchronized (myObjectCache) {
            if (!isModified(obj)) {
                myModified = true;

                if (useSerializableTransactions) {
                    final ThreadTransactionContext ctx = getTransactionContext();

                    if (ctx.nested != 0) { // serializable transaction
                        ctx.modified.add(obj);
                        return;
                    }
                }

                myObjectCache.setDirty(obj);
            }
        }
    }

    @Override
    public void open(final IFile file) {
        open(file, DEFAULT_PAGE_POOL_SIZE);
    }

    @Override
    public synchronized void open(final IFile file, final long pagePoolSize) {
        Page pg;
        int i;

        if (myOpened) {
            throw new StorageError(StorageError.STORAGE_ALREADY_OPENED);
        }

        initialize(file, pagePoolSize);

        if (myMulticlientSupport) {
            beginThreadTransaction(myReadOnly ? READ_ONLY_TRANSACTION : READ_WRITE_TRANSACTION);
        }

        final byte[] buf = new byte[Header.SIZE];
        final int rc = file.read(0, buf);
        final int corruptionError = (file instanceof Rc4File) || (file instanceof CompressedReadWriteFile)
                ? StorageError.WRONG_CIPHER_KEY : StorageError.DATABASE_CORRUPTED;

        if ((rc > 0) && (rc < Header.SIZE)) {
            throw new StorageError(corruptionError);
        }

        myHeader.unpack(buf);

        if ((myHeader.myCurrentRoot < 0) || (myHeader.myCurrentRoot > 1)) {
            throw new StorageError(corruptionError);
        }

        myTransactionId = myHeader.myTransactionId;

        if (myHeader.myDatabaseFormatVersion == 0) { // database not initialized
            if (myReadOnly) {
                throw new StorageError(StorageError.READ_ONLY_DATABASE);
            }

            int indexSize = myInitIndexSize;

            if (indexSize < DB_FIRST_USER_ID) {
                indexSize = DB_FIRST_USER_ID;
            }

            indexSize = ((indexSize + DB_HANDLES_PER_PAGE) - 1) & ~(DB_HANDLES_PER_PAGE - 1);

            myBitmapExtentBase = DB_BITMAP_PAGES;

            myHeader.myCurrentRoot = myCurrentIndex = 0;
            long used = Page.pageSize;
            myHeader.myRootPage[0].myIndex = used;
            myHeader.myRootPage[0].myIndexSize = indexSize;
            myHeader.myRootPage[0].myIndexUsed = DB_FIRST_USER_ID;
            myHeader.myRootPage[0].myFreeList = 0;
            used += indexSize * 8L;
            myHeader.myRootPage[1].myIndex = used;
            myHeader.myRootPage[1].myIndexSize = indexSize;
            myHeader.myRootPage[1].myIndexUsed = DB_FIRST_USER_ID;
            myHeader.myRootPage[1].myFreeList = 0;
            used += indexSize * 8L;

            myHeader.myRootPage[0].myShadowIndex = myHeader.myRootPage[1].myIndex;
            myHeader.myRootPage[1].myShadowIndex = myHeader.myRootPage[0].myIndex;
            myHeader.myRootPage[0].myShadowIndexSize = indexSize;
            myHeader.myRootPage[1].myShadowIndexSize = indexSize;

            final int bitmapPages = (int) (((used + (Page.pageSize * ((DB_ALLOCATION_QUANTUM * 8) - 1))) - 1) /
                    (Page.pageSize * ((DB_ALLOCATION_QUANTUM * 8) - 1)));
            final long bitmapSize = (long) bitmapPages * Page.pageSize;
            int usedBitmapSize = (int) ((used + bitmapSize) >>> (DB_ALLOCATION_QUANTUM_BITS + 3));

            for (i = 0; i < bitmapPages; i++) {
                pg = myPagePool.putPage(used + ((long) i * Page.pageSize));

                final byte[] bitmap = pg.data;
                final int n = usedBitmapSize > Page.pageSize ? Page.pageSize : usedBitmapSize;

                for (int j = 0; j < n; j++) {
                    bitmap[j] = (byte) 0xFF;
                }

                usedBitmapSize -= Page.pageSize;
                myPagePool.unfix(pg);
            }

            final int bitmapIndexSize = ((((DB_BITMAP_ID + DB_BITMAP_PAGES) * 8) + Page.pageSize) - 1) &
                    ~(Page.pageSize - 1);
            final byte[] index = new byte[bitmapIndexSize];
            Bytes.pack8(index, DB_INVALID_ID * 8, DB_FREE_HANDLE_FLAG);

            for (i = 0; i < bitmapPages; i++) {
                Bytes.pack8(index, (DB_BITMAP_ID + i) * 8, used | DB_PAGE_OBJECT_FLAG);
                used += Page.pageSize;
            }

            myHeader.myRootPage[0].myBitmapEnd = DB_BITMAP_ID + i;
            myHeader.myRootPage[1].myBitmapEnd = DB_BITMAP_ID + i;

            while (i < DB_BITMAP_PAGES) {
                Bytes.pack8(index, (DB_BITMAP_ID + i) * 8, DB_FREE_HANDLE_FLAG);
                i += 1;
            }

            myHeader.myRootPage[0].mySize = used;
            myHeader.myRootPage[1].mySize = used;
            usedSize = used;
            myCommittedIndexSize = myCurrentIndexSize = DB_FIRST_USER_ID;

            myPagePool.write(myHeader.myRootPage[1].myIndex, index);
            myPagePool.write(myHeader.myRootPage[0].myIndex, index);

            myModified = true;
            myHeader.myDbIsDirty = true;
            myHeader.myRootPage[0].mySize = myHeader.myRootPage[1].mySize;
            pg = myPagePool.putPage(0);
            myHeader.pack(pg.data);
            myPagePool.flush();
            myPagePool.modify(pg);
            myHeader.myDatabaseFormatVersion = DB_DATABASE_FORMAT_VERSION;
            myHeader.pack(pg.data);
            myPagePool.unfix(pg);
            myPagePool.flush();
        } else {
            final int curr = myHeader.myCurrentRoot;

            myCurrentIndex = curr;

            if (myHeader.myRootPage[curr].myIndexSize != myHeader.myRootPage[curr].myShadowIndexSize) {
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

                myHeader.myRootPage[1 - curr].mySize = myHeader.myRootPage[curr].mySize;
                myHeader.myRootPage[1 - curr].myIndexUsed = myHeader.myRootPage[curr].myIndexUsed;
                myHeader.myRootPage[1 - curr].myFreeList = myHeader.myRootPage[curr].myFreeList;
                myHeader.myRootPage[1 - curr].myIndex = myHeader.myRootPage[curr].myShadowIndex;
                myHeader.myRootPage[1 - curr].myIndexSize = myHeader.myRootPage[curr].myShadowIndexSize;
                myHeader.myRootPage[1 - curr].myShadowIndex = myHeader.myRootPage[curr].myIndex;
                myHeader.myRootPage[1 - curr].myShadowIndexSize = myHeader.myRootPage[curr].myIndexSize;
                myHeader.myRootPage[1 - curr].myBitmapEnd = myHeader.myRootPage[curr].myBitmapEnd;
                myHeader.myRootPage[1 - curr].myRootObject = myHeader.myRootPage[curr].myRootObject;
                myHeader.myRootPage[1 - curr].myClassDescriptorList = myHeader.myRootPage[curr].myClassDescriptorList;
                myHeader.myRootPage[1 - curr].myBitmapExtent = myHeader.myRootPage[curr].myBitmapExtent;

                myModified = true;
                pg = myPagePool.putPage(0);
                myHeader.pack(pg.data);
                myPagePool.unfix(pg);

                myPagePool.copy(myHeader.myRootPage[1 - curr].myIndex, myHeader.myRootPage[curr].myIndex,
                        (((myHeader.myRootPage[curr].myIndexUsed * 8L) + Page.pageSize) - 1) & ~(Page.pageSize - 1));

                if (myListener != null) {
                    myListener.recoveryCompleted();
                }

                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Database recovery completed");
                }
            }

            myCurrentIndexSize = myHeader.myRootPage[1 - curr].myIndexUsed;
            myCommittedIndexSize = myCurrentIndexSize;
            usedSize = myHeader.myRootPage[curr].mySize;
        }

        final int bitmapSize = myHeader.myRootPage[1 - myCurrentIndex].myBitmapExtent == 0 ? DB_BITMAP_PAGES
                : DB_LARGE_BITMAP_PAGES;

        myBitmapPageAvailableSpace = new int[bitmapSize];

        for (i = 0; i < myBitmapPageAvailableSpace.length; i++) {
            myBitmapPageAvailableSpace[i] = Integer.MAX_VALUE;
        }

        myCurrentRecordBitmapPage = myCurrentPageBitmapPage = 0;
        myCurrentRecordBitmapOffset = myCurrentPageBitmapOffset = 0;

        myOpened = true;
        reloadScheme();

        if (myMulticlientSupport) {
            // modified = true; ??? Why it is needed here?
            endThreadTransaction();
        } else {
            commit(); // commit scheme changes
        }
    }

    @Override
    public void open(final String filePath) {
        open(filePath, DEFAULT_PAGE_POOL_SIZE);
    }

    @Override
    public synchronized void open(final String filePath, final long pagePoolSize) {
        final IFile file = filePath.startsWith("@") ? (IFile) new MultiFile(filePath.substring(1), myReadOnly,
                myNoFlush) : (IFile) new OSFile(filePath, myReadOnly, myNoFlush);

        try {
            open(file, pagePoolSize);
        } catch (final StorageError ex) {
            file.close();
            throw ex;
        }
    }

    @Override
    public synchronized void open(final String filePath, final long pagePoolSize, final String cryptKey) {
        final Rc4File file = new Rc4File(filePath, myReadOnly, myNoFlush, cryptKey);

        try {
            open(file, pagePoolSize);
        } catch (final StorageError ex) {
            file.close();
            throw ex;
        }
    }

    @Override
    public void registerClassLoader(final INamedClassLoader loader) {
        if (myClassLoaderMap == null) {
            myClassLoaderMap = new HashMap();
        }

        myClassLoaderMap.put(loader.getName(), loader);
    }

    @Override
    public synchronized void registerCustomAllocator(final Class<?> cls, final CustomAllocator allocator) {
        synchronized (myObjectCache) {
            final ClassDescriptor desc = getClassDescriptor(cls);
            desc.allocator = allocator;
            storeObject0(desc, false);

            if (customAllocatorMap == null) {
                customAllocatorMap = new HashMap();
                customAllocatorList = new ArrayList();
            }

            customAllocatorMap.put(cls, allocator);
            customAllocatorList.add(allocator);
            reserveLocation(allocator.getSegmentBase(), allocator.getSegmentSize());
        }
    }

    @Override
    public synchronized void rollback() {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        if (useSerializableTransactions && (getTransactionContext().nested != 0)) {
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
    public void rollbackThreadTransaction() {
        if (myMulticlientSupport) {
            synchronized (transactionMonitor) {
                transactionLock.reset();
                rollback();
                myFile.unlock();
                myNestedTransactionsCount = 0;
            }

            return;
        }

        final ThreadTransactionContext ctx = getTransactionContext();

        if (ctx.nested != 0) { // serializable transaction
            final ArrayList modified = ctx.modified;
            final Map locked = ctx.locked;

            synchronized (this) {
                synchronized (myObjectCache) {
                    int i = modified.size();

                    while (--i >= 0) {
                        final Object obj = modified.get(i);
                        final int oid = getOid(obj);
                        Assert.that(oid != 0);
                        invalidate(obj);

                        if (getPos(oid) == 0) {
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

            ctx.nested = 0;
            modified.clear();
            ctx.deleted.clear();
            locked.clear();

            if (myListener != null) {
                myListener.onTransactionRollback();
            }
        } else {
            synchronized (transactionMonitor) {
                transactionLock.reset();
                myNestedTransactionsCount = 0;

                if (myBlockedTransactionsCount != 0) {
                    transactionMonitor.notifyAll();
                }

                rollback();
            }
        }
    }

    @Override
    public ClassLoader setClassLoader(final ClassLoader loader) {
        final ClassLoader prev = loader;
        this.myClassLoader = loader;
        return prev;
    }

    @Override
    public void setCustomSerializer(final CustomSerializer serializer) {
        this.mySerializer = serializer;
    }

    @Override
    public void setGcThreshold(final long maxAllocatedDelta) {
        myGcThreshold = maxAllocatedDelta;
    }

    @Override
    public StorageListener setListener(final StorageListener listener) {
        final StorageListener prevListener = this.myListener;
        this.myListener = listener;
        return prevListener;
    }

    @Override
    public void setProperties(final Properties props) {
        String value;
        myProperties.putAll(props);

        if ((value = props.getProperty("sodbox.implicit.values")) != null) {
            ClassDescriptor.treateAnyNonPersistentObjectAsValue = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.serialize.transient.objects")) != null) {
            ClassDescriptor.serializeNonPersistentObjects = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.object.cache.init.size")) != null) {
            myObjectCacheInitSize = (int) getIntegerValue(value);

            if (myObjectCacheInitSize <= 0) {
                throw new IllegalArgumentException("Initial object cache size should be positive");
            }
        }

        if ((value = props.getProperty("sodbox.object.cache.kind")) != null) {
            myCacheKind = value;
        }

        if ((value = props.getProperty("sodbox.object.index.init.size")) != null) {
            myInitIndexSize = (int) getIntegerValue(value);
        }

        if ((value = props.getProperty("sodbox.extension.quantum")) != null) {
            myExtensionQuantum = getIntegerValue(value);
        }

        if ((value = props.getProperty("sodbox.gc.threshold")) != null) {
            myGcThreshold = getIntegerValue(value);
        }

        if ((value = props.getProperty("sodbox.file.readonly")) != null) {
            myReadOnly = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.file.noflush")) != null) {
            myNoFlush = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.alternative.btree")) != null) {
            myAlternativeBtree = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.background.gc")) != null) {
            myBackgroundGc = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.string.encoding")) != null) {
            myEncoding = value;
        }

        if ((value = props.getProperty("sodbox.lock.file")) != null) {
            myLockFile = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.replication.ack")) != null) {
            myReplicationAck = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.concurrent.iterator")) != null) {
            myConcurrentIterator = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.slave.connection.timeout")) != null) {
            mySlaveConnectionTimeout = (int) getIntegerValue(value);
        }

        if ((value = props.getProperty("sodbox.force.store")) != null) {
            myForceStore = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.page.pool.lru.limit")) != null) {
            myPagePoolLruLimit = getIntegerValue(value);
        }

        if ((value = props.getProperty("sodbox.multiclient.support")) != null) {
            myMulticlientSupport = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.reload.objects.on.rollback")) != null) {
            myReloadObjectsOnRollback = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.reuse.oid")) != null) {
            myReuseOid = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.serialize.system.collections")) != null) {
            mySerializeSystemCollections = getBooleanValue(value);
        }

        if ((value = props.getProperty("sodbox.compatibility.mode")) != null) {
            myCompatibilityMode = (int) getIntegerValue(value);
        }

        if (myMulticlientSupport && myBackgroundGc) {
            throw new IllegalArgumentException("In mutliclient access mode bachround GC is not supported");
        }
    }

    @Override
    public void setProperty(final String name, final Object value) {
        myProperties.put(name, value);

        if (name.equals("sodbox.implicit.values")) {
            ClassDescriptor.treateAnyNonPersistentObjectAsValue = getBooleanValue(value);
        } else if (name.equals("sodbox.serialize.transient.objects")) {
            ClassDescriptor.serializeNonPersistentObjects = getBooleanValue(value);
        } else if (name.equals("sodbox.object.cache.init.size")) {
            myObjectCacheInitSize = (int) getIntegerValue(value);

            if (myObjectCacheInitSize <= 0) {
                throw new IllegalArgumentException("Initial object cache size should be positive");
            }
        } else if (name.equals("sodbox.object.cache.kind")) {
            myCacheKind = (String) value;
        } else if (name.equals("sodbox.object.index.init.size")) {
            myInitIndexSize = (int) getIntegerValue(value);
        } else if (name.equals("sodbox.extension.quantum")) {
            myExtensionQuantum = getIntegerValue(value);
        } else if (name.equals("sodbox.gc.threshold")) {
            myGcThreshold = getIntegerValue(value);
        } else if (name.equals("sodbox.file.readonly")) {
            myReadOnly = getBooleanValue(value);
        } else if (name.equals("sodbox.file.noflush")) {
            myNoFlush = getBooleanValue(value);
        } else if (name.equals("sodbox.alternative.btree")) {
            myAlternativeBtree = getBooleanValue(value);
        } else if (name.equals("sodbox.background.gc")) {
            myBackgroundGc = getBooleanValue(value);
        } else if (name.equals("sodbox.string.encoding")) {
            myEncoding = value == null ? null : value.toString();
        } else if (name.equals("sodbox.lock.file")) {
            myLockFile = getBooleanValue(value);
        } else if (name.equals("sodbox.replication.ack")) {
            myReplicationAck = getBooleanValue(value);
        } else if (name.equals("sodbox.concurrent.iterator")) {
            myConcurrentIterator = getBooleanValue(value);
        } else if (name.equals("sodbox.slave.connection.timeout")) {
            mySlaveConnectionTimeout = (int) getIntegerValue(value);
        } else if (name.equals("sodbox.force.store")) {
            myForceStore = getBooleanValue(value);
        } else if (name.equals("sodbox.page.pool.lru.limit")) {
            myPagePoolLruLimit = getIntegerValue(value);
        } else if (name.equals("sodbox.multiclient.support")) {
            myMulticlientSupport = getBooleanValue(value);
        } else if (name.equals("sodbox.reload.objects.on.rollback")) {
            myReloadObjectsOnRollback = getBooleanValue(value);
        } else if (name.equals("sodbox.reuse.oid")) {
            myReuseOid = getBooleanValue(value);
        } else if (name.equals("sodbox.compatibility.mode")) {
            myCompatibilityMode = (int) getIntegerValue(value);
        } else if (name.equals("sodbox.serialize.system.collections")) {
            mySerializeSystemCollections = getBooleanValue(value);
        }

        if (myMulticlientSupport && myBackgroundGc) {
            throw new IllegalArgumentException("In mutliclient access mode bachround GC is not supported");
        }
    }

    @Override
    public boolean setRecursiveLoading(final Class<?> type, final boolean enabled) {
        synchronized (myRecursiveLoadingPolicy) {
            final Object prevValue = myRecursiveLoadingPolicy.put(type, enabled);
            myRecursiveLoadingPolicyDefined = true;
            return prevValue == null ? true : ((Boolean) prevValue).booleanValue();
        }
    }

    @Override
    public synchronized void setRoot(final Object root) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        if (root == null) {
            myHeader.myRootPage[1 - myCurrentIndex].myRootObject = 0;
        } else {
            if (!isPersistent(root)) {
                storeObject0(root, false);
            }

            myHeader.myRootPage[1 - myCurrentIndex].myRootObject = getOid(root);
        }

        myModified = true;
    }

    /**
     * Associate transaction context with the thread This method can be used by application to share the same
     * transaction between multiple threads
     *
     * @param ctx new transaction context
     * @return transaction context previously associated with this thread
     */
    @Override
    public ThreadTransactionContext setTransactionContext(final ThreadTransactionContext ctx) {
        final ThreadTransactionContext oldCtx = (ThreadTransactionContext) myTransactionContext.get();
        myTransactionContext.set(ctx);
        return oldCtx;
    }

    @Override
    public void store(final Object obj) {
        if (obj instanceof IPersistent) {
            ((IPersistent) obj).store();
        } else {
            synchronized (this) {
                synchronized (myObjectCache) {
                    synchronized (myObjMap) {
                        final ObjectMap.Entry e = myObjMap.put(obj);

                        if ((e.state & PinnedPersistent.RAW) != 0) {
                            throw new StorageError(StorageError.ACCESS_TO_STUB);
                        }

                        storeObject(obj);
                        e.state &= ~PinnedPersistent.DIRTY;
                    }
                }
            }
        }
    }

    @Override
    public/* protected */void storeFinalizedObject(final Object obj) {
        if (myOpened) {
            synchronized (myObjectCache) {
                if (getOid(obj) != 0) {
                    storeObject0(obj, true);
                }
            }
        }
    }

    @Override
    public/* protected */synchronized void storeObject(final Object obj) {
        if (!myOpened) {
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }

        if (useSerializableTransactions && (getTransactionContext().nested != 0)) {
            // Store should not be used in serializable transaction mode
            throw new StorageError(StorageError.INVALID_OPERATION, "store object");
        }

        synchronized (myObjectCache) {
            storeObject0(obj, false);
        }
    }

    @Override
    public void throwObject(final Object obj) {
        myObjectCache.remove(getOid(obj));
    }

    protected OidHashTable createObjectCache(final String kind, final long pagePoolSize, final int objectCacheSize) {
        if ("strong".equals(kind)) {
            return new StrongHashTable(this, objectCacheSize);
        }

        if ("soft".equals(kind)) {
            return new SoftHashTable(this, objectCacheSize);
        }

        if ("weak".equals(kind)) {
            return new WeakHashTable(this, objectCacheSize);
        }

        if ("pinned".equals(kind)) {
            return new PinWeakHashTable(this, objectCacheSize);
        }

        if ("lru".equals(kind)) {
            return new LruObjectCache(this, objectCacheSize);
        }

        return pagePoolSize == INFINITE_PAGE_POOL ? (OidHashTable) new StrongHashTable(this, objectCacheSize)
                : (OidHashTable) new LruObjectCache(this, objectCacheSize);
    }

    protected void initialize(final IFile file, final long pagePoolSize) {
        this.myFile = file;

        if (myLockFile && !myMulticlientSupport) {
            if (!file.tryLock(myReadOnly)) {
                throw new StorageError(StorageError.STORAGE_IS_USED);
            }
        }

        myDirtyPagesMap = new int[(DB_DIRTY_PAGE_BITMAP_SIZE / 4) + 1];
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

        myNestedTransactionsCount = 0;
        myBlockedTransactionsCount = 0;
        myCommittedTransactionsCount = 0;
        myScheduledCommitTime = Long.MAX_VALUE;
        transactionMonitor = new Object();
        transactionLock = new PersistentResource();

        myModified = false;

        myObjectCache = createObjectCache(myCacheKind, pagePoolSize, myObjectCacheInitSize);

        myObjMap = new ObjectMap(myObjectCacheInitSize);

        myClassDescMap = new HashMap<>();
        myDescriptorList = null;

        myRecursiveLoadingPolicy = new HashMap<>();
        myRecursiveLoadingPolicyDefined = false;

        myHeader = new Header();
        myPagePool = new PagePool((int) (pagePoolSize / Page.pageSize), myPagePoolLruLimit);
        myPagePool.open(file);
    }

    protected boolean isDirty() {
        return myHeader.myDbIsDirty;
    }

    protected int swizzle(final Object obj, final boolean finalized) {
        int oid = 0;

        if (obj != null) {
            if (!isPersistent(obj)) {
                storeObject0(obj, finalized);
            }

            oid = getOid(obj);
        }

        return oid;
    }

    protected Object unswizzle(final int oid, final Class<?> cls, final boolean recursiveLoading) {
        if (oid == 0) {
            return null;
        }

        if (recursiveLoading) {
            return lookupObject(oid, cls);
        }

        Object stub = myObjectCache.get(oid);

        if (stub != null) {
            return stub;
        }

        ClassDescriptor desc;

        if ((cls == Object.class) || ((desc = findClassDescriptor(cls)) == null) || desc.hasSubclasses) {
            final long pos = getPos(oid);
            final int offs = (int) pos & (Page.pageSize - 1);

            if ((offs & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
                throw new StorageError(StorageError.DELETED_OBJECT);
            }

            final Page pg = myPagePool.getPage(pos - offs);
            final int typeOid = ObjectHeader.getType(pg.data, offs & ~DB_FLAGS_MASK);
            myPagePool.unfix(pg);
            desc = findClassDescriptor(typeOid);
        }

        stub = desc.newInstance();
        assignOid(stub, oid, true);
        myObjectCache.put(oid, stub);
        return stub;
    }

    final long allocate(long size, final int oid) {
        synchronized (myObjectCache) {
            setDirty();
            size = ((size + DB_ALLOCATION_QUANTUM) - 1) & ~(DB_ALLOCATION_QUANTUM - 1);
            Assert.that(size != 0);
            myAllocatedDelta += size;

            if ((myAllocatedDelta > myGcThreshold) && !myInsideCloneBitmap) {
                gc0();
            }

            int objBitSize = (int) (size >> DB_ALLOCATION_QUANTUM_BITS);
            Assert.that(objBitSize == (size >> DB_ALLOCATION_QUANTUM_BITS));
            long pos;
            int holeBitSize = 0;
            final int alignment = (int) size & (Page.pageSize - 1);
            int offs, firstPage, lastPage, i, j;
            int holeBeforeFreePage = 0;
            int freeBitmapPage = 0;
            final int curr = 1 - myCurrentIndex;
            Page pg;

            lastPage = myHeader.myRootPage[curr].myBitmapEnd - DB_BITMAP_ID;
            usedSize += size;

            if (alignment == 0) {
                firstPage = myCurrentPageBitmapPage;
                offs = ((myCurrentPageBitmapOffset + INC) - 1) & ~(INC - 1);
            } else {
                firstPage = myCurrentRecordBitmapPage;
                offs = myCurrentRecordBitmapOffset;
            }

            while (true) {
                if (alignment == 0) {
                    // allocate page object
                    for (i = firstPage; i < lastPage; i++) {
                        final int spaceNeeded = (objBitSize - holeBitSize) < PAGE_BITS ? objBitSize - holeBitSize
                                : PAGE_BITS;

                        if (myBitmapPageAvailableSpace[i] <= spaceNeeded) {
                            holeBitSize = 0;
                            offs = 0;
                            continue;
                        }

                        pg = getBitmapPage(i);
                        int startOffs = offs;

                        while (offs < Page.pageSize) {
                            if (pg.data[offs++] != 0) {
                                offs = ((offs + INC) - 1) & ~(INC - 1);
                                holeBitSize = 0;
                            } else if ((holeBitSize += 8) == objBitSize) {
                                pos = (((((long) i * Page.pageSize) + offs) * 8) -
                                        holeBitSize) << DB_ALLOCATION_QUANTUM_BITS;

                                if (wasReserved(pos, size)) {
                                    startOffs = offs = ((offs + INC) - 1) & ~(INC - 1);
                                    holeBitSize = 0;
                                    continue;
                                }

                                reserveLocation(pos, size);
                                myCurrentPageBitmapPage = i;
                                myCurrentPageBitmapOffset = offs;
                                extend(pos + size);

                                if (oid != 0) {
                                    final long prev = getPos(oid);
                                    final int marker = (int) prev & DB_FLAGS_MASK;
                                    myPagePool.copy(pos, prev - marker, size);
                                    setPos(oid, pos | marker | DB_MODIFIED_FLAG);
                                }

                                myPagePool.unfix(pg);
                                pg = putBitmapPage(i);

                                int holeBytes = holeBitSize >> 3;

                                if (holeBytes > offs) {
                                    memset(pg, 0, 0xFF, offs);
                                    holeBytes -= offs;
                                    myPagePool.unfix(pg);
                                    pg = putBitmapPage(--i);
                                    offs = Page.pageSize;
                                }

                                while (holeBytes > Page.pageSize) {
                                    memset(pg, 0, 0xFF, Page.pageSize);
                                    holeBytes -= Page.pageSize;
                                    myBitmapPageAvailableSpace[i] = 0;
                                    myPagePool.unfix(pg);
                                    pg = putBitmapPage(--i);
                                }

                                memset(pg, offs - holeBytes, 0xFF, holeBytes);
                                commitLocation();
                                myPagePool.unfix(pg);

                                return pos;
                            }
                        }

                        if ((startOffs == 0) && (holeBitSize == 0) && (spaceNeeded < myBitmapPageAvailableSpace[i])) {
                            myBitmapPageAvailableSpace[i] = spaceNeeded;
                        }

                        offs = 0;
                        myPagePool.unfix(pg);
                    }
                } else {
                    for (i = firstPage; i < lastPage; i++) {
                        final int spaceNeeded = (objBitSize - holeBitSize) < PAGE_BITS ? objBitSize - holeBitSize
                                : PAGE_BITS;

                        if (myBitmapPageAvailableSpace[i] <= spaceNeeded) {
                            holeBitSize = 0;
                            offs = 0;
                            continue;
                        }

                        pg = getBitmapPage(i);

                        int startOffs = offs;

                        while (offs < Page.pageSize) {
                            final int mask = pg.data[offs] & 0xFF;

                            if ((holeBitSize + Bitmap.firstHoleSize[mask]) >= objBitSize) {
                                pos = (((((long) i * Page.pageSize) + offs) * 8) -
                                        holeBitSize) << DB_ALLOCATION_QUANTUM_BITS;

                                if (wasReserved(pos, size)) {
                                    startOffs = offs += 1;
                                    holeBitSize = 0;
                                    continue;
                                }

                                reserveLocation(pos, size);
                                myCurrentRecordBitmapPage = i;
                                myCurrentRecordBitmapOffset = offs;
                                extend(pos + size);

                                if (oid != 0) {
                                    final long prev = getPos(oid);
                                    final int marker = (int) prev & DB_FLAGS_MASK;

                                    myPagePool.copy(pos, prev - marker, size);
                                    setPos(oid, pos | marker | DB_MODIFIED_FLAG);
                                }

                                myPagePool.unfix(pg);
                                pg = putBitmapPage(i);
                                pg.data[offs] |= (byte) ((1 << (objBitSize - holeBitSize)) - 1);

                                if (holeBitSize != 0) {
                                    if (holeBitSize > (offs * 8)) {
                                        memset(pg, 0, 0xFF, offs);
                                        holeBitSize -= offs * 8;
                                        myPagePool.unfix(pg);
                                        pg = putBitmapPage(--i);
                                        offs = Page.pageSize;
                                    }

                                    while (holeBitSize > PAGE_BITS) {
                                        memset(pg, 0, 0xFF, Page.pageSize);
                                        holeBitSize -= PAGE_BITS;
                                        myBitmapPageAvailableSpace[i] = 0;
                                        myPagePool.unfix(pg);
                                        pg = putBitmapPage(--i);
                                    }

                                    while ((holeBitSize -= 8) > 0) {
                                        pg.data[--offs] = (byte) 0xFF;
                                    }

                                    pg.data[offs - 1] |= (byte) ~((1 << -holeBitSize) - 1);
                                }

                                myPagePool.unfix(pg);
                                commitLocation();

                                return pos;
                            } else if (Bitmap.maxHoleSize[mask] >= objBitSize) {
                                final int holeBitOffset = Bitmap.maxHoleOffset[mask];

                                pos = (((((long) i * Page.pageSize) + offs) * 8) +
                                        holeBitOffset) << DB_ALLOCATION_QUANTUM_BITS;

                                if (wasReserved(pos, size)) {
                                    startOffs = offs += 1;
                                    holeBitSize = 0;
                                    continue;
                                }

                                reserveLocation(pos, size);
                                myCurrentRecordBitmapPage = i;
                                myCurrentRecordBitmapOffset = offs;
                                extend(pos + size);

                                if (oid != 0) {
                                    final long prev = getPos(oid);
                                    final int marker = (int) prev & DB_FLAGS_MASK;

                                    myPagePool.copy(pos, prev - marker, size);
                                    setPos(oid, pos | marker | DB_MODIFIED_FLAG);
                                }

                                myPagePool.unfix(pg);
                                pg = putBitmapPage(i);
                                pg.data[offs] |= (byte) (((1 << objBitSize) - 1) << holeBitOffset);
                                myPagePool.unfix(pg);
                                commitLocation();

                                return pos;
                            }

                            offs += 1;

                            if (Bitmap.lastHoleSize[mask] == 8) {
                                holeBitSize += 8;
                            } else {
                                holeBitSize = Bitmap.lastHoleSize[mask];
                            }
                        }

                        if ((startOffs == 0) && (holeBitSize == 0) && (spaceNeeded < myBitmapPageAvailableSpace[i])) {
                            myBitmapPageAvailableSpace[i] = spaceNeeded;
                        }

                        offs = 0;
                        myPagePool.unfix(pg);
                    }
                }
                if (firstPage == 0) {
                    if (freeBitmapPage > i) {
                        i = freeBitmapPage;
                        holeBitSize = holeBeforeFreePage;
                    }

                    objBitSize -= holeBitSize;
                    // number of bits reserved for the object and aligned on
                    // page boundary
                    final int skip = ((objBitSize + (Page.pageSize / DB_ALLOCATION_QUANTUM)) - 1) & ~((Page.pageSize /
                            DB_ALLOCATION_QUANTUM) - 1);
                    // page aligned position after allocated object
                    pos = ((long) i << DB_BITMAP_SEGMENT_BITS) + ((long) skip << DB_ALLOCATION_QUANTUM_BITS);

                    long extension = size > myExtensionQuantum ? size : myExtensionQuantum;
                    int oldIndexSize = 0;
                    long oldIndex = 0;
                    int morePages = (int) (((extension + (Page.pageSize * ((DB_ALLOCATION_QUANTUM * 8) - 1))) - 1) /
                            (Page.pageSize * ((DB_ALLOCATION_QUANTUM * 8) - 1)));

                    if ((i + morePages) > DB_LARGE_BITMAP_PAGES) {
                        throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
                    }

                    if ((i <= DB_BITMAP_PAGES) && ((i + morePages) > DB_BITMAP_PAGES)) {
                        // We are out of space mapped by memory default
                        // allocation bitmap
                        oldIndexSize = myHeader.myRootPage[curr].myIndexSize;

                        if (oldIndexSize <= ((myCurrentIndexSize + DB_LARGE_BITMAP_PAGES) - DB_BITMAP_PAGES)) {
                            int newIndexSize = oldIndexSize;

                            oldIndex = myHeader.myRootPage[curr].myIndex;

                            do {
                                newIndexSize <<= 1;

                                if (newIndexSize < 0) {
                                    newIndexSize = Integer.MAX_VALUE & ~(DB_HANDLES_PER_PAGE - 1);

                                    if (newIndexSize < ((myCurrentIndexSize + DB_LARGE_BITMAP_PAGES) -
                                            DB_BITMAP_PAGES)) {
                                        throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
                                    }

                                    break;
                                }
                            } while (newIndexSize <= ((myCurrentIndexSize + DB_LARGE_BITMAP_PAGES) - DB_BITMAP_PAGES));

                            if ((size + (newIndexSize * 8L)) > myExtensionQuantum) {
                                extension = size + (newIndexSize * 8L);
                                morePages = (int) (((extension + (Page.pageSize * ((DB_ALLOCATION_QUANTUM * 8) - 1))) -
                                        1) / (Page.pageSize * ((DB_ALLOCATION_QUANTUM * 8) - 1)));
                            }

                            extend(pos + ((long) morePages * Page.pageSize) + (newIndexSize * 8L));
                            final long newIndex = pos + ((long) morePages * Page.pageSize);

                            fillBitmap(pos + (skip >> 3) + ((long) morePages * (Page.pageSize / DB_ALLOCATION_QUANTUM /
                                    8)), newIndexSize >>> DB_ALLOCATION_QUANTUM_BITS);

                            myPagePool.copy(newIndex, oldIndex, oldIndexSize * 8L);
                            myHeader.myRootPage[curr].myIndex = newIndex;
                            myHeader.myRootPage[curr].myIndexSize = newIndexSize;
                        }

                        final int[] newBitmapPageAvailableSpace = new int[DB_LARGE_BITMAP_PAGES];

                        System.arraycopy(myBitmapPageAvailableSpace, 0, newBitmapPageAvailableSpace, 0,
                                DB_BITMAP_PAGES);

                        for (j = DB_BITMAP_PAGES; j < DB_LARGE_BITMAP_PAGES; j++) {
                            newBitmapPageAvailableSpace[j] = Integer.MAX_VALUE;
                        }

                        myBitmapPageAvailableSpace = newBitmapPageAvailableSpace;

                        for (j = 0; j < (DB_LARGE_BITMAP_PAGES - DB_BITMAP_PAGES); j++) {
                            setPos(myCurrentIndexSize + j, DB_FREE_HANDLE_FLAG);
                        }

                        myHeader.myRootPage[curr].myBitmapExtent = myCurrentIndexSize;
                        myHeader.myRootPage[curr].myIndexUsed = myCurrentIndexSize += DB_LARGE_BITMAP_PAGES -
                                DB_BITMAP_PAGES;
                    }

                    extend(pos + ((long) morePages * Page.pageSize));

                    long adr = pos;
                    int len = objBitSize >> 3;

                    // fill bitmap pages used for allocation of object space
                    // with 0xFF
                    while (len >= Page.pageSize) {
                        pg = myPagePool.putPage(adr);
                        memset(pg, 0, 0xFF, Page.pageSize);
                        myPagePool.unfix(pg);
                        adr += Page.pageSize;
                        len -= Page.pageSize;
                    }

                    // fill part of last page responsible for allocation of
                    // object space
                    pg = myPagePool.putPage(adr);
                    memset(pg, 0, 0xFF, len);
                    pg.data[len] = (byte) ((1 << (objBitSize & 7)) - 1);
                    myPagePool.unfix(pg);

                    // mark in bitmap newly allocated object
                    fillBitmap(pos + (skip >> 3), morePages * (Page.pageSize / DB_ALLOCATION_QUANTUM / 8));

                    j = i;

                    while (--morePages >= 0) {
                        setPos(getBitmapPageId(j++), pos | DB_PAGE_OBJECT_FLAG | DB_MODIFIED_FLAG);
                        pos += Page.pageSize;
                    }

                    myHeader.myRootPage[curr].myBitmapEnd = j + DB_BITMAP_ID;
                    j = i + (objBitSize / PAGE_BITS);

                    if (alignment != 0) {
                        myCurrentRecordBitmapPage = j;
                        myCurrentRecordBitmapOffset = 0;
                    } else {
                        myCurrentPageBitmapPage = j;
                        myCurrentPageBitmapOffset = 0;
                    }

                    while (j > i) {
                        myBitmapPageAvailableSpace[--j] = 0;
                    }

                    pos = (((long) i * Page.pageSize * 8) - holeBitSize) << DB_ALLOCATION_QUANTUM_BITS;

                    if (oid != 0) {
                        final long prev = getPos(oid);
                        final int marker = (int) prev & DB_FLAGS_MASK;

                        myPagePool.copy(pos, prev - marker, size);
                        setPos(oid, pos | marker | DB_MODIFIED_FLAG);
                    }

                    if (holeBitSize != 0) {
                        reserveLocation(pos, size);

                        while (holeBitSize > PAGE_BITS) {
                            holeBitSize -= PAGE_BITS;
                            pg = putBitmapPage(--i);
                            memset(pg, 0, 0xFF, Page.pageSize);
                            myBitmapPageAvailableSpace[i] = 0;
                            myPagePool.unfix(pg);
                        }

                        pg = putBitmapPage(--i);
                        offs = Page.pageSize;

                        while ((holeBitSize -= 8) > 0) {
                            pg.data[--offs] = (byte) 0xFF;
                        }

                        pg.data[offs - 1] |= (byte) ~((1 << -holeBitSize) - 1);
                        myPagePool.unfix(pg);
                        commitLocation();
                    }

                    if (oldIndex != 0) {
                        free(oldIndex, oldIndexSize * 8L);
                    }

                    return pos;
                }

                if ((myGcThreshold != Long.MAX_VALUE) && !myGcDone && !myGcActive && !myInsideCloneBitmap) {
                    myAllocatedDelta -= size;
                    usedSize -= size;
                    gc0();
                    myCurrentRecordBitmapPage = myCurrentPageBitmapPage = 0;
                    myCurrentRecordBitmapOffset = myCurrentPageBitmapOffset = 0;
                    return allocate(size, oid);
                }

                freeBitmapPage = i;
                holeBeforeFreePage = holeBitSize;
                holeBitSize = 0;
                lastPage = firstPage + 1;
                firstPage = 0;
                offs = 0;
            }
        }
    }

    int allocateId() {
        synchronized (myObjectCache) {
            int oid;
            final int curr = 1 - myCurrentIndex;

            setDirty();

            if (myReuseOid && ((oid = myHeader.myRootPage[curr].myFreeList) != 0)) {
                myHeader.myRootPage[curr].myFreeList = (int) (getPos(oid) >> DB_FLAGS_BITS);
                Assert.that(myHeader.myRootPage[curr].myFreeList >= 0);
                myDirtyPagesMap[oid >>> (DB_HANDLES_PER_PAGE_BITS + 5)] |= 1 << ((oid >>> DB_HANDLES_PER_PAGE_BITS) &
                        31);
                return oid;
            }

            if (myCurrentIndexSize > DB_MAX_OBJECT_OID) {
                throw new StorageError(StorageError.TOO_MUCH_OBJECTS);
            }

            if (myCurrentIndexSize >= myHeader.myRootPage[curr].myIndexSize) {
                final int oldIndexSize = myHeader.myRootPage[curr].myIndexSize;
                int newIndexSize = oldIndexSize << 1;

                if (newIndexSize < oldIndexSize) {
                    newIndexSize = Integer.MAX_VALUE & ~(DB_HANDLES_PER_PAGE - 1);

                    if (newIndexSize <= oldIndexSize) {
                        throw new StorageError(StorageError.NOT_ENOUGH_SPACE);
                    }
                }

                final long newIndex = allocate(newIndexSize * 8L, 0);

                if (myCurrentIndexSize >= myHeader.myRootPage[curr].myIndexSize) {
                    final long oldIndex = myHeader.myRootPage[curr].myIndex;
                    myPagePool.copy(newIndex, oldIndex, myCurrentIndexSize * 8L);
                    myHeader.myRootPage[curr].myIndex = newIndex;
                    myHeader.myRootPage[curr].myIndexSize = newIndexSize;
                    free(oldIndex, oldIndexSize * 8L);
                } else {
                    // index was already reallocated
                    free(newIndex, newIndexSize * 8L);
                }
            }

            oid = myCurrentIndexSize;
            myHeader.myRootPage[curr].myIndexUsed = ++myCurrentIndexSize;

            return oid;
        }
    }

    int allocatePage() {
        final int oid = allocateId();

        setPos(oid, allocate(Page.pageSize, 0) | DB_PAGE_OBJECT_FLAG | DB_MODIFIED_FLAG);

        return oid;
    }

    void assignOid(final Object obj, final int oid, final boolean raw) {
        if (obj instanceof IPersistent) {
            ((IPersistent) obj).assignOid(this, oid, raw);
        } else {
            synchronized (myObjMap) {
                final ObjectMap.Entry e = myObjMap.put(obj);
                e.oid = oid;

                if (raw) {
                    e.state = PinnedPersistent.RAW;
                }
            }
        }

        if (myListener != null) {
            myListener.onObjectAssignOid(obj);
        }
    }

    final void cloneBitmap(long pos, long size) {
        synchronized (myObjectCache) {
            if (myInsideCloneBitmap) {
                Assert.that(size == Page.pageSize);
                myCloneList = new CloneNode(pos, myCloneList);
            } else {
                myInsideCloneBitmap = true;

                while (true) {
                    final long quantNo = pos >>> DB_ALLOCATION_QUANTUM_BITS;
                    int objBitSize = (int) (((size + DB_ALLOCATION_QUANTUM) - 1) >>> DB_ALLOCATION_QUANTUM_BITS);
                    int pageId = (int) (quantNo >>> (Page.pageSizeLog + 3));
                    int offs = (int) (quantNo & ((Page.pageSize * 8) - 1)) >> 3;
                    final int bitOffs = (int) quantNo & 7;
                    int oid = getBitmapPageId(pageId);
                    pos = getPos(oid);

                    if ((pos & DB_MODIFIED_FLAG) == 0) {
                        myDirtyPagesMap[oid >>> (DB_HANDLES_PER_PAGE_BITS + 5)] |=
                                1 << ((oid >>> DB_HANDLES_PER_PAGE_BITS) & 31);
                        allocate(Page.pageSize, oid);
                        cloneBitmap(pos & ~DB_FLAGS_MASK, Page.pageSize);
                    }

                    if (objBitSize > (8 - bitOffs)) {
                        objBitSize -= 8 - bitOffs;
                        offs += 1;

                        while ((objBitSize + (offs * 8)) > (Page.pageSize * 8)) {
                            oid = getBitmapPageId(++pageId);
                            pos = getPos(oid);

                            if ((pos & DB_MODIFIED_FLAG) == 0) {
                                myDirtyPagesMap[oid >>> (DB_HANDLES_PER_PAGE_BITS + 5)] |=
                                        1 << ((oid >>> DB_HANDLES_PER_PAGE_BITS) & 31);
                                allocate(Page.pageSize, oid);
                                cloneBitmap(pos & ~DB_FLAGS_MASK, Page.pageSize);
                            }

                            objBitSize -= (Page.pageSize - offs) * 8;
                            offs = 0;
                        }
                    }

                    if (myCloneList == null) {
                        break;
                    }

                    pos = myCloneList.pos;
                    size = Page.pageSize;
                    myCloneList = myCloneList.next;
                }

                myInsideCloneBitmap = false;
            }
        }
    }

    final void commitLocation() {
        myReservedChain = myReservedChain.next;
    }

    final void extend(final long size) {
        if (size > myHeader.myRootPage[1 - myCurrentIndex].mySize) {
            myHeader.myRootPage[1 - myCurrentIndex].mySize = size;
        }
    }

    final void fillBitmap(long adr, int len) {
        while (true) {
            final int off = (int) adr & (Page.pageSize - 1);
            final Page pg = myPagePool.putPage(adr - off);

            if ((Page.pageSize - off) >= len) {
                memset(pg, off, 0xFF, len);
                myPagePool.unfix(pg);
                break;
            } else {
                memset(pg, off, 0xFF, Page.pageSize - off);
                myPagePool.unfix(pg);
                adr += Page.pageSize - off;
                len -= Page.pageSize - off;
            }
        }
    }

    final ClassDescriptor findClassDescriptor(final Class<?> aClass) {
        return myClassDescMap.get(aClass);
    }

    final ClassDescriptor findClassDescriptor(final int oid) {
        return (ClassDescriptor) lookupObject(oid, ClassDescriptor.class);
    }

    final void free(final long pos, final long size) {
        synchronized (myObjectCache) {
            Assert.that((pos != 0) && ((pos & (DB_ALLOCATION_QUANTUM - 1)) == 0));
            final long quantNo = pos >>> DB_ALLOCATION_QUANTUM_BITS;
            int objBitSize = (int) (((size + DB_ALLOCATION_QUANTUM) - 1) >>> DB_ALLOCATION_QUANTUM_BITS);
            int pageId = (int) (quantNo >>> (Page.pageSizeLog + 3));
            int offs = (int) (quantNo & ((Page.pageSize * 8) - 1)) >> 3;
            Page pg = putBitmapPage(pageId);
            final int bitOffs = (int) quantNo & 7;

            myAllocatedDelta -= (long) objBitSize << DB_ALLOCATION_QUANTUM_BITS;
            usedSize -= (long) objBitSize << DB_ALLOCATION_QUANTUM_BITS;

            if (((pos & (Page.pageSize - 1)) == 0) && (size >= Page.pageSize)) {
                if ((pageId == myCurrentPageBitmapPage) && (offs < myCurrentPageBitmapOffset)) {
                    myCurrentPageBitmapOffset = offs;
                }
            }

            if ((pageId == myCurrentRecordBitmapPage) && (offs < myCurrentRecordBitmapOffset)) {
                myCurrentRecordBitmapOffset = offs;
            }

            myBitmapPageAvailableSpace[pageId] = Integer.MAX_VALUE;

            if (objBitSize > (8 - bitOffs)) {
                objBitSize -= 8 - bitOffs;
                pg.data[offs++] &= (1 << bitOffs) - 1;

                while ((objBitSize + (offs * 8)) > (Page.pageSize * 8)) {
                    memset(pg, offs, 0, Page.pageSize - offs);
                    myPagePool.unfix(pg);
                    pg = putBitmapPage(++pageId);
                    myBitmapPageAvailableSpace[pageId] = Integer.MAX_VALUE;
                    objBitSize -= (Page.pageSize - offs) * 8;
                    offs = 0;
                }

                while ((objBitSize -= 8) > 0) {
                    pg.data[offs++] = (byte) 0;
                }

                pg.data[offs] &= (byte) ~((1 << (objBitSize + 8)) - 1);
            } else {
                pg.data[offs] &= (byte) ~(((1 << objBitSize) - 1) << bitOffs);
            }

            myPagePool.unfix(pg);
        }
    }

    void freeId(final int oid) {
        synchronized (myObjectCache) {
            setPos(oid, ((long) myHeader.myRootPage[1 - myCurrentIndex].myFreeList << DB_FLAGS_BITS) |
                    DB_FREE_HANDLE_FLAG);
            myHeader.myRootPage[1 - myCurrentIndex].myFreeList = oid;
        }
    }

    final void freePage(final int oid) {
        final long pos = getPos(oid);

        Assert.that((pos & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) == DB_PAGE_OBJECT_FLAG);

        if ((pos & DB_MODIFIED_FLAG) != 0) {
            free(pos & ~DB_FLAGS_MASK, Page.pageSize);
        } else {
            cloneBitmap(pos & ~DB_FLAGS_MASK, Page.pageSize);
        }

        freeId(oid);
    }

    final byte[] get(final int oid) {
        final long pos = getPos(oid);

        if ((pos & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
            throw new StorageError(StorageError.INVALID_OID);
        }

        return myPagePool.get(pos & ~DB_FLAGS_MASK);
    }

    final Page getBitmapPage(final int i) {
        return getPage(getBitmapPageId(i));
    }

    final int getBitmapPageId(final int i) {
        return i < DB_BITMAP_PAGES ? DB_BITMAP_ID + i : (myHeader.myRootPage[1 - myCurrentIndex].myBitmapExtent + i) -
                myBitmapExtentBase;
    }

    final long getBitmapUsedSpace(int from, final int till) {
        long allocated = 0;

        while (from < till) {
            final Page pg = getGCPage(from);

            for (int j = 0; j < Page.pageSize; j++) {
                int mask = pg.data[j] & 0xFF;

                while (mask != 0) {
                    if ((mask & 1) != 0) {
                        allocated += DB_ALLOCATION_QUANTUM;
                    }

                    mask >>= 1;
                }
            }

            myPagePool.unfix(pg);
            from += 1;
        }

        return allocated;
    }

    final ClassDescriptor getClassDescriptor(final Class<?> aClass) {
        ClassDescriptor desc = findClassDescriptor(aClass);

        if (desc == null) {
            desc = new ClassDescriptor(this, aClass);
            registerClassDescriptor(desc);
        }

        return desc;
    }

    final Page getGCPage(final int oid) {
        return myPagePool.getPage(getGCPos(oid) & ~DB_FLAGS_MASK);
    }

    final long getGCPos(final int oid) {
        final Page pg = myPagePool.getPage(myHeader.myRootPage[myCurrentIndex].myIndex +
                ((long) (oid >>> DB_HANDLES_PER_PAGE_BITS) << Page.pageSizeLog));
        final long pos = Bytes.unpack8(pg.data, (oid & (DB_HANDLES_PER_PAGE - 1)) << 3);

        myPagePool.unfix(pg);
        return pos;
    }

    final Page getPage(final int oid) {
        final long pos = getPos(oid);

        if ((pos & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != DB_PAGE_OBJECT_FLAG) {
            throw new StorageError(StorageError.DELETED_OBJECT);
        }

        return myPagePool.getPage(pos & ~DB_FLAGS_MASK);
    }

    final long getPos(final int oid) {
        synchronized (myObjectCache) {
            if ((oid == 0) || (oid >= myCurrentIndexSize)) {
                throw new StorageError(StorageError.INVALID_OID);
            }

            if (myMulticlientSupport && !isInsideThreadTransaction()) {
                throw new StorageError(StorageError.NOT_IN_TRANSACTION);
            }

            final Page pg = myPagePool.getPage(myHeader.myRootPage[1 - myCurrentIndex].myIndex +
                    ((long) (oid >>> DB_HANDLES_PER_PAGE_BITS) << Page.pageSizeLog));
            final long pos = Bytes.unpack8(pg.data, (oid & (DB_HANDLES_PER_PAGE - 1)) << 3);

            myPagePool.unfix(pg);

            return pos;
        }
    }

    boolean isDeleted(final Object obj) {
        return obj instanceof IPersistent ? ((IPersistent) obj).isDeleted() : obj == null ? false : (myObjMap.getState(
                obj) & PinnedPersistent.DELETED) != 0;
    }

    boolean isLoaded(final Object obj) {
        if (obj instanceof IPersistent) {
            final IPersistent po = (IPersistent) obj;
            return !po.isRaw() && po.isPersistent();
        } else {
            synchronized (myObjMap) {
                final ObjectMap.Entry e = myObjMap.get(obj);
                return (e != null) && ((e.state & PinnedPersistent.RAW) == 0) && (e.oid != 0);
            }
        }
    }

    boolean isModified(final Object obj) {
        return obj instanceof IPersistent ? ((IPersistent) obj).isModified() : obj == null ? false : (myObjMap.getState(
                obj) & PinnedPersistent.DIRTY) != 0;
    }

    boolean isPersistent(final Object obj) {
        return getOid(obj) != 0;
    }

    boolean isRaw(final Object obj) {
        return obj instanceof IPersistent ? ((IPersistent) obj).isRaw() : obj == null ? false : (myObjMap.getState(
                obj) & PinnedPersistent.RAW) != 0;
    }

    final Object loadStub(final int oid, Object obj, final Class<?> cls) {
        final long pos = getPos(oid);

        if ((pos & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
            throw new StorageError(StorageError.DELETED_OBJECT);
        }

        final byte[] body = myPagePool.get(pos & ~DB_FLAGS_MASK);
        ClassDescriptor desc;
        final int typeOid = ObjectHeader.getType(body, 0);

        if (typeOid == 0) {
            desc = findClassDescriptor(cls);
        } else {
            desc = findClassDescriptor(typeOid);
        }

        // synchronized (objectCache)
        {
            if (obj == null) {
                obj = desc.customSerializable ? mySerializer.create(desc.cls) : desc.newInstance();
                myObjectCache.put(oid, obj);
            }

            assignOid(obj, oid, false);

            try {
                if (obj instanceof SelfSerializable) {
                    ((SelfSerializable) obj).unpack(new ByteArrayObjectInputStream(body, ObjectHeader.sizeof, obj,
                            recursiveLoading(obj), false));
                } else if (desc.customSerializable) {
                    mySerializer.unpack(obj, new ByteArrayObjectInputStream(body, ObjectHeader.sizeof, obj,
                            recursiveLoading(obj), false));
                } else {
                    unpackObject(obj, desc, recursiveLoading(obj), body, ObjectHeader.sizeof, obj);
                }
            } catch (final Exception x) {
                throw new StorageError(StorageError.ACCESS_VIOLATION, x);
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

    final synchronized Object lookupObject(final int oid, final Class<?> cls) {
        Object obj = myObjectCache.get(oid);

        if ((obj == null) || isRaw(obj)) {
            obj = loadStub(oid, obj, cls);
        }

        return obj;
    }

    final int markObject(final byte[] obj, int offs, final ClassDescriptor desc) {
        final ClassDescriptor.FieldDescriptor[] all = desc.allFields;

        for (final FieldDescriptor fd : all) {
            switch (fd.type) {
                case ClassDescriptor.tpBoolean:
                case ClassDescriptor.tpByte:
                    offs += 1;
                    continue;
                case ClassDescriptor.tpChar:
                case ClassDescriptor.tpShort:
                    offs += 2;
                    continue;
                case ClassDescriptor.tpInt:
                case ClassDescriptor.tpEnum:
                case ClassDescriptor.tpFloat:
                    offs += 4;
                    continue;
                case ClassDescriptor.tpLong:
                case ClassDescriptor.tpDouble:
                case ClassDescriptor.tpDate:
                    offs += 8;
                    continue;
                case ClassDescriptor.tpString:
                case ClassDescriptor.tpClass:
                    offs = Bytes.skipString(obj, offs);
                    continue;
                case ClassDescriptor.tpObject:
                    offs = markObjectReference(obj, offs);
                    continue;
                case ClassDescriptor.tpValue:
                    offs = markObject(obj, offs, fd.valueDesc);
                    continue;
                case ClassDescriptor.tpRaw: {
                    final int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    if (len > 0) {
                        offs += len;
                    } else if (len == (-2 - ClassDescriptor.tpObject)) {
                        markOid(Bytes.unpack4(obj, offs));
                        offs += 4;
                    } else if (len < -1) {
                        offs += ClassDescriptor.sizeof[-2 - len];
                    }
                    continue;
                }
                case ClassDescriptor.tpCustom:
                    try {
                        final ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(obj, offs, null, false,
                                true);
                        mySerializer.unpack(in);
                        offs = in.getPosition();
                    } catch (final IOException x) {
                        throw new StorageError(StorageError.ACCESS_VIOLATION, x);
                    }
                    continue;
                case ClassDescriptor.tpArrayOfByte:
                case ClassDescriptor.tpArrayOfBoolean: {
                    final int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    if (len > 0) {
                        offs += len;
                    } else if (len < -1) {
                        offs += ClassDescriptor.sizeof[-2 - len];
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfShort:
                case ClassDescriptor.tpArrayOfChar: {
                    final int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    if (len > 0) {
                        offs += len * 2;
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfInt:
                case ClassDescriptor.tpArrayOfEnum:
                case ClassDescriptor.tpArrayOfFloat: {
                    final int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    if (len > 0) {
                        offs += len * 4;
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfLong:
                case ClassDescriptor.tpArrayOfDouble:
                case ClassDescriptor.tpArrayOfDate: {
                    final int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    if (len > 0) {
                        offs += len * 8;
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfString: {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    while (--len >= 0) {
                        offs = Bytes.skipString(obj, offs);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfObject: {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    while (--len >= 0) {
                        offs = markObjectReference(obj, offs);
                    }
                    continue;
                }
                case ClassDescriptor.tpLink: {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    while (--len >= 0) {
                        markOid(Bytes.unpack4(obj, offs));
                        offs += 4;
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfValue: {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    final ClassDescriptor valueDesc = fd.valueDesc;
                    while (--len >= 0) {
                        offs = markObject(obj, offs, valueDesc);
                    }
                    continue;
                }
                case ClassDescriptor.tpArrayOfRaw: {
                    int len = Bytes.unpack4(obj, offs);
                    offs += 8;
                    while (--len >= 0) {
                        offs = markObjectReference(obj, offs);
                    }
                    continue;
                }
            }
        }

        return offs;
    }

    final int markObjectReference(final byte[] obj, int offs) {
        final int oid = Bytes.unpack4(obj, offs);
        offs += 4;

        if (oid < 0) {
            final int tid = -1 - oid;
            int len;
            switch (tid) {
                case ClassDescriptor.tpString:
                case ClassDescriptor.tpClass:
                    offs = Bytes.skipString(obj, offs);
                    break;
                case ClassDescriptor.tpArrayOfByte:
                    len = Bytes.unpack4(obj, offs);
                    offs += len + 4;
                    break;
                case ClassDescriptor.tpArrayOfObject:
                    len = Bytes.unpack4(obj, offs);
                    offs += 4;
                    for (int i = 0; i < len; i++) {
                        offs = markObjectReference(obj, offs);
                    }
                    break;
                case ClassDescriptor.tpArrayOfRaw:
                    len = Bytes.unpack4(obj, offs);
                    offs += 8;
                    for (int i = 0; i < len; i++) {
                        offs = markObjectReference(obj, offs);
                    }
                    break;
                case ClassDescriptor.tpCustom:
                    try {
                        final ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(obj, offs, null, false,
                                true);
                        mySerializer.unpack(in);
                        offs = in.getPosition();
                        break;
                    } catch (final IOException x) {
                        throw new StorageError(StorageError.ACCESS_VIOLATION, x);
                    }
                default:
                    if (tid >= ClassDescriptor.tpValueTypeBias) {
                        final int typeOid = -ClassDescriptor.tpValueTypeBias - oid;
                        final ClassDescriptor desc = findClassDescriptor(typeOid);
                        if (desc.isCollection) {
                            len = Bytes.unpack4(obj, offs);
                            offs += 4;
                            for (int i = 0; i < len; i++) {
                                offs = markObjectReference(obj, offs);
                            }
                        } else if (desc.isMap) {
                            len = Bytes.unpack4(obj, offs);
                            offs += 4;
                            for (int i = 0; i < len; i++) {
                                offs = markObjectReference(obj, offs);
                                offs = markObjectReference(obj, offs);
                            }
                        } else {
                            offs = markObject(obj, offs, desc);
                        }
                    } else {
                        offs += ClassDescriptor.sizeof[tid];
                    }
            }
        } else {
            markOid(oid);
        }

        return offs;
    }

    final void markOid(final int oid) {
        if (oid != 0) {
            final long pos = getGCPos(oid);

            if ((pos & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
                throw new StorageError(StorageError.INVALID_OID);
            }

            if (pos < myHeader.myRootPage[myCurrentIndex].mySize) {
                // object was not allocated by custom allocator
                final int bit = (int) (pos >>> DB_ALLOCATION_QUANTUM_BITS);

                if ((myBlackBitmap[bit >>> 5] & (1 << (bit & 31))) == 0) {
                    myGreyBitmap[bit >>> 5] |= 1 << (bit & 31);
                }
            }
        }
    }

    final byte[] packObject(final Object obj, final boolean finalized) {
        final ByteBuffer buf = new ByteBuffer(this, obj, finalized);
        int offs = ObjectHeader.sizeof;
        buf.extend(offs);
        final ClassDescriptor desc = getClassDescriptor(obj.getClass());

        try {
            if (obj instanceof SelfSerializable) {
                ((SelfSerializable) obj).pack(buf.getOutputStream());
                offs = buf.used;
            } else if (desc.customSerializable) {
                mySerializer.pack(obj, buf.getOutputStream());
                offs = buf.used;
            } else {
                offs = packObject(obj, desc, offs, buf);
            }
        } catch (final Exception x) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }

        ObjectHeader.setSize(buf.arr, 0, offs);
        ObjectHeader.setType(buf.arr, 0, desc.getOid());
        return buf.arr;
    }

    final int packObject(final Object obj, final ClassDescriptor desc, int offs, final ByteBuffer buf)
            throws Exception {
        final ClassDescriptor.FieldDescriptor[] flds = desc.allFields;

        for (final FieldDescriptor fd : flds) {
            final Field f = fd.field;

            switch (fd.type) {
                case ClassDescriptor.tpByte:
                    buf.extend(offs + 1);
                    buf.arr[offs++] = f.getByte(obj);
                    continue;
                case ClassDescriptor.tpBoolean:
                    buf.extend(offs + 1);
                    buf.arr[offs++] = (byte) (f.getBoolean(obj) ? 1 : 0);
                    continue;
                case ClassDescriptor.tpShort:
                    buf.extend(offs + 2);
                    Bytes.pack2(buf.arr, offs, f.getShort(obj));
                    offs += 2;
                    continue;
                case ClassDescriptor.tpChar:
                    buf.extend(offs + 2);
                    Bytes.pack2(buf.arr, offs, (short) f.getChar(obj));
                    offs += 2;
                    continue;
                case ClassDescriptor.tpInt:
                    buf.extend(offs + 4);
                    Bytes.pack4(buf.arr, offs, f.getInt(obj));
                    offs += 4;
                    continue;
                case ClassDescriptor.tpLong:
                    buf.extend(offs + 8);
                    Bytes.pack8(buf.arr, offs, f.getLong(obj));
                    offs += 8;
                    continue;
                case ClassDescriptor.tpFloat:
                    buf.extend(offs + 4);
                    Bytes.packF4(buf.arr, offs, f.getFloat(obj));
                    offs += 4;
                    continue;
                case ClassDescriptor.tpDouble:
                    buf.extend(offs + 8);
                    Bytes.packF8(buf.arr, offs, f.getDouble(obj));
                    offs += 8;
                    continue;
                case ClassDescriptor.tpEnum: {
                    final Enum e = (Enum) f.get(obj);
                    buf.extend(offs + 4);

                    if (e == null) {
                        Bytes.pack4(buf.arr, offs, -1);
                    } else {
                        Bytes.pack4(buf.arr, offs, e.ordinal());
                    }

                    offs += 4;
                    continue;
                }
                case ClassDescriptor.tpDate: {
                    buf.extend(offs + 8);
                    final Date d = (Date) f.get(obj);
                    final long msec = d == null ? -1 : d.getTime();
                    Bytes.pack8(buf.arr, offs, msec);
                    offs += 8;
                    continue;
                }
                case ClassDescriptor.tpString:
                    offs = buf.packString(offs, (String) f.get(obj));
                    continue;
                case ClassDescriptor.tpClass:
                    offs = buf.packString(offs, ClassDescriptor.getClassName((Class) f.get(obj)));
                    continue;
                case ClassDescriptor.tpObject:
                    offs = swizzle(buf, offs, f.get(obj));
                    continue;
                case ClassDescriptor.tpValue: {
                    final Object value = f.get(obj);

                    if (value == null) {
                        throw new StorageError(StorageError.NULL_VALUE, fd.fieldName);
                    } else if (value instanceof IPersistent) {
                        throw new StorageError(StorageError.SERIALIZE_PERSISTENT);
                    }

                    offs = packObject(value, fd.valueDesc, offs, buf);
                    continue;
                }
                case ClassDescriptor.tpRaw:
                    offs = packValue(f.get(obj), offs, buf);
                    continue;
                case ClassDescriptor.tpCustom: {
                    mySerializer.pack(f.get(obj), buf.getOutputStream());
                    offs = buf.size();
                    continue;
                }
                case ClassDescriptor.tpArrayOfByte: {
                    final byte[] arr = (byte[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4 + len);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        System.arraycopy(arr, 0, buf.arr, offs, len);
                        offs += len;
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfBoolean: {
                    final boolean[] arr = (boolean[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4 + len);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++, offs++) {
                            buf.arr[offs] = (byte) (arr[j] ? 1 : 0);
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfShort: {
                    final short[] arr = (short[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4 + (len * 2));
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            Bytes.pack2(buf.arr, offs, arr[j]);
                            offs += 2;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfChar: {
                    final char[] arr = (char[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4 + (len * 2));
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            Bytes.pack2(buf.arr, offs, (short) arr[j]);
                            offs += 2;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfInt: {
                    final int[] arr = (int[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4 + (len * 4));
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            Bytes.pack4(buf.arr, offs, arr[j]);
                            offs += 4;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfEnum: {
                    final Enum[] arr = (Enum[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4 + (len * 4));
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            if (arr[j] == null) {
                                Bytes.pack4(buf.arr, offs, -1);
                            } else {
                                Bytes.pack4(buf.arr, offs, arr[j].ordinal());
                            }

                            offs += 4;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfLong: {
                    final long[] arr = (long[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4 + (len * 8));
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            Bytes.pack8(buf.arr, offs, arr[j]);
                            offs += 8;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfFloat: {
                    final float[] arr = (float[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4 + (len * 4));
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            Bytes.packF4(buf.arr, offs, arr[j]);
                            offs += 4;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfDouble: {
                    final double[] arr = (double[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4 + (len * 8));
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            Bytes.packF8(buf.arr, offs, arr[j]);
                            offs += 8;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfDate: {
                    final Date[] arr = (Date[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4 + (len * 8));
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            final Date d = arr[j];
                            final long msec = d == null ? -1 : d.getTime();
                            Bytes.pack8(buf.arr, offs, msec);
                            offs += 8;
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfString: {
                    final String[] arr = (String[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            offs = buf.packString(offs, arr[j]);
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfObject: {
                    final Object[] arr = (Object[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4 + (len * 4));
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            offs = swizzle(buf, offs, arr[j]);
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpArrayOfValue: {
                    final Object[] arr = (Object[]) f.get(obj);

                    if (arr == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        final int len = arr.length;
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        final ClassDescriptor elemDesc = fd.valueDesc;

                        for (int j = 0; j < len; j++) {
                            final Object value = arr[j];

                            if (value == null) {
                                throw new StorageError(StorageError.NULL_VALUE, fd.fieldName);
                            }

                            offs = packObject(value, elemDesc, offs, buf);
                        }
                    }

                    continue;
                }
                case ClassDescriptor.tpLink: {
                    final LinkImpl link = (LinkImpl) f.get(obj);

                    if (link == null) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        link.owner = buf.parent;
                        final int len = link.size();
                        buf.extend(offs + 4 + (len * 4));
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            Bytes.pack4(buf.arr, offs, swizzle(link.getRaw(j), buf.finalized));
                            offs += 4;
                        }

                        if (!buf.finalized) {
                            link.unpin();
                        }
                    }

                    continue;
                }
            }
        }
        return offs;
    }

    final int packValue(final Object value, int offs, final ByteBuffer buf) throws Exception {
        if (value == null) {
            buf.extend(offs + 4);
            Bytes.pack4(buf.arr, offs, -1);
            offs += 4;
        } else if (value instanceof IPersistent) {
            buf.extend(offs + 8);
            Bytes.pack4(buf.arr, offs, -2 - ClassDescriptor.tpObject);
            Bytes.pack4(buf.arr, offs + 4, swizzle(value, buf.finalized));
            offs += 8;
        } else {
            final Class<?> c = value.getClass();

            if (c == Boolean.class) {
                buf.extend(offs + 5);
                Bytes.pack4(buf.arr, offs, -2 - ClassDescriptor.tpBoolean);
                buf.arr[offs + 4] = (byte) (((Boolean) value).booleanValue() ? 1 : 0);
                offs += 5;
            } else if (c == Character.class) {
                buf.extend(offs + 6);
                Bytes.pack4(buf.arr, offs, -2 - ClassDescriptor.tpChar);
                Bytes.pack2(buf.arr, offs + 4, (short) ((Character) value).charValue());
                offs += 6;
            } else if (c == Byte.class) {
                buf.extend(offs + 5);
                Bytes.pack4(buf.arr, offs, -2 - ClassDescriptor.tpByte);
                buf.arr[offs + 4] = ((Byte) value).byteValue();
                offs += 5;
            } else if (c == Short.class) {
                buf.extend(offs + 6);
                Bytes.pack4(buf.arr, offs, -2 - ClassDescriptor.tpShort);
                Bytes.pack2(buf.arr, offs + 4, ((Short) value).shortValue());
                offs += 6;
            } else if (c == Integer.class) {
                buf.extend(offs + 8);
                Bytes.pack4(buf.arr, offs, -2 - ClassDescriptor.tpInt);
                Bytes.pack4(buf.arr, offs + 4, ((Integer) value).intValue());
                offs += 8;
            } else if (c == Long.class) {
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -2 - ClassDescriptor.tpLong);
                Bytes.pack8(buf.arr, offs + 4, ((Long) value).longValue());
                offs += 12;
            } else if (c == Float.class) {
                buf.extend(offs + 8);
                Bytes.pack4(buf.arr, offs, -2 - ClassDescriptor.tpFloat);
                Bytes.pack4(buf.arr, offs + 4, Float.floatToIntBits(((Float) value).floatValue()));
                offs += 8;
            } else if (c == Double.class) {
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -2 - ClassDescriptor.tpDouble);
                Bytes.pack8(buf.arr, offs + 4, Double.doubleToLongBits(((Double) value).doubleValue()));
                offs += 12;
            } else if (c == Date.class) {
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -2 - ClassDescriptor.tpDate);
                Bytes.pack8(buf.arr, offs + 4, ((Date) value).getTime());
                offs += 12;
            } else {
                final ByteArrayOutputStream bout = new ByteArrayOutputStream();
                final ObjectOutputStream out = (myClassLoaderMap != null) && ((myCompatibilityMode &
                        CLASS_LOADER_SERIALIZATION_COMPATIBILITY_MODE) == 0)
                                ? (ObjectOutputStream) new AnnotatedPersistentObjectOutputStream(bout)
                                : (ObjectOutputStream) new PersistentObjectOutputStream(bout);
                out.writeObject(value);
                out.close();
                final byte[] arr = bout.toByteArray();
                final int len = arr.length;
                buf.extend(offs + 4 + len);
                Bytes.pack4(buf.arr, offs, len);
                offs += 4;
                System.arraycopy(arr, 0, buf.arr, offs, len);
                offs += len;
            }
        }

        return offs;
    }

    final Page putBitmapPage(final int i) {
        return putPage(getBitmapPageId(i));
    }

    final Page putPage(final int oid) {
        synchronized (myObjectCache) {
            long pos = getPos(oid);

            if ((pos & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != DB_PAGE_OBJECT_FLAG) {
                throw new StorageError(StorageError.DELETED_OBJECT);
            }

            if ((pos & DB_MODIFIED_FLAG) == 0) {
                myDirtyPagesMap[oid >>> (DB_HANDLES_PER_PAGE_BITS + 5)] |= 1 << ((oid >>> DB_HANDLES_PER_PAGE_BITS) &
                        31);
                allocate(Page.pageSize, oid);
                cloneBitmap(pos & ~DB_FLAGS_MASK, Page.pageSize);
                pos = getPos(oid);
            }

            myModified = true;

            return myPagePool.putPage(pos & ~DB_FLAGS_MASK);
        }
    }

    boolean recursiveLoading(final Object obj) {
        if (myRecursiveLoadingPolicyDefined) {
            synchronized (myRecursiveLoadingPolicy) {
                Class<?> type = obj.getClass();

                do {
                    final Object enabled = myRecursiveLoadingPolicy.get(type);

                    if (enabled != null) {
                        return ((Boolean) enabled).booleanValue();
                    }
                } while ((type = type.getSuperclass()) != null);
            }
        }

        return obj instanceof IPersistent ? ((IPersistent) obj).recursiveLoading() : true;
    }

    final void registerClassDescriptor(final ClassDescriptor desc) {
        myClassDescMap.put(desc.cls, desc);
        desc.next = myDescriptorList;
        myDescriptorList = desc;
        checkIfFinal(desc);
        storeObject0(desc, false);
        myHeader.myRootPage[1 - myCurrentIndex].myClassDescriptorList = desc.getOid();
        myModified = true;
    }

    void reloadScheme() {
        myClassDescMap.clear();
        customAllocatorMap = null;
        customAllocatorList = null;
        defaultAllocator = new DefaultAllocator(this);
        final int descListOid = myHeader.myRootPage[1 - myCurrentIndex].myClassDescriptorList;

        final ClassDescriptor metaclass = new ClassDescriptor(this, ClassDescriptor.class);
        final ClassDescriptor metafield = new ClassDescriptor(this, ClassDescriptor.FieldDescriptor.class);

        myClassDescMap.put(ClassDescriptor.class, metaclass);
        myClassDescMap.put(ClassDescriptor.FieldDescriptor.class, metafield);

        if (((myCompatibilityMode & IBM_JAVA5_COMPATIBILITY_MODE) != 0) && (getDatabaseFormatVersion() == 1)) {
            ClassDescriptor.FieldDescriptor tmp = metaclass.allFields[2];
            metaclass.allFields[2] = metaclass.allFields[3];
            metaclass.allFields[3] = metaclass.allFields[4];
            metaclass.allFields[4] = tmp;
            tmp = metafield.allFields[2];
            metafield.allFields[2] = metafield.allFields[3];
            metafield.allFields[3] = tmp;
        }

        if (descListOid != 0) {
            ClassDescriptor desc;
            myDescriptorList = findClassDescriptor(descListOid);

            for (desc = myDescriptorList; desc != null; desc = desc.next) {
                desc.load();
            }

            for (desc = myDescriptorList; desc != null; desc = desc.next) {
                if (findClassDescriptor(desc.cls) == desc) {
                    desc.resolve();
                }

                if (desc.allocator != null) {
                    if (customAllocatorMap == null) {
                        customAllocatorMap = new HashMap();
                        customAllocatorList = new ArrayList();
                    }

                    final CustomAllocator allocator = desc.allocator;
                    allocator.load();
                    customAllocatorMap.put(desc.cls, allocator);
                    customAllocatorList.add(allocator);
                    reserveLocation(allocator.getSegmentBase(), allocator.getSegmentSize());
                }

                checkIfFinal(desc);
            }
        } else {
            myDescriptorList = null;
        }
    }

    final void reserveLocation(final long pos, final long size) {
        final Location location = new Location();

        location.pos = pos;
        location.size = size;
        location.next = myReservedChain;
        myReservedChain = location;
    }

    final void setDirty() {
        myModified = true;

        if (!myHeader.myDbIsDirty) {
            myHeader.myDbIsDirty = true;
            final Page pg = myPagePool.putPage(0);
            myHeader.pack(pg.data);
            myPagePool.flush();
            myPagePool.unfix(pg);
        }
    }

    final void setPos(final int oid, final long pos) {
        synchronized (myObjectCache) {
            myDirtyPagesMap[oid >>> (DB_HANDLES_PER_PAGE_BITS + 5)] |= 1 << ((oid >>> DB_HANDLES_PER_PAGE_BITS) & 31);
            final Page pg = myPagePool.putPage(myHeader.myRootPage[1 - myCurrentIndex].myIndex +
                    ((long) (oid >>> DB_HANDLES_PER_PAGE_BITS) << Page.pageSizeLog));
            Bytes.pack8(pg.data, (oid & (DB_HANDLES_PER_PAGE - 1)) << 3, pos);
            myPagePool.unfix(pg);
        }
    }

    final int skipObjectReference(final byte[] obj, int offs) throws Exception {
        final int oid = Bytes.unpack4(obj, offs);
        int len;
        offs += 4;
        if (oid < 0) {
            final int tid = -1 - oid;

            switch (tid) {
                case ClassDescriptor.tpString:
                case ClassDescriptor.tpClass:
                    offs = Bytes.skipString(obj, offs);
                    break;
                case ClassDescriptor.tpArrayOfByte:
                    len = Bytes.unpack4(obj, offs);
                    offs += len + 4;
                    break;
                case ClassDescriptor.tpArrayOfObject:
                    len = Bytes.unpack4(obj, offs);
                    offs += 4;

                    for (int i = 0; i < len; i++) {
                        offs = skipObjectReference(obj, offs);
                    }

                    break;
                case ClassDescriptor.tpArrayOfRaw:
                    len = Bytes.unpack4(obj, offs);
                    offs += 8;

                    for (int i = 0; i < len; i++) {
                        offs = skipObjectReference(obj, offs);
                    }

                    break;
                default:
                    if (tid >= ClassDescriptor.tpValueTypeBias) {
                        final int typeOid = -ClassDescriptor.tpValueTypeBias - oid;
                        final ClassDescriptor desc = findClassDescriptor(typeOid);

                        if (desc.isCollection) {
                            len = Bytes.unpack4(obj, offs);
                            offs += 4;

                            for (int i = 0; i < len; i++) {
                                offs = skipObjectReference(obj, offs);
                            }
                        } else if (desc.isMap) {
                            len = Bytes.unpack4(obj, offs);
                            offs += 4;

                            for (int i = 0; i < len; i++) {
                                offs = skipObjectReference(obj, offs);
                                offs = skipObjectReference(obj, offs);
                            }
                        } else {
                            offs = unpackObject(null, findClassDescriptor(typeOid), false, obj, offs, null);
                        }
                    } else {
                        offs += ClassDescriptor.sizeof[tid];
                    }
            }
        }

        return offs;
    }

    final int swizzle(final ByteBuffer buf, int offs, final Object obj) throws Exception {
        if ((obj instanceof IPersistent) || (obj == null)) {
            offs = buf.packI4(offs, swizzle(obj, buf.finalized));
        } else {
            final Class<?> t = obj.getClass();

            if (t == Boolean.class) {
                buf.extend(offs + 5);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpBoolean);
                buf.arr[offs + 4] = (byte) (((Boolean) obj).booleanValue() ? 1 : 0);
                offs += 5;
            } else if (t == Character.class) {
                buf.extend(offs + 6);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpChar);
                Bytes.pack2(buf.arr, offs + 4, (short) ((Character) obj).charValue());
                offs += 6;
            } else if (t == Byte.class) {
                buf.extend(offs + 5);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpByte);
                buf.arr[offs + 4] = ((Byte) obj).byteValue();
                offs += 5;
            } else if (t == Short.class) {
                buf.extend(offs + 6);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpShort);
                Bytes.pack2(buf.arr, offs + 4, ((Short) obj).shortValue());
                offs += 6;
            } else if (t == Integer.class) {
                buf.extend(offs + 8);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpInt);
                Bytes.pack4(buf.arr, offs + 4, ((Integer) obj).intValue());
                offs += 8;
            } else if (t == Long.class) {
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpLong);
                Bytes.pack8(buf.arr, offs + 4, ((Long) obj).longValue());
                offs += 12;
            } else if (t == Float.class) {
                buf.extend(offs + 8);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpFloat);
                Bytes.packF4(buf.arr, offs + 4, ((Float) obj).floatValue());
                offs += 8;
            } else if (t == Double.class) {
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpDouble);
                Bytes.packF8(buf.arr, offs + 4, ((Double) obj).doubleValue());
                offs += 12;
            } else if (t == Date.class) {
                buf.extend(offs + 12);
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpDate);
                Bytes.pack8(buf.arr, offs + 4, ((Date) obj).getTime());
                offs += 12;
            } else if (t == String.class) {
                offs = buf.packI4(offs, -1 - ClassDescriptor.tpString);
                offs = buf.packString(offs, (String) obj);
            } else if (t == Class.class) {
                offs = buf.packI4(offs, -1 - ClassDescriptor.tpClass);
                offs = buf.packString(offs, ClassDescriptor.getClassName((Class) obj));
            } else if (obj instanceof LinkImpl) {
                final LinkImpl link = (LinkImpl) obj;
                link.owner = buf.parent;
                final int len = link.size();
                buf.extend(offs + 8 + (len * 4));
                Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpLink);
                offs += 4;
                Bytes.pack4(buf.arr, offs, len);
                offs += 4;

                for (int j = 0; j < len; j++) {
                    Bytes.pack4(buf.arr, offs, swizzle(link.getRaw(j), buf.finalized));
                    offs += 4;
                }
            } else if ((obj instanceof Collection) && (!mySerializeSystemCollections || t.getName().startsWith(
                    "java.util."))) {
                final ClassDescriptor valueDesc = getClassDescriptor(obj.getClass());
                offs = buf.packI4(offs, -ClassDescriptor.tpValueTypeBias - valueDesc.getOid());
                final Collection c = (Collection) obj;
                offs = buf.packI4(offs, c.size());

                for (final Object elem : c) {
                    offs = swizzle(buf, offs, elem);
                }
            } else if ((obj instanceof Map) && (!mySerializeSystemCollections || t.getName().startsWith(
                    "java.util."))) {
                final ClassDescriptor valueDesc = getClassDescriptor(obj.getClass());
                offs = buf.packI4(offs, -ClassDescriptor.tpValueTypeBias - valueDesc.getOid());
                final Map map = (Map) obj;
                offs = buf.packI4(offs, map.size());

                for (final Object entry : map.entrySet()) {
                    final Map.Entry e = (Map.Entry) entry;
                    offs = swizzle(buf, offs, e.getKey());
                    offs = swizzle(buf, offs, e.getValue());
                }
            } else if (obj instanceof IValue) {
                final ClassDescriptor valueDesc = getClassDescriptor(obj.getClass());
                offs = buf.packI4(offs, -ClassDescriptor.tpValueTypeBias - valueDesc.getOid());
                offs = packObject(obj, valueDesc, offs, buf);
            } else if (t.isArray()) {
                final Class<?> elemType = t.getComponentType();

                if (elemType == byte.class) {
                    final byte[] arr = (byte[]) obj;
                    final int len = arr.length;
                    buf.extend(offs + len + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfByte);
                    Bytes.pack4(buf.arr, offs + 4, len);
                    System.arraycopy(arr, 0, buf.arr, offs + 8, len);
                    offs += 8 + len;
                } else if (elemType == boolean.class) {
                    final boolean[] arr = (boolean[]) obj;
                    final int len = arr.length;
                    buf.extend(offs + len + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfBoolean);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;

                    for (int i = 0; i < len; i++) {
                        buf.arr[offs++] = (byte) (arr[i] ? 1 : 0);
                    }
                } else if (elemType == char.class) {
                    final char[] arr = (char[]) obj;
                    final int len = arr.length;
                    buf.extend(offs + (len * 2) + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfChar);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;

                    for (int i = 0; i < len; i++) {
                        Bytes.pack2(buf.arr, offs, (short) arr[i]);
                        offs += 2;
                    }
                } else if (elemType == short.class) {
                    final short[] arr = (short[]) obj;
                    final int len = arr.length;
                    buf.extend(offs + (len * 2) + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfShort);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;

                    for (int i = 0; i < len; i++) {
                        Bytes.pack2(buf.arr, offs, arr[i]);
                        offs += 2;
                    }
                } else if (elemType == int.class) {
                    final int[] arr = (int[]) obj;
                    final int len = arr.length;
                    buf.extend(offs + (len * 4) + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfInt);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;

                    for (int i = 0; i < len; i++) {
                        Bytes.pack4(buf.arr, offs, arr[i]);
                        offs += 4;
                    }
                } else if (elemType == long.class) {
                    final long[] arr = (long[]) obj;
                    final int len = arr.length;
                    buf.extend(offs + (len * 8) + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfLong);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;

                    for (int i = 0; i < len; i++) {
                        Bytes.pack8(buf.arr, offs, arr[i]);
                        offs += 8;
                    }
                } else if (elemType == float.class) {
                    final float[] arr = (float[]) obj;
                    final int len = arr.length;
                    buf.extend(offs + (len * 4) + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfFloat);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;

                    for (int i = 0; i < len; i++) {
                        Bytes.packF4(buf.arr, offs, arr[i]);
                        offs += 4;
                    }
                } else if (elemType == double.class) {
                    final double[] arr = (double[]) obj;
                    final int len = arr.length;
                    buf.extend(offs + (len * 8) + 8);
                    Bytes.pack4(buf.arr, offs, -1 - ClassDescriptor.tpArrayOfLong);
                    offs += 4;
                    Bytes.pack4(buf.arr, offs, len);
                    offs += 4;

                    for (int i = 0; i < len; i++) {
                        Bytes.packF8(buf.arr, offs, arr[i]);
                        offs += 8;
                    }
                } else if (elemType == Object.class) {
                    offs = buf.packI4(offs, -1 - ClassDescriptor.tpArrayOfObject);
                    final Object[] arr = (Object[]) obj;
                    final int len = arr.length;
                    offs = buf.packI4(offs, len);

                    for (int i = 0; i < len; i++) {
                        offs = swizzle(buf, offs, arr[i]);
                    }
                } else {
                    offs = buf.packI4(offs, -1 - ClassDescriptor.tpArrayOfRaw);
                    final int len = Array.getLength(obj);
                    offs = buf.packI4(offs, len);

                    if (elemType.equals(Comparable.class)) {
                        offs = buf.packI4(offs, -1);
                    } else {
                        final ClassDescriptor desc = getClassDescriptor(elemType);
                        offs = buf.packI4(offs, desc.getOid());
                    }

                    for (int i = 0; i < len; i++) {
                        offs = swizzle(buf, offs, Array.get(obj, i));
                    }
                }
            } else if ((mySerializer != null) && mySerializer.isEmbedded(obj)) {
                buf.packI4(offs, -1 - ClassDescriptor.tpCustom);
                mySerializer.pack(obj, buf.getOutputStream());
                offs = buf.used;
            } else {
                offs = buf.packI4(offs, swizzle(obj, buf.finalized));
            }
        }

        return offs;
    }

    void unassignOid(final Object obj) {
        if (obj instanceof IPersistent) {
            ((IPersistent) obj).unassignOid();
        } else {
            myObjMap.remove(obj);
        }
    }

    final int unpackObject(final Object obj, final ClassDescriptor desc, final boolean recursiveLoading,
            final byte[] body, int offs, final Object parent) throws Exception {
        final ClassDescriptor.FieldDescriptor[] all = desc.allFields;
        final ReflectionProvider provider = ClassDescriptor.getReflectionProvider();
        int len;

        for (final FieldDescriptor fd : all) {
            final Field f = fd.field;

            if ((f == null) || (obj == null)) {
                switch (fd.type) {
                    case ClassDescriptor.tpBoolean:
                    case ClassDescriptor.tpByte:
                        offs += 1;
                        continue;
                    case ClassDescriptor.tpChar:
                    case ClassDescriptor.tpShort:
                        offs += 2;
                        continue;
                    case ClassDescriptor.tpInt:
                    case ClassDescriptor.tpFloat:
                    case ClassDescriptor.tpEnum:
                        offs += 4;
                        continue;
                    case ClassDescriptor.tpObject:
                        offs = skipObjectReference(body, offs);
                        continue;
                    case ClassDescriptor.tpLong:
                    case ClassDescriptor.tpDouble:
                    case ClassDescriptor.tpDate:
                        offs += 8;
                        continue;
                    case ClassDescriptor.tpString:
                    case ClassDescriptor.tpClass:
                        offs = Bytes.skipString(body, offs);
                        continue;
                    case ClassDescriptor.tpValue:
                        offs = unpackObject(null, fd.valueDesc, recursiveLoading, body, offs, parent);
                        continue;
                    case ClassDescriptor.tpRaw:
                    case ClassDescriptor.tpArrayOfByte:
                    case ClassDescriptor.tpArrayOfBoolean:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len > 0) {
                            offs += len;
                        } else if (len < -1) {
                            offs += ClassDescriptor.sizeof[-2 - len];
                        }

                        continue;
                    case ClassDescriptor.tpCustom: {
                        final ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(body, offs, parent,
                                recursiveLoading, false);
                        mySerializer.unpack(in);
                        offs = in.getPosition();
                        continue;
                    }
                    case ClassDescriptor.tpArrayOfShort:
                    case ClassDescriptor.tpArrayOfChar:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len > 0) {
                            offs += len * 2;
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfObject:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        for (int j = 0; j < len; j++) {
                            offs = skipObjectReference(body, offs);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfInt:
                    case ClassDescriptor.tpArrayOfEnum:
                    case ClassDescriptor.tpArrayOfFloat:
                    case ClassDescriptor.tpLink:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len > 0) {
                            offs += len * 4;
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfLong:
                    case ClassDescriptor.tpArrayOfDouble:
                    case ClassDescriptor.tpArrayOfDate:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len > 0) {
                            offs += len * 8;
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfString:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len > 0) {
                            for (int j = 0; j < len; j++) {
                                offs = Bytes.skipString(body, offs);
                            }
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfValue:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len > 0) {
                            final ClassDescriptor valueDesc = fd.valueDesc;

                            for (int j = 0; j < len; j++) {
                                offs = unpackObject(null, valueDesc, recursiveLoading, body, offs, parent);
                            }
                        }

                        continue;
                }
            } else if (offs < body.length) {
                switch (fd.type) {
                    case ClassDescriptor.tpBoolean:
                        provider.setBoolean(f, obj, body[offs++] != 0);
                        continue;
                    case ClassDescriptor.tpByte:
                        provider.setByte(f, obj, body[offs++]);
                        continue;
                    case ClassDescriptor.tpChar:
                        provider.setChar(f, obj, (char) Bytes.unpack2(body, offs));
                        offs += 2;
                        continue;
                    case ClassDescriptor.tpShort:
                        provider.setShort(f, obj, Bytes.unpack2(body, offs));
                        offs += 2;
                        continue;
                    case ClassDescriptor.tpInt:
                        provider.setInt(f, obj, Bytes.unpack4(body, offs));
                        offs += 4;
                        continue;
                    case ClassDescriptor.tpLong:
                        provider.setLong(f, obj, Bytes.unpack8(body, offs));
                        offs += 8;
                        continue;
                    case ClassDescriptor.tpFloat:
                        provider.setFloat(f, obj, Bytes.unpackF4(body, offs));
                        offs += 4;
                        continue;
                    case ClassDescriptor.tpDouble:
                        provider.setDouble(f, obj, Bytes.unpackF8(body, offs));
                        offs += 8;
                        continue;
                    case ClassDescriptor.tpEnum: {
                        final int index = Bytes.unpack4(body, offs);

                        if (index >= 0) {
                            provider.set(f, obj, fd.field.getType().getEnumConstants()[index]);
                        } else {
                            provider.set(f, obj, null);
                        }

                        offs += 4;
                        continue;
                    }
                    case ClassDescriptor.tpString: {
                        final ArrayPos pos = new ArrayPos(body, offs);
                        provider.set(f, obj, Bytes.unpackString(pos, myEncoding));
                        offs = pos.offs;
                        continue;
                    }
                    case ClassDescriptor.tpClass: {
                        final ArrayPos pos = new ArrayPos(body, offs);
                        final Class<?> cls = ClassDescriptor.loadClass(this, Bytes.unpackString(pos, myEncoding));
                        provider.set(f, obj, cls);
                        offs = pos.offs;
                        continue;
                    }
                    case ClassDescriptor.tpDate: {
                        final long msec = Bytes.unpack8(body, offs);
                        offs += 8;
                        Date date = null;

                        if (msec >= 0) {
                            date = new Date(msec);
                        }

                        provider.set(f, obj, date);
                        continue;
                    }
                    case ClassDescriptor.tpObject: {
                        final ArrayPos pos = new ArrayPos(body, offs);
                        provider.set(f, obj, unswizzle(pos, f.getType(), parent, recursiveLoading));
                        offs = pos.offs;
                        continue;
                    }
                    case ClassDescriptor.tpValue: {
                        final Object value = fd.valueDesc.newInstance();
                        offs = unpackObject(value, fd.valueDesc, recursiveLoading, body, offs, parent);
                        provider.set(f, obj, value);
                        continue;
                    }
                    case ClassDescriptor.tpRaw:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len >= 0) {
                            final ByteArrayInputStream bin = new ByteArrayInputStream(body, offs, len);
                            final ObjectInputStream in = new PersistentObjectInputStream(bin);
                            provider.set(f, obj, in.readObject());
                            in.close();
                            offs += len;
                        } else if (len < 0) {
                            Object val = null;

                            switch (-2 - len) {
                                case ClassDescriptor.tpBoolean:
                                    val = Boolean.valueOf(body[offs++] != 0);
                                    break;
                                case ClassDescriptor.tpByte:
                                    val = new Byte(body[offs++]);
                                    break;
                                case ClassDescriptor.tpChar:
                                    val = new Character((char) Bytes.unpack2(body, offs));
                                    offs += 2;
                                    break;
                                case ClassDescriptor.tpShort:
                                    val = new Short(Bytes.unpack2(body, offs));
                                    offs += 2;
                                    break;
                                case ClassDescriptor.tpInt:
                                    val = new Integer(Bytes.unpack4(body, offs));
                                    offs += 4;
                                    break;
                                case ClassDescriptor.tpLong:
                                    val = new Long(Bytes.unpack8(body, offs));
                                    offs += 8;
                                    break;
                                case ClassDescriptor.tpFloat:
                                    val = new Float(Float.intBitsToFloat(Bytes.unpack4(body, offs)));
                                    offs += 4;
                                    break;
                                case ClassDescriptor.tpDouble:
                                    val = new Double(Double.longBitsToDouble(Bytes.unpack8(body, offs)));
                                    offs += 8;
                                    break;
                                case ClassDescriptor.tpDate:
                                    val = new Date(Bytes.unpack8(body, offs));
                                    offs += 8;
                                    break;
                                case ClassDescriptor.tpObject:
                                    val = unswizzle(Bytes.unpack4(body, offs), Persistent.class, recursiveLoading);
                                    offs += 4;
                            }

                            provider.set(f, obj, val);
                        }

                        continue;
                    case ClassDescriptor.tpCustom: {
                        final ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(body, offs, parent,
                                recursiveLoading, false);
                        mySerializer.unpack(in);
                        provider.set(f, obj, mySerializer.unpack(in));
                        offs = in.getPosition();
                        continue;
                    }
                    case ClassDescriptor.tpArrayOfByte:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final byte[] arr = new byte[len];
                            System.arraycopy(body, offs, arr, 0, len);
                            offs += len;
                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfBoolean:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final boolean[] arr = new boolean[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = body[offs++] != 0;
                            }

                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfShort:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final short[] arr = new short[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpack2(body, offs);
                                offs += 2;
                            }

                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfChar:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final char[] arr = new char[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = (char) Bytes.unpack2(body, offs);
                                offs += 2;
                            }

                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfInt:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final int[] arr = new int[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpack4(body, offs);
                                offs += 4;
                            }

                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfEnum:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            f.set(obj, null);
                        } else {
                            final Class<?> elemType = f.getType().getComponentType();
                            final Enum[] enumConstants = (Enum[]) elemType.getEnumConstants();
                            final Enum[] arr = (Enum[]) Array.newInstance(elemType, len);

                            for (int j = 0; j < len; j++) {
                                final int index = Bytes.unpack4(body, offs);

                                if (index >= 0) {
                                    arr[j] = enumConstants[index];
                                }

                                offs += 4;
                            }

                            f.set(obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfLong:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final long[] arr = new long[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpack8(body, offs);
                                offs += 8;
                            }

                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfFloat:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final float[] arr = new float[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpackF4(body, offs);
                                offs += 4;
                            }

                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfDouble:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final double[] arr = new double[len];

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpackF8(body, offs);
                                offs += 8;
                            }

                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfDate:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final Date[] arr = new Date[len];

                            for (int j = 0; j < len; j++) {
                                final long msec = Bytes.unpack8(body, offs);
                                offs += 8;

                                if (msec >= 0) {
                                    arr[j] = new Date(msec);
                                }
                            }

                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfString:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final String[] arr = new String[len];
                            final ArrayPos pos = new ArrayPos(body, offs);

                            for (int j = 0; j < len; j++) {
                                arr[j] = Bytes.unpackString(pos, myEncoding);
                            }

                            offs = pos.offs;
                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfObject:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final Class<?> elemType = f.getType().getComponentType();
                            final Object[] arr = (Object[]) Array.newInstance(elemType, len);
                            final ArrayPos pos = new ArrayPos(body, offs);

                            for (int j = 0; j < len; j++) {
                                arr[j] = unswizzle(pos, elemType, parent, recursiveLoading);
                            }

                            offs = pos.offs;
                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpArrayOfValue:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final Class<?> elemType = f.getType().getComponentType();
                            final Object[] arr = (Object[]) Array.newInstance(elemType, len);
                            final ClassDescriptor valueDesc = fd.valueDesc;

                            for (int j = 0; j < len; j++) {
                                final Object value = valueDesc.newInstance();
                                offs = unpackObject(value, valueDesc, recursiveLoading, body, offs, parent);
                                arr[j] = value;
                            }

                            provider.set(f, obj, arr);
                        }

                        continue;
                    case ClassDescriptor.tpLink:
                        len = Bytes.unpack4(body, offs);
                        offs += 4;

                        if (len < 0) {
                            provider.set(f, obj, null);
                        } else {
                            final Object[] arr = new Object[len];

                            for (int j = 0; j < len; j++) {
                                final int elemOid = Bytes.unpack4(body, offs);
                                offs += 4;

                                if (elemOid != 0) {
                                    arr[j] = new PersistentStub(this, elemOid);
                                }
                            }

                            provider.set(f, obj, new LinkImpl(this, arr, parent));
                        }
                }
            }
        }

        return offs;
    }

    final Object unswizzle(final ArrayPos obj, final Class<?> cls, final Object parent, final boolean recursiveLoading)
            throws Exception {
        final byte[] body = obj.body;
        int offs = obj.offs;
        final int oid = Bytes.unpack4(body, offs);
        offs += 4;
        Object val;

        if (oid < 0) {
            switch (-1 - oid) {
                case ClassDescriptor.tpBoolean:
                    val = body[offs++] != 0;
                    break;
                case ClassDescriptor.tpByte:
                    val = body[offs++];
                    break;
                case ClassDescriptor.tpChar:
                    val = (char) Bytes.unpack2(body, offs);
                    offs += 2;
                    break;
                case ClassDescriptor.tpShort:
                    val = Bytes.unpack2(body, offs);
                    offs += 2;
                    break;
                case ClassDescriptor.tpInt:
                    val = Bytes.unpack4(body, offs);
                    offs += 4;
                    break;
                case ClassDescriptor.tpLong:
                    val = Bytes.unpack8(body, offs);
                    offs += 8;
                    break;
                case ClassDescriptor.tpFloat:
                    val = Bytes.unpackF4(body, offs);
                    offs += 4;
                    break;
                case ClassDescriptor.tpDouble:
                    val = Bytes.unpackF8(body, offs);
                    offs += 8;
                    break;
                case ClassDescriptor.tpDate:
                    val = new Date(Bytes.unpack8(body, offs));
                    offs += 8;
                    break;
                case ClassDescriptor.tpString:
                    obj.offs = offs;
                    return Bytes.unpackString(obj, myEncoding);
                case ClassDescriptor.tpClass:
                    obj.offs = offs;
                    return ClassDescriptor.loadClass(this, Bytes.unpackString(obj, myEncoding));
                case ClassDescriptor.tpLink: {
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

                    val = new LinkImpl(this, arr, parent);
                    break;
                }
                case ClassDescriptor.tpArrayOfByte: {
                    final int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    final byte[] arr = new byte[len];
                    System.arraycopy(body, offs, arr, 0, len);
                    offs += len;
                    val = arr;
                    break;
                }
                case ClassDescriptor.tpArrayOfBoolean: {
                    final int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    final boolean[] arr = new boolean[len];

                    for (int j = 0; j < len; j++) {
                        arr[j] = body[offs++] != 0;
                    }

                    val = arr;
                    break;
                }
                case ClassDescriptor.tpArrayOfShort: {
                    final int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    final short[] arr = new short[len];

                    for (int j = 0; j < len; j++) {
                        arr[j] = Bytes.unpack2(body, offs);
                        offs += 2;
                    }

                    val = arr;
                    break;
                }
                case ClassDescriptor.tpArrayOfChar: {
                    final int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    final char[] arr = new char[len];

                    for (int j = 0; j < len; j++) {
                        arr[j] = (char) Bytes.unpack2(body, offs);
                        offs += 2;
                    }

                    val = arr;
                    break;
                }
                case ClassDescriptor.tpArrayOfInt: {
                    final int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    final int[] arr = new int[len];

                    for (int j = 0; j < len; j++) {
                        arr[j] = Bytes.unpack4(body, offs);
                        offs += 4;
                    }

                    val = arr;
                    break;
                }
                case ClassDescriptor.tpArrayOfLong: {
                    final int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    final long[] arr = new long[len];

                    for (int j = 0; j < len; j++) {
                        arr[j] = Bytes.unpack8(body, offs);
                        offs += 8;
                    }

                    val = arr;
                    break;
                }
                case ClassDescriptor.tpArrayOfFloat: {
                    final int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    final float[] arr = new float[len];

                    for (int j = 0; j < len; j++) {
                        arr[j] = Bytes.unpackF4(body, offs);
                        offs += 4;
                    }

                    val = arr;
                    break;
                }
                case ClassDescriptor.tpArrayOfDouble: {
                    final int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    final double[] arr = new double[len];

                    for (int j = 0; j < len; j++) {
                        arr[j] = Bytes.unpackF8(body, offs);
                        offs += 8;
                    }

                    val = arr;
                    break;
                }
                case ClassDescriptor.tpArrayOfObject: {
                    final int len = Bytes.unpack4(body, offs);
                    obj.offs = offs + 4;
                    final Object[] arr = new Object[len];

                    for (int j = 0; j < len; j++) {
                        arr[j] = unswizzle(obj, Object.class, parent, recursiveLoading);
                    }

                    return arr;
                }
                case ClassDescriptor.tpArrayOfRaw: {
                    final int len = Bytes.unpack4(body, offs);
                    final int typeOid = Bytes.unpack4(body, offs + 4);
                    obj.offs = offs + 8;
                    Class<?> elemType;

                    if (typeOid == -1) {
                        elemType = Comparable.class;
                    } else {
                        final ClassDescriptor desc = findClassDescriptor(typeOid);
                        elemType = desc.cls;
                    }

                    final Object arr = Array.newInstance(elemType, len);

                    for (int j = 0; j < len; j++) {
                        Array.set(arr, j, unswizzle(obj, elemType, parent, recursiveLoading));
                    }

                    return arr;
                }
                case ClassDescriptor.tpCustom: {
                    final ByteArrayObjectInputStream in = new ByteArrayObjectInputStream(body, offs, parent,
                            recursiveLoading, false);
                    val = mySerializer.unpack(in);
                    offs = in.getPosition();
                    break;
                }
                default:
                    if (oid < -ClassDescriptor.tpValueTypeBias) {
                        final int typeOid = -ClassDescriptor.tpValueTypeBias - oid;
                        final ClassDescriptor desc = findClassDescriptor(typeOid);
                        val = desc.newInstance();

                        if (desc.isCollection) {
                            final int len = Bytes.unpack4(body, offs);
                            obj.offs = offs + 4;
                            final Collection collection = (Collection) val;

                            for (int i = 0; i < len; i++) {
                                collection.add(unswizzle(obj, Object.class, parent, recursiveLoading));
                            }

                            return collection;
                        } else if (desc.isMap) {
                            final int len = Bytes.unpack4(body, offs);
                            obj.offs = offs + 4;
                            final Map map = (Map) val;

                            for (int i = 0; i < len; i++) {
                                final Object key = unswizzle(obj, Object.class, parent, recursiveLoading);
                                final Object value = unswizzle(obj, Object.class, parent, recursiveLoading);
                                map.put(key, value);
                            }

                            return map;
                        } else {
                            offs = unpackObject(val, desc, recursiveLoading, body, offs, parent);
                        }
                    } else {
                        throw new StorageError(StorageError.UNSUPPORTED_TYPE);
                    }
            }
        } else {
            val = unswizzle(oid, cls, recursiveLoading);
        }

        obj.offs = offs;
        return val;
    }

    final boolean wasReserved(final long pos, final long size) {
        for (Location location = myReservedChain; location != null; location = location.next) {
            if (((pos >= location.pos) && ((pos - location.pos) < location.size)) || ((pos <= location.pos) &&
                    ((location.pos - pos) < size))) {
                return true;
            }
        }

        return false;
    }

    private final void commit0() {
        int i, j, n;
        int curr = myCurrentIndex;
        final int[] map = myDirtyPagesMap;
        final int oldIndexSize = myHeader.myRootPage[curr].myIndexSize;
        int newIndexSize = myHeader.myRootPage[1 - curr].myIndexSize;
        final int nPages = myCommittedIndexSize >>> DB_HANDLES_PER_PAGE_BITS;
        Page pg;

        if (newIndexSize > oldIndexSize) {
            cloneBitmap(myHeader.myRootPage[curr].myIndex, oldIndexSize * 8L);
            long newIndex;

            while (true) {
                newIndex = allocate(newIndexSize * 8L, 0);

                if (newIndexSize == myHeader.myRootPage[1 - curr].myIndexSize) {
                    break;
                }

                free(newIndex, newIndexSize * 8L);
                newIndexSize = myHeader.myRootPage[1 - curr].myIndexSize;
            }

            myHeader.myRootPage[1 - curr].myShadowIndex = newIndex;
            myHeader.myRootPage[1 - curr].myShadowIndexSize = newIndexSize;
            free(myHeader.myRootPage[curr].myIndex, oldIndexSize * 8L);
        }

        final long currSize = myHeader.myRootPage[1 - curr].mySize;

        for (i = 0; i < nPages; i++) {
            if ((map[i >> 5] & (1 << (i & 31))) != 0) {
                final Page srcIndex = myPagePool.getPage(myHeader.myRootPage[1 - curr].myIndex + ((long) i *
                        Page.pageSize));
                final Page dstIndex = myPagePool.getPage(myHeader.myRootPage[curr].myIndex + ((long) i *
                        Page.pageSize));

                for (j = 0; j < Page.pageSize; j += 8) {
                    final long pos = Bytes.unpack8(dstIndex.data, j);

                    if ((Bytes.unpack8(srcIndex.data, j) != pos) && (pos < currSize)) {
                        if ((pos & DB_FREE_HANDLE_FLAG) == 0) {
                            if ((pos & DB_PAGE_OBJECT_FLAG) != 0) {
                                free(pos & ~DB_FLAGS_MASK, Page.pageSize);
                            } else if (pos != 0) {
                                final int offs = (int) pos & (Page.pageSize - 1);
                                pg = myPagePool.getPage(pos - offs);
                                free(pos, ObjectHeader.getSize(pg.data, offs));
                                myPagePool.unfix(pg);
                            }
                        }
                    }
                }

                myPagePool.unfix(srcIndex);
                myPagePool.unfix(dstIndex);
            }
        }

        n = myCommittedIndexSize & (DB_HANDLES_PER_PAGE - 1);

        if ((n != 0) && ((map[i >> 5] & (1 << (i & 31))) != 0)) {
            final Page srcIndex = myPagePool.getPage(myHeader.myRootPage[1 - curr].myIndex + ((long) i *
                    Page.pageSize));
            final Page dstIndex = myPagePool.getPage(myHeader.myRootPage[curr].myIndex + ((long) i * Page.pageSize));
            j = 0;

            do {
                final long pos = Bytes.unpack8(dstIndex.data, j);

                if ((Bytes.unpack8(srcIndex.data, j) != pos) && (pos < currSize)) {
                    if ((pos & DB_FREE_HANDLE_FLAG) == 0) {
                        if ((pos & DB_PAGE_OBJECT_FLAG) != 0) {
                            free(pos & ~DB_FLAGS_MASK, Page.pageSize);
                        } else if (pos != 0) {
                            final int offs = (int) pos & (Page.pageSize - 1);
                            pg = myPagePool.getPage(pos - offs);
                            free(pos, ObjectHeader.getSize(pg.data, offs));
                            myPagePool.unfix(pg);
                        }
                    }
                }

                j += 8;
            } while (--n != 0);

            myPagePool.unfix(srcIndex);
            myPagePool.unfix(dstIndex);
        }

        for (i = 0; i <= nPages; i++) {
            if ((map[i >> 5] & (1 << (i & 31))) != 0) {
                pg = myPagePool.putPage(myHeader.myRootPage[1 - curr].myIndex + ((long) i * Page.pageSize));

                for (j = 0; j < Page.pageSize; j += 8) {
                    Bytes.pack8(pg.data, j, Bytes.unpack8(pg.data, j) & ~DB_MODIFIED_FLAG);
                }

                myPagePool.unfix(pg);
            }
        }

        if (myCurrentIndexSize > myCommittedIndexSize) {
            long page = (myHeader.myRootPage[1 - curr].myIndex + (myCommittedIndexSize * 8L)) & ~(Page.pageSize - 1);
            final long end = (((myHeader.myRootPage[1 - curr].myIndex + Page.pageSize) - 1) + (myCurrentIndexSize *
                    8L)) & ~(Page.pageSize - 1);

            while (page < end) {
                pg = myPagePool.putPage(page);

                for (j = 0; j < Page.pageSize; j += 8) {
                    Bytes.pack8(pg.data, j, Bytes.unpack8(pg.data, j) & ~DB_MODIFIED_FLAG);
                }

                myPagePool.unfix(pg);
                page += Page.pageSize;
            }
        }

        myHeader.myRootPage[1 - curr].myUsedSize = usedSize;
        pg = myPagePool.putPage(0);
        myHeader.pack(pg.data);
        myPagePool.flush();
        myPagePool.modify(pg);
        Assert.that(myHeader.myTransactionId == myTransactionId);
        myHeader.myTransactionId = ++myTransactionId;
        myHeader.myCurrentRoot = curr ^= 1;
        myHeader.myDbIsDirty = true;
        myHeader.pack(pg.data);
        myPagePool.unfix(pg);
        myPagePool.flush();
        myHeader.myRootPage[1 - curr].mySize = myHeader.myRootPage[curr].mySize;
        myHeader.myRootPage[1 - curr].myIndexUsed = myCurrentIndexSize;
        myHeader.myRootPage[1 - curr].myFreeList = myHeader.myRootPage[curr].myFreeList;
        myHeader.myRootPage[1 - curr].myBitmapEnd = myHeader.myRootPage[curr].myBitmapEnd;
        myHeader.myRootPage[1 - curr].myRootObject = myHeader.myRootPage[curr].myRootObject;
        myHeader.myRootPage[1 - curr].myClassDescriptorList = myHeader.myRootPage[curr].myClassDescriptorList;
        myHeader.myRootPage[1 - curr].myBitmapExtent = myHeader.myRootPage[curr].myBitmapExtent;

        if ((myCurrentIndexSize == 0) || (newIndexSize != oldIndexSize)) {
            myHeader.myRootPage[1 - curr].myIndex = myHeader.myRootPage[curr].myShadowIndex;
            myHeader.myRootPage[1 - curr].myIndexSize = myHeader.myRootPage[curr].myShadowIndexSize;
            myHeader.myRootPage[1 - curr].myShadowIndex = myHeader.myRootPage[curr].myIndex;
            myHeader.myRootPage[1 - curr].myShadowIndexSize = myHeader.myRootPage[curr].myIndexSize;
            myPagePool.copy(myHeader.myRootPage[1 - curr].myIndex, myHeader.myRootPage[curr].myIndex,
                    myCurrentIndexSize * 8L);
            i = ((myCurrentIndexSize + (DB_HANDLES_PER_PAGE * 32)) - 1) >>> (DB_HANDLES_PER_PAGE_BITS + 5);

            while (--i >= 0) {
                map[i] = 0;
            }
        } else {
            for (i = 0; i < nPages; i++) {
                if ((map[i >> 5] & (1 << (i & 31))) != 0) {
                    map[i >> 5] -= 1 << (i & 31);
                    myPagePool.copy(myHeader.myRootPage[1 - curr].myIndex + ((long) i * Page.pageSize),
                            myHeader.myRootPage[curr].myIndex + ((long) i * Page.pageSize), Page.pageSize);
                }
            }

            if ((myCurrentIndexSize > (i * DB_HANDLES_PER_PAGE)) && (((map[i >> 5] & (1 << (i & 31))) != 0) ||
                    (myCurrentIndexSize != myCommittedIndexSize))) {
                myPagePool.copy(myHeader.myRootPage[1 - curr].myIndex + ((long) i * Page.pageSize),
                        myHeader.myRootPage[curr].myIndex + ((long) i * Page.pageSize), (8L * myCurrentIndexSize) -
                                ((long) i * Page.pageSize));
                j = i >>> 5;
                n = ((myCurrentIndexSize + (DB_HANDLES_PER_PAGE * 32)) - 1) >>> (DB_HANDLES_PER_PAGE_BITS + 5);

                while (j < n) {
                    map[j++] = 0;
                }
            }
        }

        myGcDone = false;
        myCurrentIndex = curr;
        myCommittedIndexSize = myCurrentIndexSize;

        if (myMulticlientSupport) {
            myPagePool.flush();
            pg = myPagePool.putPage(0);
            myHeader.myDbIsDirty = false;
            myHeader.pack(pg.data);
            myPagePool.unfix(pg);
            myPagePool.flush();
        }

        if (myListener != null) {
            myListener.onTransactionCommit();
        }
    }

    private void deallocateObject0(final Object obj) {
        if (myListener != null) {
            myListener.onObjectDelete(obj);
        }

        final int oid = getOid(obj);
        final long pos = getPos(oid);
        myObjectCache.remove(oid);
        int offs = (int) pos & (Page.pageSize - 1);

        if ((offs & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
            throw new StorageError(StorageError.DELETED_OBJECT);
        }

        final Page pg = myPagePool.getPage(pos - offs);
        offs &= ~DB_FLAGS_MASK;
        final int size = ObjectHeader.getSize(pg.data, offs);
        myPagePool.unfix(pg);
        freeId(oid);
        final CustomAllocator allocator = customAllocatorMap != null ? getCustomAllocator(obj.getClass()) : null;

        if (allocator != null) {
            allocator.free(pos & ~DB_FLAGS_MASK, size);
        } else {
            if ((pos & DB_MODIFIED_FLAG) != 0) {
                free(pos & ~DB_FLAGS_MASK, size);
            } else {
                cloneBitmap(pos, size);
            }
        }

        unassignOid(obj);
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
            // System.out.println("Start GC, allocatedDelta=" + allocatedDelta +
            // ", header[" + currIndex + "].size=" + header.root[currIndex].size
            // + ", gcTreshold=" + gcThreshold);

            mark();

            return sweep();
        }
    }

    private boolean getBooleanValue(final Object value) {
        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        } else if (value instanceof String) {
            final String s = (String) value;

            if ("true".equalsIgnoreCase(s) || "t".equalsIgnoreCase(s) || "1".equals(s)) {
                return true;
            } else if ("false".equalsIgnoreCase(s) || "f".equalsIgnoreCase(s) || "0".equals(s)) {
                return false;
            }
        }

        throw new StorageError(StorageError.BAD_PROPERTY_VALUE);
    }

    private final CustomAllocator getCustomAllocator(final Class<?> aClass) {
        final Object a = customAllocatorMap.get(aClass);

        if (a != null) {
            return a == defaultAllocator ? null : (CustomAllocator) a;
        }

        final Class<?> superclass = aClass.getSuperclass();

        if (superclass != null) {
            final CustomAllocator alloc = getCustomAllocator(superclass);

            if (alloc != null) {
                customAllocatorMap.put(aClass, alloc);
                return alloc;
            }
        }

        final Class<?>[] interfaces = aClass.getInterfaces();

        for (final Class<?> interface1 : interfaces) {
            final CustomAllocator alloc = getCustomAllocator(interface1);

            if (alloc != null) {
                customAllocatorMap.put(aClass, alloc);
                return alloc;
            }
        }

        customAllocatorMap.put(aClass, defaultAllocator);
        return null;
    }

    private long getIntegerValue(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            try {
                return Long.parseLong((String) value, 10);
            } catch (final NumberFormatException x) {
            }
        }

        throw new StorageError(StorageError.BAD_PROPERTY_VALUE);
    }

    private void mark() {
        final int bitmapSize = (int) (myHeader.myRootPage[myCurrentIndex].mySize >>> (DB_ALLOCATION_QUANTUM_BITS + 5)) +
                1;
        boolean existsNotMarkedObjects;
        long pos;
        int i, j;

        if (myListener != null) {
            myListener.gcStarted();
        }

        myGreyBitmap = new int[bitmapSize];
        myBlackBitmap = new int[bitmapSize];

        final int rootOid = myHeader.myRootPage[myCurrentIndex].myRootObject;

        if (rootOid != 0) {
            markOid(rootOid);

            do {
                existsNotMarkedObjects = false;
                for (i = 0; i < bitmapSize; i++) {
                    if (myGreyBitmap[i] != 0) {
                        existsNotMarkedObjects = true;

                        for (j = 0; j < 32; j++) {
                            if ((myGreyBitmap[i] & (1 << j)) != 0) {
                                pos = (((long) i << 5) + j) << DB_ALLOCATION_QUANTUM_BITS;
                                myGreyBitmap[i] &= ~(1 << j);
                                myBlackBitmap[i] |= 1 << j;

                                final int offs = (int) pos & (Page.pageSize - 1);
                                final Page pg = myPagePool.getPage(pos - offs);
                                final int typeOid = ObjectHeader.getType(pg.data, offs);

                                if (typeOid != 0) {
                                    final ClassDescriptor desc = findClassDescriptor(typeOid);

                                    if (Btree.class.isAssignableFrom(desc.cls)) {
                                        final Btree btree = new Btree(pg.data, ObjectHeader.sizeof + offs);
                                        btree.assignOid(this, 0, false);
                                        btree.markTree();
                                    } else if (desc.hasReferences) {
                                        markObject(myPagePool.get(pos), ObjectHeader.sizeof, desc);
                                    }
                                }

                                myPagePool.unfix(pg);
                            }
                        }
                    }
                }
            } while (existsNotMarkedObjects);
        }
    }

    private final void rollback0() {
        final int curr = myCurrentIndex;
        final int[] map = myDirtyPagesMap;

        if (myHeader.myRootPage[1 - curr].myIndex != myHeader.myRootPage[curr].myShadowIndex) {
            myPagePool.copy(myHeader.myRootPage[curr].myShadowIndex, myHeader.myRootPage[curr].myIndex, 8L *
                    myCommittedIndexSize);
        } else {
            final int nPages = ((myCommittedIndexSize + DB_HANDLES_PER_PAGE) - 1) >>> DB_HANDLES_PER_PAGE_BITS;

            for (int i = 0; i < nPages; i++) {
                if ((map[i >> 5] & (1 << (i & 31))) != 0) {
                    myPagePool.copy(myHeader.myRootPage[curr].myShadowIndex + ((long) i * Page.pageSize),
                            myHeader.myRootPage[curr].myIndex + ((long) i * Page.pageSize), Page.pageSize);
                }
            }
        }

        for (int j = ((myCurrentIndexSize + (DB_HANDLES_PER_PAGE * 32)) - 1) >>> (DB_HANDLES_PER_PAGE_BITS +
                5); --j >= 0; map[j] = 0) {
            ;
        }

        myHeader.myRootPage[1 - curr].myIndex = myHeader.myRootPage[curr].myShadowIndex;
        myHeader.myRootPage[1 - curr].myIndexSize = myHeader.myRootPage[curr].myShadowIndexSize;
        myHeader.myRootPage[1 - curr].myIndexUsed = myCommittedIndexSize;
        myHeader.myRootPage[1 - curr].myFreeList = myHeader.myRootPage[curr].myFreeList;
        myHeader.myRootPage[1 - curr].myBitmapEnd = myHeader.myRootPage[curr].myBitmapEnd;
        myHeader.myRootPage[1 - curr].mySize = myHeader.myRootPage[curr].mySize;
        myHeader.myRootPage[1 - curr].myRootObject = myHeader.myRootPage[curr].myRootObject;
        myHeader.myRootPage[1 - curr].myClassDescriptorList = myHeader.myRootPage[curr].myClassDescriptorList;
        myHeader.myRootPage[1 - curr].myBitmapExtent = myHeader.myRootPage[curr].myBitmapExtent;
        usedSize = myHeader.myRootPage[curr].mySize;
        myCurrentIndexSize = myCommittedIndexSize;
        myCurrentRecordBitmapPage = myCurrentPageBitmapPage = 0;
        myCurrentRecordBitmapOffset = myCurrentPageBitmapOffset = 0;
        reloadScheme();

        if (myListener != null) {
            myListener.onTransactionRollback();
        }
    }

    private final void storeObject0(final Object obj, final boolean finalized) {
        if (obj instanceof IStoreable) {
            ((IPersistent) obj).onStore();
        }

        if (myListener != null) {
            myListener.onObjectStore(obj);
        }

        int oid = getOid(obj);
        boolean newObject = false;

        if (oid == 0) {
            oid = allocateId();

            if (!finalized) {
                myObjectCache.put(oid, obj);
            }

            assignOid(obj, oid, false);
            newObject = true;
        } else if (isModified(obj)) {
            myObjectCache.clearDirty(obj);
        }

        final byte[] data = packObject(obj, finalized);
        long pos;
        final int newSize = ObjectHeader.getSize(data, 0);
        final CustomAllocator allocator = customAllocatorMap != null ? getCustomAllocator(obj.getClass()) : null;

        if (newObject || ((pos = getPos(oid)) == 0)) {
            pos = allocator != null ? allocator.allocate(newSize) : allocate(newSize, 0);
            setPos(oid, pos | DB_MODIFIED_FLAG);
        } else {
            final int offs = (int) pos & (Page.pageSize - 1);

            if ((offs & (DB_FREE_HANDLE_FLAG | DB_PAGE_OBJECT_FLAG)) != 0) {
                throw new StorageError(StorageError.DELETED_OBJECT);
            }

            final Page pg = myPagePool.getPage(pos - offs);
            final int size = ObjectHeader.getSize(pg.data, offs & ~DB_FLAGS_MASK);
            myPagePool.unfix(pg);

            if ((pos & DB_MODIFIED_FLAG) == 0) {
                if (allocator != null) {
                    allocator.free(pos & ~DB_FLAGS_MASK, size);
                    pos = allocator.allocate(newSize);
                } else {
                    cloneBitmap(pos & ~DB_FLAGS_MASK, size);
                    pos = allocate(newSize, 0);
                }

                setPos(oid, pos | DB_MODIFIED_FLAG);
            } else {
                pos &= ~DB_FLAGS_MASK;

                if (newSize != size) {
                    if (allocator != null) {
                        final long newPos = allocator.reallocate(pos, size, newSize);

                        if (newPos != pos) {
                            pos = newPos;
                            setPos(oid, pos | DB_MODIFIED_FLAG);
                        } else if (newSize < size) {
                            ObjectHeader.setSize(data, 0, size);
                        }
                    } else {
                        if ((((newSize + DB_ALLOCATION_QUANTUM) - 1) & ~(DB_ALLOCATION_QUANTUM - 1)) > (((size +
                                DB_ALLOCATION_QUANTUM) - 1) & ~(DB_ALLOCATION_QUANTUM - 1))) {
                            final long newPos = allocate(newSize, 0);
                            cloneBitmap(pos, size);
                            free(pos, size);
                            pos = newPos;
                            setPos(oid, pos | DB_MODIFIED_FLAG);
                        } else if (newSize < size) {
                            ObjectHeader.setSize(data, 0, size);
                        }
                    }
                }
            }
        }

        myModified = true;
        myPagePool.put(pos, data, newSize);
    }

    private int sweep() {
        int nDeallocated = 0;
        long pos;
        myGcDone = true;

        for (int i = DB_FIRST_USER_ID, j = myCommittedIndexSize; i < j; i++) {
            pos = getGCPos(i);

            if ((pos != 0) && (((int) pos & (DB_PAGE_OBJECT_FLAG | DB_FREE_HANDLE_FLAG)) == 0)) {
                final int bit = (int) (pos >>> DB_ALLOCATION_QUANTUM_BITS);

                if ((myBlackBitmap[bit >>> 5] & (1 << (bit & 31))) == 0) {
                    // object is not accessible
                    if (getPos(i) == pos) {
                        final int offs = (int) pos & (Page.pageSize - 1);
                        final Page pg = myPagePool.getPage(pos - offs);
                        final int typeOid = ObjectHeader.getType(pg.data, offs);

                        if (typeOid != 0) {
                            final ClassDescriptor desc = findClassDescriptor(typeOid);
                            nDeallocated += 1;

                            if (Btree.class.isAssignableFrom(desc.cls)) {
                                final Btree btree = new Btree(pg.data, ObjectHeader.sizeof + offs);
                                myPagePool.unfix(pg);
                                btree.assignOid(this, i, false);
                                btree.deallocate();
                            } else {
                                final int size = ObjectHeader.getSize(pg.data, offs);
                                myPagePool.unfix(pg);
                                freeId(i);
                                myObjectCache.remove(i);
                                cloneBitmap(pos, size);
                            }

                            if (myListener != null) {
                                myListener.deallocateObject(desc.cls, i);
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
            myListener.gcCompleted(nDeallocated);
        }

        return nDeallocated;
    }

    public class AnnotatedPersistentObjectOutputStream extends PersistentObjectOutputStream {

        AnnotatedPersistentObjectOutputStream(final OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void annotateClass(final Class<?> aClass) throws IOException {
            final ClassLoader loader = aClass.getClassLoader();
            writeObject(loader instanceof INamedClassLoader ? ((INamedClassLoader) loader).getName() : null);
        }
    }

    public class PersistentObjectOutputStream extends ObjectOutputStream {

        PersistentObjectOutputStream(final OutputStream out) throws IOException {
            super(out);
        }

        public Storage getStorage() {
            return StorageImpl.this;
        }
    }

    class ByteArrayObjectInputStream extends SodboxInputStream {

        private final byte[] buf;

        private final boolean markReferences;

        private final Object parent;

        private final boolean recursiveLoading;

        ByteArrayObjectInputStream(final byte[] buf, final int offs, final Object parent,
                final boolean aRecursiveLoading, final boolean aMarkReferences) {
            super(new ByteArrayInputStream(buf, offs, buf.length - offs));

            this.buf = buf;
            this.parent = parent;

            recursiveLoading = aRecursiveLoading;
            markReferences = aMarkReferences;
        }

        @Override
        public Object readObject() throws IOException {
            int offs = getPosition();
            Object obj = null;

            if (markReferences) {
                offs = markObjectReference(buf, offs) - offs;
            } else {
                try {
                    final ArrayPos pos = new ArrayPos(buf, offs);
                    obj = unswizzle(pos, Object.class, parent, recursiveLoading);
                    offs = pos.offs - offs;
                } catch (final Exception x) {
                    throw new StorageError(StorageError.ACCESS_VIOLATION, x);
                }
            }

            in.skip(offs);
            return obj;
        }

        @Override
        public String readString() throws IOException {
            final int offs = getPosition();
            final ArrayPos pos = new ArrayPos(buf, offs);
            final String str = Bytes.unpackString(pos, myEncoding);
            in.skip(pos.offs - offs);
            return str;
        }

        int getPosition() throws IOException {
            return buf.length - in.available();
        }
    }

    static class CloneNode {

        CloneNode next;

        long pos;

        CloneNode(final long pos, final CloneNode list) {
            this.pos = pos;
            next = list;
        }
    }

    class GcThread extends Thread {

        private boolean go;

        GcThread() {
            start();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    synchronized (myBackgroundGcStartMonitor) {
                        while (!go && myOpened) {
                            myBackgroundGcStartMonitor.wait();
                        }

                        if (!myOpened) {
                            return;
                        }

                        go = false;
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
            } catch (final InterruptedException x) {
            }
        }

        void activate() {
            synchronized (myBackgroundGcStartMonitor) {
                go = true;
                myBackgroundGcStartMonitor.notify();
            }
        }
    }

    class HashIterator implements PersistentIterator, Iterator {

        Iterator oids;

        HashIterator(final HashSet result) {
            oids = result.iterator();
        }

        @Override
        public boolean hasNext() {
            return oids.hasNext();
        }

        @Override
        public Object next() {
            final int oid = ((Integer) oids.next()).intValue();
            return lookupObject(oid, null);
        }

        @Override
        public int nextOid() {
            return oids.hasNext() ? ((Integer) oids.next()).intValue() : 0;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static class Location {

        Location next;

        long pos;

        long size;
    }

    class PersistentObjectInputStream extends ObjectInputStream {

        PersistentObjectInputStream(final InputStream in) throws IOException {
            super(in);
            enableResolveObject(true);
        }

        @Override
        protected Class<?> resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            String classLoaderName = null;

            if ((myClassLoaderMap != null) && ((myCompatibilityMode &
                    CLASS_LOADER_SERIALIZATION_COMPATIBILITY_MODE) == 0)) {
                classLoaderName = (String) readObject();
            }

            final ClassLoader cl = classLoaderName != null ? findClassLoader(classLoaderName) : myClassLoader;

            if (cl != null) {
                try {
                    return Class.forName(desc.getName(), false, cl);
                } catch (final ClassNotFoundException x) {
                }
            }

            return super.resolveClass(desc);
        }

        @Override
        protected Object resolveObject(final Object obj) throws IOException {
            final int oid = getOid(obj);

            if (oid != 0) {
                return lookupObject(oid, obj.getClass());
            }

            return obj;
        }
    }
}

class Header {

    static final int SIZE = 3 + (RootPage.SIZE * 2) + 8;

    int myCurrentRoot;

    byte myDatabaseFormatVersion;

    boolean myDbIsDirty; // database was not closed normally

    RootPage myRootPage[];

    long myTransactionId;

    final void pack(final byte[] rec) {
        int offs = 0;

        rec[offs++] = (byte) myCurrentRoot;
        rec[offs++] = (byte) (myDbIsDirty ? 1 : 0);
        rec[offs++] = myDatabaseFormatVersion;

        for (int i = 0; i < 2; i++) {
            Bytes.pack8(rec, offs, myRootPage[i].mySize);
            offs += 8;
            Bytes.pack8(rec, offs, myRootPage[i].myIndex);
            offs += 8;
            Bytes.pack8(rec, offs, myRootPage[i].myShadowIndex);
            offs += 8;
            Bytes.pack8(rec, offs, myRootPage[i].myUsedSize);
            offs += 8;
            Bytes.pack4(rec, offs, myRootPage[i].myIndexSize);
            offs += 4;
            Bytes.pack4(rec, offs, myRootPage[i].myShadowIndexSize);
            offs += 4;
            Bytes.pack4(rec, offs, myRootPage[i].myIndexUsed);
            offs += 4;
            Bytes.pack4(rec, offs, myRootPage[i].myFreeList);
            offs += 4;
            Bytes.pack4(rec, offs, myRootPage[i].myBitmapEnd);
            offs += 4;
            Bytes.pack4(rec, offs, myRootPage[i].myRootObject);
            offs += 4;
            Bytes.pack4(rec, offs, myRootPage[i].myClassDescriptorList);
            offs += 4;
            Bytes.pack4(rec, offs, myRootPage[i].myBitmapExtent);
            offs += 4;
        }

        Bytes.pack8(rec, offs, myTransactionId);
        offs += 8;
        Assert.that(offs == SIZE);
    }

    final void unpack(final byte[] rec) {
        int offs = 0;
        myCurrentRoot = rec[offs++];
        myDbIsDirty = rec[offs++] != 0;
        myDatabaseFormatVersion = rec[offs++];
        myRootPage = new RootPage[2];

        for (int i = 0; i < 2; i++) {
            myRootPage[i] = new RootPage();
            myRootPage[i].mySize = Bytes.unpack8(rec, offs);
            offs += 8;
            myRootPage[i].myIndex = Bytes.unpack8(rec, offs);
            offs += 8;
            myRootPage[i].myShadowIndex = Bytes.unpack8(rec, offs);
            offs += 8;
            myRootPage[i].myUsedSize = Bytes.unpack8(rec, offs);
            offs += 8;
            myRootPage[i].myIndexSize = Bytes.unpack4(rec, offs);
            offs += 4;
            myRootPage[i].myShadowIndexSize = Bytes.unpack4(rec, offs);
            offs += 4;
            myRootPage[i].myIndexUsed = Bytes.unpack4(rec, offs);
            offs += 4;
            myRootPage[i].myFreeList = Bytes.unpack4(rec, offs);
            offs += 4;
            myRootPage[i].myBitmapEnd = Bytes.unpack4(rec, offs);
            offs += 4;
            myRootPage[i].myRootObject = Bytes.unpack4(rec, offs);
            offs += 4;
            myRootPage[i].myClassDescriptorList = Bytes.unpack4(rec, offs);
            offs += 4;
            myRootPage[i].myBitmapExtent = Bytes.unpack4(rec, offs);
            offs += 4;
        }

        myTransactionId = Bytes.unpack8(rec, offs);
        offs += 8;
        Assert.that(offs == SIZE);
    }
}

class RootPage {

    static final int SIZE = 64;

    int myBitmapEnd; // index of last allocated bitmap page

    int myBitmapExtent; // Offset of extended bitmap pages in object index

    int myClassDescriptorList; // List of class descriptors

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
