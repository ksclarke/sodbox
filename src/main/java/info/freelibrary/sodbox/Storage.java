
package info.freelibrary.sodbox;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import info.freelibrary.sodbox.fulltext.FullTextIndex;
import info.freelibrary.sodbox.fulltext.FullTextSearchHelper;
import info.freelibrary.sodbox.impl.ThreadTransactionContext;

/**
 * Object storage.
 */
public interface Storage {

    /**
     * Constant specifying that page pool should be dynamically extended to contains all database file pages.
     */
    int INFINITE_PAGE_POOL = 0;

    /**
     * Constant specifying default pool size.
     */
    int DEFAULT_PAGE_POOL_SIZE = 4 * 1024 * 1024;

    /**
     * Compatibility with databases created by Sodbox prior to 2.73 release by IBM Java5 VM.
     */
    int IBM_JAVA5_COMPATIBILITY_MODE = 1;

    /**
     * Do not store information about class loaders when serializing objects with custom class loaders.
     */
    int CLASS_LOADER_SERIALIZATION_COMPATIBILITY_MODE = 2;

    /**
     * Exclusive per-thread transaction: each thread access database in exclusive mode.
     */
    int EXCLUSIVE_TRANSACTION = 0;

    /**
     * Alias for EXCLUSIVE_TRANSACTION. In case of multiclient access, any transaction modifying database should be
     * exclusive.
     */
    int READ_WRITE_TRANSACTION = EXCLUSIVE_TRANSACTION;

    /**
     * Cooperative mode; all threads share the same transaction. Commit will commit changes made by all threads. To make
     * this schema work correctly, it is necessary to ensure (using locking) that no thread is performing update of the
     * database while another one tries to perform commit. Also please notice that rollback will undo the work of all
     * threads.
     */
    int COOPERATIVE_TRANSACTION = 1;

    /**
     * Alias for COOPERATIVE_TRANSACTION. In case of multi-client access, only read-only transactions can be executed in
     * parallel.
     */
    int READ_ONLY_TRANSACTION = COOPERATIVE_TRANSACTION;

    /**
     * Serializable per-thread transaction. Unlike exclusive mode, threads can concurrently access database, but effect
     * will be the same as them work exclusively. To provide such behavior, programmer should lock all access objects
     * (or use hierarchical locking). When object is updated, exclusive lock should be set, otherwise shared lock is
     * enough. Lock should be preserved until the end of transaction.
     */
    int SERIALIZABLE_TRANSACTION = 2;

    /**
     * Read only transaction which can be started at replication slave node. It runs concurrently with receiving updates
     * from master node.
     */
    int REPLICATION_SLAVE_TRANSACTION = 3;

    /**
     * Backup current state of database.
     *
     * @param aOutStream An output stream to which backup is done
     */
    void backup(OutputStream aOutStream) throws IOException;

    /**
     * Backup current state of database to the file with specified path.
     *
     * @param aFilePath A path to the backup file
     * @param aCipherKey A cipher key for the encryption of the backup file, null to disable encryption
     */
    void backup(String aFilePath, String aCipherKey) throws IOException;

    /**
     * Start serializable transaction. This call is equivalent to
     * <code>beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION)</code>.
     */
    void beginSerializableTransaction();

    /**
     * Begin per-thread transaction. Three types of per-thread transactions are supported: exclusive, cooperative and
     * serializable. In case of exclusive transaction, only one thread can update the database. In cooperative mode,
     * multiple transaction can work concurrently and commit() method will be invoked only when transactions of all
     * threads are terminated. Serializable transactions can also work concurrently. But unlike cooperative transaction,
     * the threads are isolated from each other. Each thread has its own associated set of modified objects and
     * committing the transaction will cause saving only of these objects to the database. To synchronize access to the
     * objects in case of serializable transaction programmer should use lock methods of IResource interface. Shared
     * lock should be set before read access to any object, and exclusive lock - before write access. Locks will be
     * automatically released when transaction is committed (so programmer should not explicitly invoke unlock method)
     * In this case it is guaranteed that transactions are serializable.<br>
     * It is not possible to use <code>IPersistent.store()</code> method in serializable transactions. That is why it is
     * also not possible to use Index and FieldIndex containers (since them are based on B-Tree and B-Tree directly
     * access database pages and use <code>store()</code> method to assign OID to inserted object. You should use
     * <code>SortedCollection</code> based on T-Tree instead or alternative B-Tree implementation (set
     * "sodbox.alternative.btree" property).
     *
     * @param aMode <code>EXCLUSIVE_TRANSACTION</code>, <code>COOPERATIVE_TRANSACTION</code>,
     *        <code>SERIALIZABLE_TRANSACTION</code> or <code>REPLICATION_SLAVE_TRANSACTION</code>
     */
    void beginThreadTransaction(int aMode);

    /**
     * Clear database object cache. This method can be used with "strong" object cache to avoid memory overflow. It is
     * no valid to invoke this method when there are some uncommitted changes in the database (some modified objects).
     * Also all variables containing references to persistent object should be reset after invocation of this method -
     * it is not correct to accessed object directly though such variables, objects has to be reloaded from the storage.
     */
    void clearObjectCache();

    /**
     * Commit transaction (if needed) and close the storage.
     */
    void close();

    /**
     * Commit changes done by the last transaction. Transaction is started implicitly with first update operation.
     */
    void commit();

    /**
     * Commit serializable transaction. This call is equivalent to <code>endThreadTransaction</code> but it checks that
     * serializable transaction was previously started using <code>beginSerializableTransaction()</code> method.
     *
     * @throws StorageError NOT_IN_TRANSACTION if this method is invoked outside serializable transaction body
     */
    void commitSerializableTransaction() throws StorageError;

    /**
     * Create new persistent multi-set (allowing several occurrences of the same object). Implementation of this set is
     * based on B-Tree so it can efficiently handle large number of objects but in case of very small set memory
     * overhead is too high.
     *
     * @return persistent object implementing set
     */
    <T> IPersistentSet<T> createBag();

    /**
     * Create bitmap custom allocator.
     *
     * @param aQuantum size in bytes of allocation quantum. Should be power of two.
     * @param aBase base address for allocator (it should match offset of multifile segment)
     * @param aExtension size by which space mapped by allocator is extended each time when no suitable hole is found in
     *        bitmap (it should be large enough to improve allocation speed and locality of references)
     * @param aLimit maximal size of memory allocated by this allocator (pass Long.MAX_VALUE if you do not want to limit
     *        space)
     * @return A created allocator
     */
    CustomAllocator createBitmapAllocator(int aQuantum, long aBase, long aExtension, long aLimit);

    /**
     * Create new BLOB. Create object for storing large binary data.
     *
     * @return empty BLOB
     */
    Blob createBlob();

    /**
     * Create new field index.
     *
     * @param aType objects of which type (or derived from which type) will be included in the index
     * @param aFieldName name of the index field. Field with such name should be present in specified class
     *        <code>type</code>
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing field index
     * @throws StorageError StorageError.INDEXED_FIELD_NOT_FOUND if there is no such field in specified class,
     *         StorageError.UNSUPPORTED_INDEX_TYPE exception if type of specified field is not supported by
     *         implementation
     */
    <T> FieldIndex<T> createFieldIndex(Class<?> aType, String aFieldName, boolean aUnique);

    /**
     * Create new field index.
     *
     * @param aType objects of which type (or derived from which type) will be included in the index
     * @param aFieldName name of the index field. Field with such name should be present in specified class
     *        <code>type</code>
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @param aCaseInsensitive If string index is case insensitive
     * @return persistent object implementing field index
     * @throws StorageError StorageError.INDEXED_FIELD_NOT_FOUND if there is no such field in specified class,
     *         StorageError.UNSUPPORTED_INDEX_TYPE exception if type of specified field is not supported by
     *         implementation
     */
    <T> FieldIndex<T> createFieldIndex(Class<?> aType, String aFieldName, boolean aUnique, boolean aCaseInsensitive);

    /**
     * Create new field index.
     *
     * @param aType objects of which type (or derived from which type) will be included in the index
     * @param aFieldName name of the index field. Field with such name should be present in specified class
     *        <code>type</code>
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @param aCaseInsensitive if string index is case insensitive
     * @param aThickIndex Index should be optimized to handle large number of duplicate key values
     * @return persistent object implementing field index
     * @throws StorageError StorageError.INDEXED_FIELD_NOT_FOUND if there is no such field in specified class,
     *         StorageError.UNSUPPORTED_INDEX_TYPE exception if type of specified field is not supported by
     *         implementation
     */
    <T> FieldIndex<T> createFieldIndex(Class<?> aType, String aFieldName, boolean aUnique, boolean aCaseInsensitive,
            boolean aThickIndex);

    /**
     * Create new multi-field index.
     *
     * @param aType objects of which type (or derived from which type) will be included in the index
     * @param aFieldNames names of the index fields. Fields with such name should be present in specified class
     *        <code>type</code>
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing field index
     * @throws StorageError StorageError.INDEXED_FIELD_NOT_FOUND if there is no such field in specified class,
     *         StorageError.UNSUPPORTED_INDEX_TYPE exception if type of specified field is not supported by
     *         implementation
     */
    <T> FieldIndex<T> createFieldIndex(Class<?> aType, String[] aFieldNames, boolean aUnique);

    /**
     * Create new multi-field index.
     *
     * @param aType objects of which type (or derived from which type) will be included in the index
     * @param aFieldNames names of the index fields. Fields with such name should be present in specified class
     *        <code>type</code>
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @param aCaseInsensitive whether index is case insensitive (ignored for non-string keys)
     * @return Persistent object implementing field index
     * @throws StorageError StorageError.INDEXED_FIELD_NOT_FOUND if there is no such field in specified class,
     *         StorageError.UNSUPPORTED_INDEX_TYPE exception if type of specified field is not supported by
     *         implementation
     */
    <T> FieldIndex<T> createFieldIndex(Class<?> aType, String[] aFieldNames, boolean aUnique, boolean aCaseInsensitive);

    /**
     * Create full text search index with default helper.
     *
     * @return full text search index
     */
    FullTextIndex createFullTextIndex();

    /**
     * Create full text search index.
     *
     * @param aHelper A helper class which provides method for scanning, stemming and tuning query
     * @return Full text search index
     */
    FullTextIndex createFullTextIndex(FullTextSearchHelper aHelper);

    /**
     * Create hierarchical hash table. Levels of tree are added on demand.
     *
     * @return persistent hash table
     */
    <K, V> IPersistentHash<K, V> createHash();

    /**
     * Create hierarchical hash table. Levels of tree are added on demand.
     *
     * @param aPageSize A page size
     * @param aLoadFactor A load factor
     * @return persistent hash table
     */
    <K, V> IPersistentHash<K, V> createHash(int aPageSize, int aLoadFactor);

    /**
     * Create new index.
     *
     * @param aType type of the index key (you should path here <code>String.class</code>, <code>int.class</code>, ...)
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @return Persistent object implementing index
     * @throws StorageError StorageError.UNSUPPORTED_INDEX_TYPE exception if specified key type is not supported by
     *         implementation.
     */
    <T> Index<T> createIndex(Class<?> aType, boolean aUnique);

    /**
     * Create new compound index.
     *
     * @param aTypes types of the index compound key components
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @return persistent object implementing compound index
     * @throws StorageError StorageError.UNSUPPORTED_INDEX_TYPE exception if specified key type is not supported by
     *         implementation.
     */
    <T> Index<T> createIndex(Class<?>[] aTypes, boolean aUnique);

    /**
     * Create one-to-many link.
     *
     * @return New empty link, new members can be added to the link later.
     */
    <T> Link<T> createLink();

    /**
     * Create one-to-many link with specified initially allocated size.
     *
     * @param aInitialSize initial size of array
     * @return New empty link, new members can be added to the link later.
     */
    <T> Link<T> createLink(int aInitialSize);

    /**
     * Create new persistent list. Implementation of this list is based on B-Tree so it can efficiently handle large
     * number of objects but in case of very small list memory overhead is too high.
     *
     * @return Persistent object implementing list
     */
    <T> IPersistentList<T> createList();

    /**
     * Create scalable persistent map. This container can efficiently handle both small and large number of members. For
     * small maps, implementation uses sorted array. For large maps - B-Tree.
     *
     * @param aKeyType map key type
     * @return Scalable map implementation
     */
    <K extends Comparable<?>, V> IPersistentMap<K, V> createMap(Class<?> aKeyType);

    /**
     * Create scalable persistent map. This container can efficiently handle both small and large number of members. For
     * small maps, implementation uses sorted array. For large maps - B-Tree.
     *
     * @param aKeyType map key type
     * @param aInitialSize initial allocated size of the list
     * @return Scalable map implementation
     */
    <K extends Comparable<?>, V> IPersistentMap<K, V> createMap(Class<?> aKeyType, int aInitialSize);

    /**
     * Create new multidimensional index for specified fields of the class.
     *
     * @param aType class of objects included in this index
     * @param aFieldNames name of the fields which are treated as index dimensions, if null then all declared fields of
     *        the class are used.
     * @param aTreatZeroAsUndefinedValue if value of scalar field in QBE object is 0 (default value) then assume that
     *        condition is not defined for this field
     * @return Multidimensional index
     */
    <T> MultidimensionalIndex<T> createMultidimensionalIndex(Class<?> aType, String[] aFieldNames,
            boolean aTreatZeroAsUndefinedValue);

    /**
     * Create new multidimensional index.
     *
     * @param aComparator multidimensional comparator
     * @return Multidimensional index
     */
    <T> MultidimensionalIndex<T> createMultidimensionalIndex(MultidimensionalComparator<T> aComparator);

    /**
     * Create PATRICIA trie (Practical Algorithm To Retrieve Information Coded In Alphanumeric) Tries are a kind of tree
     * where each node holds a common part of one or more keys. PATRICIA trie is one of the many existing variants of
     * the trie, which adds path compression by grouping common sequences of nodes together.<BR>
     * This structure provides a very efficient way of storing values while maintaining the lookup time for a key in
     * O(N) in the worst case, where N is the length of the longest key. This structure has it's main use in IP routing
     * software, but can provide an interesting alternative to other structures such as hashtables when memory space is
     * of concern.
     *
     * @return created PATRICIA trie
     */
    <T> PatriciaTrie<T> createPatriciaTrie();

    /**
     * Create new random access BLOB. Create file-like object providing efficient random position access.
     *
     * @return empty BLOB
     */
    Blob createRandomAccessBlob();

    /**
     * Create new field index optimized for access by element position.
     *
     * @param aType objects of which type (or derived from which type) will be included in the index
     * @param aFieldName name of the index field. Field with such name should be present in specified class
     *        <code>type</code>
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @return Persistent object implementing field index
     * @throws StorageError StorageError.INDEXED_FIELD_NOT_FOUND if there is no such field in specified class,
     *         StorageError.UNSUPPORTED_INDEX_TYPE exception if type of specified field is not supported by
     *         implementation
     */
    <T> FieldIndex<T> createRandomAccessFieldIndex(Class<?> aType, String aFieldName, boolean aUnique);

    /**
     * Create new field index optimized for access by element position.
     *
     * @param aType objects of which type (or derived from which type) will be included in the index
     * @param aFieldName name of the index field. Field with such name should be present in specified class
     *        <code>type</code>
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @param aCaseInsensitive whether index is case insensitive (ignored for non-string keys)
     * @return Persistent object implementing field index
     * @throws StorageError StorageError.INDEXED_FIELD_NOT_FOUND if there is no such field in specified class,
     *         StorageError.UNSUPPORTED_INDEX_TYPE exception if type of specified field is not supported by
     *         implementation
     */
    <T> FieldIndex<T> createRandomAccessFieldIndex(Class<?> aType, String aFieldName, boolean aUnique,
            boolean aCaseInsensitive);

    /**
     * Create new multi-field index optimized for access by element position.
     *
     * @param aType objects of which type (or derived from which type) will be included in the index
     * @param aFieldNames names of the index fields. Fields with such name should be present in specified class
     *        <code>type</code>
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @return Persistent object implementing field index
     * @throws StorageError StorageError.INDEXED_FIELD_NOT_FOUND if there is no such field in specified class,
     *         StorageError.UNSUPPORTED_INDEX_TYPE exception if type of specified field is not supported by
     *         implementation
     */
    <T> FieldIndex<T> createRandomAccessFieldIndex(Class<?> aType, String[] aFieldNames, boolean aUnique);

    /**
     * Create new multi-field index optimized for access by element position.
     *
     * @param aType objects of which type (or derived from which type) will be included in the index
     * @param aFieldNames names of the index fields. Fields with such name should be present in specified class
     *        <code>type</code>
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @param aCaseInsensitive whether index is case insensitive (ignored for non-string keys)
     * @return Persistent object implementing field index
     * @throws StorageError StorageError.INDEXED_FIELD_NOT_FOUND if there is no such field in specified class,
     *         StorageError.UNSUPPORTED_INDEX_TYPE exception if type of specified field is not supported by
     *         implementation
     */
    <T> FieldIndex<T> createRandomAccessFieldIndex(Class<?> aType, String[] aFieldNames, boolean aUnique,
            boolean aCaseInsensitive);

    /**
     * Create new index optimized for access by element position.
     *
     * @param aType type of the index key (you should path here <code>String.class</code>, <code>int.class</code>, ...)
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @return Persistent object implementing index
     * @throws StorageError StorageError.UNSUPPORTED_INDEX_TYPE exception if specified key type is not supported by
     *         implementation.
     */
    <T> Index<T> createRandomAccessIndex(Class<?> aType, boolean aUnique);

    /**
     * Create new compound index optimized for access by element position.
     *
     * @param aTypes types of the index compound key components
     * @param aUnique whether index is unique (duplicate value of keys are not allowed)
     * @return Persistent object implementing compound index
     * @throws StorageError StorageError.UNSUPPORTED_INDEX_TYPE exception if specified key type is not supported by
     *         implementation.
     */
    <T> Index<T> createRandomAccessIndex(Class<?>[] aTypes, boolean aUnique);

    /**
     * Create relation object. Unlike link which represent embedded relation and stored inside owner object, this
     * Relation object is stand-alone persistent object containing references to owner and members of the relation.
     *
     * @param aOwner A owner of the relation
     * @return Object representing empty relation (relation with specified owner and no members), new members can be
     *         added to the link later.
     */
    <M, O> Relation<M, O> createRelation(O aOwner);

    /**
     * Create new scalable list of persistent objects. This container can efficiently handle small lists as well as
     * large lists When number of members is small, Link class is used to store set members. When number of members
     * exceeds some threshold, PersistentList (based on B-Tree) is used instead.
     *
     * @return Scalable set implementation
     */
    <T> IPersistentList<T> createScalableList();

    /**
     * Create new scalable list of persistent objects. This container can efficiently handle small lists as well as
     * large lists When number of members is small, Link class is used to store set members. When number of members
     * exceeds some threshold, PersistentList (based on B-Tree) is used instead.
     *
     * @param aInitialSize initial allocated size of the list
     * @return Scalable set implementation
     */
    <T> IPersistentList<T> createScalableList(int aInitialSize);

    /**
     * Create new scalable set references to persistent objects. This container can efficiently store small number of
     * references as well as very large number references. When number of members is small, Link class is used to store
     * set members. When number of members exceeds some threshold, PersistentSet (based on B-Tree) is used instead.
     *
     * @return scalable set implementation
     */
    <T> IPersistentSet<T> createScalableSet();

    /**
     * Create new scalable set references to persistent objects. This container can efficiently store small number of
     * references as well as very large number references. When number of members is small, Link class is used to store
     * set members. When number of members exceeds some threshold, PersistentSet (based on B-Tree) is used instead.
     *
     * @param aInitialSize initial size of the set
     * @return Scalable set implementation
     */
    <T> IPersistentSet<T> createScalableSet(int aInitialSize);

    /**
     * Create new persistent set. Implementation of this set is based on B-Tree so it can efficiently handle large
     * number of objects but in case of very small set memory overhead is too high.
     *
     * @return persistent object implementing set
     */
    <T> IPersistentSet<T> createSet();

    /**
     * Create new sorted collection.
     *
     * @param aComparator A comparator class specifying order in the collection
     * @param aUnique Whether index is collection (members with the same key value are not allowed)
     * @return Persistent object implementing sorted collection
     */
    <T> SortedCollection<T> createSortedCollection(PersistentComparator<T> aComparator, boolean aUnique);

    /**
     * Create new spatial index with integer coordinates.
     *
     * @return persistent object implementing spatial index
     */
    <T> SpatialIndex<T> createSpatialIndex();

    /**
     * Create new R2 spatial index.
     *
     * @return persistent object implementing spatial index
     */
    <T> SpatialIndexR2<T> createSpatialIndexR2();

    /**
     * Create new think index (index with large number of duplicated keys).
     *
     * @param aType type of the index key (you should path here <code>String.class</code>, <code>int.class</code>, ...)
     * @return Persistent object implementing index
     * @throws StorageError StorageError.UNSUPPORTED_INDEX_TYPE exception if specified key type is not supported by
     *         implementation.
     */
    <T> Index<T> createThickIndex(Class<?> aType);

    /**
     * Create new time series object.
     *
     * @param aBlockClass class derived from TimeSeries.Block
     * @param aMaxBlockTimeInterval maximal difference in milliseconds between timestamps of the first and the last
     *        elements in a block. If value of this parameter is too small, then most blocks will contains less elements
     *        than preallocated. If it is too large, then searching of block will be inefficient, because index search
     *        will select a lot of extra blocks which do not contain any element from the specified range. Usually the
     *        value of this parameter should be set as (number of elements in block)*(tick interval)*2. Coefficient 2
     *        here is used to compensate for possible holes in time series. For example, if we collect stocks data, we
     *        will have data only for working hours. If number of element in block is 100, time series period is 1 day,
     *        then value of maxBlockTimeInterval can be set as 100*(24*60*60*1000)*2
     * @return New empty time series
     */
    <T extends TimeSeries.Tick> TimeSeries<T> createTimeSeries(Class<?> aBlockClass, long aMaxBlockTimeInterval);

    /**
     * Deallocate object.
     *
     * @param aObj An object to be deallocated
     */
    void deallocate(Object aObj);

    /**
     * Deallocate object.
     *
     * @param aObj An object to be deallocated
     */
    void deallocateObject(Object aObj);

    /**
     * End per-thread transaction started by beginThreadTransaction method.<br>
     * If transaction is <i>exclusive</i>, this method commits the transaction and allows other thread to proceed.<br>
     * If transaction is <i>serializable</i>, this method commits all changes done by this thread and release all locks
     * set by this thread.<br>
     * If transaction is <i>cooperative</i>, this method decrement counter of cooperative transactions and if it becomes
     * zero - commit the work.
     */
    void endThreadTransaction();

    /**
     * End per-thread cooperative transaction with specified maximal delay of transaction commit. When cooperative
     * transaction is ended, data is not immediately committed to the disk (because other cooperative transaction can be
     * active at this moment of time). Instead of it cooperative transaction counter is decremented. Commit is performed
     * only when this counter reaches zero value. But in case of heavy load there can be a lot of requests and so a lot
     * of active cooperative transactions. So transaction counter never reaches zero value. If system crash happens a
     * large amount of work will be lost in this case. To prevent such scenario, it is possible to specify maximal delay
     * of pending transaction commit. In this case when such timeout is expired, new cooperative transaction will be
     * blocked until transaction is committed.
     *
     * @param aMaxDelay maximal delay in milliseconds of committing transaction. Please notice, that Sodbox could not
     *        force other threads to commit their cooperative transactions when this timeout is expired. It will only
     *        block new cooperative transactions to make it possible to current transaction to complete their work. If
     *        <code>maxDelay</code> is 0, current thread will be blocked until all other cooperative transaction are
     *        also finished and changes will be committed to the database.
     */
    void endThreadTransaction(int aMaxDelay);

    /**
     * Export database in XML format.
     *
     * @param aWriter A writer for generated XML document
     */
    void exportXML(Writer aWriter) throws IOException;

    /**
     * Find registered class loaders by name.
     *
     * @param aName A class loader name
     * @return ClassLoader with such name or null if no class loader is found
     */
    ClassLoader findClassLoader(String aName);

    /**
     * Explicit start of garbage collector.
     *
     * @return Number of collected (deallocated) objects
     */
    int gc();

    /**
     * Get class loader used to locate classes for loaded class descriptors.
     *
     * @return ClassLoader previously set by <code>setClassLoader</code> method or <code>null</code> if not specified.
     */
    ClassLoader getClassLoader();

    /**
     * Get version of database format for this database. When new database is created it is always assigned the current
     * database format version.
     *
     * @return Database format version
     */
    int getDatabaseFormatVersion();

    /**
     * Get size of the database
     */
    long getDatabaseSize();

    /**
     * Get storage listener.
     *
     * @return current storage listener
     */
    StorageListener getListener();

    /**
     * Get database memory dump. This function returns hashmap which key is classes of stored objects and value -
     * MemoryUsage object which specifies number of instances of particular class in the storage and total size of
     * memory used by these instance. Size of internal database structures (object index,* memory allocation bitmap) is
     * associated with <code>Storage</code> class. Size of class descriptors - with <code>java.lang.Class</code> class.
     * <p>
     * This method traverse the storage as garbage collection do - starting from the root object and recursively
     * visiting all reachable objects. So it reports statistic only for visible objects. If total database size is
     * significantly larger than total size of all instances reported by this method, it means that there is garbage in
     * the database. You can explicitly invoke garbage collector in this case.
     * </p>
     */
    HashMap<Class<?>, MemoryUsage> getMemoryDump();

    /**
     * Retrieve object by OID. This method should be used with care because if object is deallocated, its OID can be
     * reused. In this case getObjectByOID will return reference to the new object with may be different type.
     *
     * @param aOID An object OID
     * @return Reference to the object with specified OID
     */
    Object getObjectByOID(int aOID);

    /**
     * Get object identifier.
     *
     * @param aObj An inspected object
     */
    int getOid(Object aObj);

    /**
     * Get all set properties.
     *
     * @return All properties set by setProperty or setProperties method
     */
    Properties getProperties();

    /**
     * Get property value.
     *
     * @param aName property name
     * @return Value of the property previously assigned by setProperty or setProperties method or <code>null</code> if
     *         property was not set
     */
    Object getProperty(String aName);

    /**
     * Get storage root. Storage can have exactly one root object. If you need to have several root object and access
     * them by name (as is is possible in many other OODBMSes), you should create index and use it as root object.
     *
     * @return Root object or <code>null</code> if root is not specified (storage is not yet initialized)
     */
    Object getRoot();

    /**
     * This method is used internally by Sodbox to get transaction context associated with current thread. But it can be
     * also used by application to get transaction context, store it in some variable and use in another thread. I will
     * make it possible to share one transaction between multiple threads.
     *
     * @return Transaction context associated with current thread
     */
    ThreadTransactionContext getTransactionContext();

    /**
     * Get total size of all allocated objects in the database.
     */
    long getUsedSize();

    /**
     * Import data from XML file.
     *
     * @param aReader XML document reader
     * @throws XMLImportException If import didn't succeed
     */
    void importXML(Reader aReader) throws XMLImportException;

    /**
     * Check if nested thread transaction is active.
     *
     * @return true if code executing this method is inside per-thread transaction (serializable, exclusive or
     *         cooperative)
     */
    boolean isInsideThreadTransaction();

    /**
     * Check if database is opened.
     *
     * @return <code>true</code> if database was opened by <code>open</code> method, <code>false</code> otherwise
     */
    boolean isOpened();

    /**
     * Join results of several index searches. This method efficiently join selections without loading objects
     * themselves
     *
     * @param aSelections Selections to be merged
     * @return Iterator through joined result
     */
    Iterator<?> join(Iterator<?>[] aSelections);

    /**
     * Load raw object.
     *
     * @param aObj A loaded object
     */
    void load(Object aObj);

    /**
     * Load raw object.
     *
     * @param aObj A loaded object
     */
    void loadObject(Object aObj);

    /**
     * Locks an object.
     *
     * @param aObj An object to lock
     * @return If the object was locked
     */
    boolean lockObject(Object aObj);

    /**
     * Explicitly make object persistent. Usually objects are made persistent implicitly using "persistent on
     * reachability approach", but this method allows to do it explicitly. If object is already persistent, execution of
     * this method has no effect.
     *
     * @param aObj object to be made persistent
     * @return OID assigned to the object
     */
    int makePersistent(Object aObj);

    /**
     * Merge results of several index searches. This method efficiently merge selections without loading objects
     * themselves
     *
     * @param aSelections selections to be merged
     * @return Iterator through merged result
     */
    Iterator<?> merge(Iterator<?>[] aSelections);

    /**
     * Mark object as been modified.
     *
     * @param aObj modified object
     */
    void modify(Object aObj);

    /**
     * Mark object as been modified.
     *
     * @param aObj modified object
     */
    void modifyObject(Object aObj);

    /**
     * Open the storage with default page pool size.
     *
     * @param aFile A user specific implementation of IFile interface
     */
    void open(IFile aFile);

    /**
     * Open the storage.
     *
     * @param aFile A user specific implementation of IFile interface
     * @param aPagePoolSize A size of page pool (in bytes). Page pool should contain at least ten 4kb pages, so minimal
     *        page pool size should be at least 40Kb. But larger page pool usually leads to better performance (unless
     *        it could not fit in memory and cause swapping).
     */
    void open(IFile aFile, long aPagePoolSize);

    /**
     * Open the storage with default page pool size.
     *
     * @param aFilePath path to the database file
     */
    void open(String aFilePath);

    /**
     * Open the storage.
     *
     * @param aFilePath A path to the database file
     * @param aPagePoolSize A size of page pool (in bytes). Page pool should contain at least ten 4kb pages, so minimal
     *        page pool size should be at least 40Kb. But larger page pool usually leads to better performance (unless
     *        it could not fit in memory and cause swapping). Value 0 of this parameter corresponds to infinite page
     *        pool (all pages are cashed in memory). It is especially useful for in-memory database, when storage is
     *        created with NullFile.
     */
    void open(String aFilePath, long aPagePoolSize);

    /**
     * Open the encrypted storage.
     *
     * @param aFilePath A path to the database file
     * @param aPagePoolSize A size of page pool (in bytes). Page pool should contain at least then 4kb pages, so minimal
     *        page pool size should be at least 40Kb. But larger page pool usually leads to better performance (unless
     *        it could not fit in memory and cause swapping).
     * @param aCipherKey A cipher key
     */
    void open(String aFilePath, long aPagePoolSize, String aCipherKey);

    /**
     * Register named class loader in the storage. Mechanism of named class loaders allows to store in database
     * association between class and its class loader. All named class loaders should be registered before database
     * open.
     *
     * @param aClassLoader A registered named class loader
     */
    void registerClassLoader(INamedClassLoader aClassLoader);

    /**
     * Register custom allocator for specified class. Instances of this and derived classes will be allocated in the
     * storage using specified allocator.
     *
     * @param aClass A class of the persistent object which instances will be allocated using this allocator
     * @param aAllocator A custom allocator
     */
    void registerCustomAllocator(Class<?> aClass, CustomAllocator aAllocator);

    /**
     * Rollback changes made by the last transaction. By default, Sodbox doesn't reload modified objects after a
     * transaction rollback. In this case, the programmer should not use references to the persistent objects stored in
     * program variables. Instead, the application should fetch the object tree from the beginning, starting from
     * obtaining the root object using the Storage.getRoot method. Setting the "sodbox.reload.objects.on.rollback"
     * property instructs Sodbox to reload all objects modified by the aborted (rolled back) transaction. It takes
     * additional processing time, but in this case it is not necessary to ignore references stored in variables, unless
     * they point to the objects created by this transactions (which were invalidated when the transaction was rolled
     * back). Unfortunately, there is no way to prohibit access to such objects or somehow invalidate references to
     * them. So this option should be used with care.
     */
    void rollback();

    /**
     * Rollback serializable transaction. This call is equivalent to <code>rollbackThreadTransaction</code> but it
     * checks that serializable transaction was previously started using <code>beginSerializableTransaction()</code>
     * method.
     *
     * @throws StorageError NOT_IN_TRANSACTION if this method is invoked outside serializable transaction body
     */
    void rollbackSerializableTransaction();

    /**
     * Rollback per-thread transaction. It is safe to use this method only for exclusive transactions. In case of
     * cooperative transactions, this method rollback results of all transactions.
     */
    void rollbackThreadTransaction();

    /**
     * Set class loader. This class loader will be used to locate classes for loaded class descriptors. If class loader
     * is not specified or it did find the class, then <code>Class.forName()</code> method will be used to get class for
     * the specified name.
     *
     * @param aClassLoader A class loader
     * @return Previous ClassLoader or null if not specified
     */
    ClassLoader setClassLoader(ClassLoader aClassLoader);

    /**
     * Set custom serializer used for packing/unpacking fields of persistent objects which types implement
     * CustomSerializable interface.
     *
     * @param aSerializer A custom serializer
     */
    void setCustomSerializer(CustomSerializer aSerializer);

    /**
     * Set threshold for initiation of garbage collection. By default garbage collection is disable (threshold is set to
     * Long.MAX_VALUE). If it is set to the value different from Long.MAX_VALUE, GC will be started each time when delta
     * between total size of allocated and deallocated objects exceeds specified threshold OR after reaching end of
     * allocation bitmap in allocator.
     *
     * @param aAllocatedDelta A delta between total size of allocated and deallocated object since last GC or storage
     *        opening
     */
    void setGcThreshold(long aAllocatedDelta);

    /**
     * Set storage listener.
     *
     * @param aListener A new storage listener (may be null)
     * @return Previous storage listener
     */
    StorageListener setListener(StorageListener aListener);

    /**
     * Set database properties. This method should be invoked before opening database. For list of supported properties
     * please see <code>setProperty</code> command. All not recognized properties are ignored.
     *
     * @param aProps Database properties
     */
    void setProperties(Properties aProps);

    /**
     * Set database property. This method should be invoked before opening database. Currently the following boolean
     * properties are supported:
     * <TABLE>
     * <TR>
     * <TH>Property name</TH>
     * <TH>Parameter type</TH>
     * <TH>Default value</TH>
     * <TH>Description</TH>
     * </TR>
     * <TR>
     * <TD><code>sodbox.implicit.values</code></TD>
     * <TD>Boolean</TD>
     * <TD>false</TD>
     * <TD>Treat any class not derived from IPersistent as <i>value</i>. This object will be embedded inside persistent
     * object containing reference to this object. If this object is referenced from N persistent object, N instances of
     * this object will be stored in the database and after loading there will be N instances in memory. As well as
     * persistent capable classes, value classes should have default constructor (constructor with empty list of
     * parameters) or has no constructors at all. For example <code>Integer</code> class can not be stored as value in
     * Sodbox because it has no such constructor. In this case serialization mechanism can be used (see below)</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.serialize.transient.objects</code></TD>
     * <TD>Boolean</TD>
     * <TD>false</TD>
     * <TD>Serialize any class not derived from IPersistent or IValue using standard Java serialization mechanism.
     * Packed object closure is stored in database as byte array. Latter the same mechanism is used to unpack the
     * objects. To be able to use this mechanism object and all objects referenced from it should implement
     * <code>java.io.Serializable</code> interface and should not contain references to persistent objects. If such
     * object is referenced from N persistent object, N instances of this object will be stored in the database and
     * after loading there will be N instances in memory.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.object.cache.init.size</code></TD>
     * <TD>Integer</TD>
     * <TD>1319</TD>
     * <TD>Initial size of object cache</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.object.cache.kind</code></TD>
     * <TD>String</TD>
     * <TD>"lru"</TD>
     * <TD>Kind of object cache. The following values are supported: "strong", "weak", "soft", "pinned", "lru".
     * <B>Strong</B> cache uses strong (normal) references to refer persistent objects. Thus none of loaded persistent
     * objects can be deallocated by GC. <B>Weak</B> cache use a weak references and soft cache - <B>soft</B>
     * references. The main difference between soft and weak references is that garbage collector is not required to
     * remove soft referenced objects immediately when object is detected to be <i>soft referenced</i>, so it may
     * improve caching of objects. But it also may increase amount of memory used by application, and as far as
     * persistent object requires finalization it can cause memory overflow even though garbage collector is required to
     * clear all soft references before throwing OutOfMemoryException.<br>
     * But Java specification says nothing about the policy used by GC for soft references (except the rule mentioned
     * above). Unlike it <B>lru</B> cache provide determined behavior, pinning most recently used objects in memory.
     * Number of pinned objects is determined for lru cache by <code>sodbox.object.index.init.size</code> parameter (it
     * can be 0).<br>
     * Pinned object cache pin in memory all modified objects while using weak referenced for non-modified objects. This
     * kind of cache eliminate need in finalization mechanism - all modified objects are kept in memory and are flushed
     * to the disk only at the end of transaction. So the size of transaction is limited by amount of main memory.
     * Non-modified objects are accessed only through weak references so them are not protected from GC and can be
     * thrown away.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.object.index.init.size</code></TD>
     * <TD>Integer</TD>
     * <TD>1024</TD>
     * <TD>Initial size of object index (specifying large value increase initial size of database, but reduce number of
     * index reallocations)</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.extension.quantum</code></TD>
     * <TD>Long</TD>
     * <TD>1048576</TD>
     * <TD>Object allocation bitmap extension quantum. Memory is allocate by scanning bitmap. If there is no large
     * enough hole, then database is extended by the value of dbDefaultExtensionQuantum. This parameter should not be
     * smaller than 64Kb.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.gc.threshold</code></TD>
     * <TD>Long</TD>
     * <TD>Long.MAX_VALUE</TD>
     * <TD>Threshold for initiation of garbage collection. If it is set to the value different from Long.MAX_VALUE, GC
     * will be started each time when delta between total size of allocated and deallocated objects exceeds specified
     * threshold OR after reaching end of allocation bitmap in allocator.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.lock.file</code></TD>
     * <TD>Boolean</TD>
     * <TD>false</TD>
     * <TD>Lock database file to prevent concurrent access to the database by more than one application.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.file.readonly</code></TD>
     * <TD>Boolean</TD>
     * <TD>false</TD>
     * <TD>Database file should be opened in read-only mode.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.file.noflush</code></TD>
     * <TD>Boolean</TD>
     * <TD>false</TD>
     * <TD>Do not flush file during transaction commit. It will greatly increase performance because eliminate
     * synchronous write to the disk (when program has to wait until all changed are actually written to the disk). But
     * it can cause database corruption in case of OS or power failure (but abnormal termination of application itself
     * should not cause the problem, because all data which were written to the file, but is not yet saved to the disk
     * is stored in OS file buffers and sooner or later them will be written to the disk)</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.alternative.btree</code></TD>
     * <TD>Boolean</TD>
     * <TD>false</TD>
     * <TD>Use alternative implementation of B-Tree (not using direct access to database file pages). This
     * implementation should be used in case of serialized per thread transactions. New implementation of B-Tree will be
     * used instead of old implementation if "sodbox.alternative.btree" property is set. New B-Tree has incompatible
     * format with old B-Tree, so you could not use old database or XML export file with new indices. Alternative B-Tree
     * is needed to provide serializable transaction (old one could not be used). Also it provides better performance
     * (about 3 times compared with old implementation) because of object caching. And B-Tree supports keys of user
     * defined types.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.background.gc</code></TD>
     * <TD>Boolean</TD>
     * <TD>false</TD>
     * <TD>Perform garbage collection in separate thread without blocking the main application.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.string.encoding</code></TD>
     * <TD>String</TD>
     * <TD>null</TD>
     * <TD>Specifies encoding of storing strings in the database. By default Sodbox stores strings as sequence of chars
     * (two bytes per char). If all strings in application are in the same language, then using encoding can
     * significantly reduce space needed to store string (about two times). But please notice, that this option has
     * influence on all strings stored in database. So if you already have some data in the storage and then change
     * encoding, then it will cause database crash.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.replication.ack</code></TD>
     * <TD>Boolean</TD>
     * <TD>false</TD>
     * <TD>Request acknowledgment from slave that it receives all data before transaction commit. If this option is not
     * set, then replication master node just writes data to the socket not warring whether it reaches slave node or
     * not. When this option is set to true, master not will wait during each transaction commit acknowledgment from
     * slave node. Please notice that this option should be either set or not set both at slave and master node. If it
     * is set only on one of this nodes then behavior of the system is unpredicted. This option can be used both in
     * synchronous and asynchronous replication mode. The only difference is that in first case main application thread
     * will be blocked waiting for acknowledgment, while in the asynchronous mode special replication thread will be
     * blocked allowing thread performing commit to proceed.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.concurrent.iterator</code></TD>
     * <TD>Boolean</TD>
     * <TD>false</TD>
     * <TD>By default iterator will throw ConcurrentModificationException if iterated collection was changed outside
     * iterator, when the value of this property is true then iterator will try to restore current position and continue
     * iteration</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.slave.connection.timeout</code></TD>
     * <TD>Integer</TD>
     * <TD>60</TD>
     * <TD>Timeout in seconds during which master node will try to establish connection with slave node. If connection
     * can not be established within specified time, then master will not perform replication to this slave node</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.force.store</code></TD>
     * <TD>Boolean</TD>
     * <TD>true</TD>
     * <TD>When the value of this parameter is true Storage.makePersistent method cause immediate storing of object in
     * the storage, otherwise object is assigned OID and is marked as modified. Storage.makePersistent method is mostly
     * used when object is inserted in B-Tree. If application put in index object referencing a large number of other
     * objects which also has to be made persistent, then marking object as modified instead of immediate storing may
     * cause memory overflow because garbage collector and finalization threads will store objects with less speed than
     * application creates new ones. But if object will be updated after been placed in B-Tree, then immediate store
     * will just cause cause extra overhead, because object has to be stored twice.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.page.pool.lru.limit</code></TD>
     * <TD>Long</TD>
     * <TD>1L << 60</TD>
     * <TD>Set boundary for caching database pages in page pool. By default Sodbox is using LRU algorithm for finding
     * candidate for replacement. But for example for BLOBs this strategy is not optimal and fetching BLOB can cause
     * flushing the whole page pool if LRU discipline is used. And with high probability fetched BLOB pages will no be
     * used any more. So it is preferable not to cache BLOB pages at all (throw away such page immediately when it is
     * not used any more). This parameter in conjunction with custom allocator allows to disable caching for BLOB
     * objects. If you set value of "sodbox.page.lru.scope" property equal to base address of custom allocator (which
     * will be used to allocate BLOBs), then page containing objects allocated by this allocator will not be cached in
     * page pool.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.multiclient.support</code></TD>
     * <TD>Boolean</TD>
     * <TD>false</TD>
     * <TD>Supports access to the same database file by multiple applications. In this case Sodbox will use file locking
     * to synchronize access to the database file. An application MUST wrap any access to the database with
     * beginThreadThreansaction/endThreadTransaction methods. For read only access use READ_ONLY_TRANSACTION mode and if
     * transaction may modify database then READ_WRITE_TRANSACTION mode should be used.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.reload.objects.on.rollback</code></TD>
     * <TD>Boolean</TD>
     * <TD>false</TD>
     * <TD>By default, Sodbox doesn't reload modified objects after a transaction rollback. In this case, the programmer
     * should not use references to the persistent objects stored in program variables. Instead, the application should
     * fetch the object tree from the beginning, starting from obtaining the root object using the Storage.getRoot
     * method. Setting the "sodbox.reload.objects.on.rollback" property instructs Sodbox to reload all objects modified
     * by the aborted (rolled back) transaction. It takes additional processing time, but in this case it is not
     * necessary to ignore references stored in variables, unless they point to the objects created by this transactions
     * (which were invalidated when the transaction was rolled back). Unfortunately, there is no way to prohibit access
     * to such objects or somehow invalidate references to them. So this option should be used with care.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.reuse.oid</code></TD>
     * <TD>bool</TD>
     * <TD>true</TD>
     * <TD>This parameters allows to disable reusing OID of deallocated objects. It can simplify debugging of
     * application performing explicit deallocation of objects (not using garbage collection). Explicit object
     * deallocation can cause "dangling references" problem - when there are live references to the deallocated object.
     * Access to such object should cause <code>StorageError(DELETED_OBJECT)</code> exception. But if OID of the object
     * can be reused and assigned to some newly deallocated object, then we will get type cast or field access errors
     * when try to work with this object. In the worst case OID will be reused by the object of the same type - then
     * application will not notice that referenced object was substituted. Disabling reuse of OID allows to eliminate
     * such unpredictable behavior - access to the deallocated object will always cause
     * <code>StorageError(DELETED_OBJECT)</code> exception. But please notice that disabling reuse of OID for a long
     * time and intensive allocation/deallocation of objects can cause exhaustion of OID space (2Gb).</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.compatibilit.mode</code></TD>
     * <TD>int</TD>
     * <TD>0</TD>
     * <TD>Bitmask of different compatibility modes. Right now the following modes are supported:
     * IBM_JAVA5_COMPATIBILITY_MODE: compatibility with databases created by Sodbox prior to 2.73 release by IBM Java5
     * VM CLASS_LOADER_SERIALIZATION_COMPATIBILITY_MODE: compatibility with databases created by Sodbox prior to 2.66
     * and using custom class loaders and serialization of transient objects</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.global.class.extent</code></TD>
     * <TD>bool</TD>
     * <TD>true</TD>
     * <TD>This parameter is used by Database class in "auto register table" mode. In this mode Sodbox automatically
     * creates indices (class extents) for all derived classes of the inserted object if there are no such class extents
     * yet. It include class extent for Persistent class allowing to enumerate all objects in the storage. If such list
     * is not needed, then this option can be set to false to eliminate extra index maintenance overhead.</TD>
     * </TR>
     * <TR>
     * <TD><code>sodbox.search.base.classes</code></TD>
     * <TD>bool</TD>
     * <TD>true</TD>
     * <TD>This parameter is used by Database class. If there is no table (class extent) corresponding to the requested
     * class, then Sodbox tries to locate class extend for base class and so on. By setting this property to false it is
     * possible to prohibit lookup of base classes. Please notice that lookup on base classes is also not performed if
     * "auto register table" mode is active.</TD>
     * </TR>
     * </TABLE>
     *
     * @param aName A name of the property
     * @param aValue A value of the property (for boolean properties pass <code>Boolean.TRUE</code> and
     *        <code>Boolean.FALSE</code>
     */
    void setProperty(String aName, Object aValue);

    // Internal methods

    /**
     * Enable or disable recursive loading for specified class. Recursive loading can be also controlled by overriding
     * recursiveLoading method of Persistent class, but if class is not derived from Persistent base class and not
     * implementing IPersistent interface, this method can be used to control recursive loading.
     *
     * @param aType Class for which recursive loading policy is specified. By default recursive loading is enabled for
     *        all classes. Disabling recursive loading for some class also affect all derived classes unless policy is
     *        explicitly specified for such class.
     * @param aEnabled Whether recursive loading is enabled or disabled for this class
     * @return Previous status of recursive loading policy for the specified class
     */
    boolean setRecursiveLoading(Class<?> aType, boolean aEnabled);

    /**
     * Set new storage root object. Previous reference to the root object is rewritten but old root is not automatically
     * deallocated.
     *
     * @param aRoot object to become new storage root. If it is not persistent yet, it is made persistent and stored in
     *        the storage
     */
    void setRoot(Object aRoot);

    /**
     * Associate transaction context with the thread This method can be used by application to share the same
     * transaction between multiple threads.
     *
     * @param aContext A new transaction context
     * @return Transaction context previously associated with this thread
     */
    ThreadTransactionContext setTransactionContext(ThreadTransactionContext aContext);

    /**
     * Store object in storage.
     *
     * @param aObj An object to store
     */
    void store(Object aObj);

    /**
     * Store object in storage.
     *
     * @param aObj An object to store
     */
    void storeFinalizedObject(Object aObj);

    /**
     * Store object in storage.
     *
     * @param aObj An object to store
     */
    void storeObject(Object aObj);

    /**
     * Throw object.
     *
     * @param aObj An object
     */
    void throwObject(Object aObj);

}
