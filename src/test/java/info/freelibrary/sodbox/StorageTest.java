/**
 *
 */

package info.freelibrary.sodbox;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import info.freelibrary.sodbox.fulltext.FullTextSearchHelper;
import info.freelibrary.sodbox.impl.ThreadTransactionContext;
import info.freelibrary.util.Logger;
import info.freelibrary.util.LoggerFactory;

/**
 *
 */
public class StorageTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageTest.class, Constants.MESSAGES);

    private static final int PAGE_POOL_SIZE = 32 * 1024 * 1024;

    private static final String TMP_DIR = System.getProperty("java.io.tmpdir") + File.separator;

    private static final String RANDOM_STRING = UUID.randomUUID().toString();

    private Storage myStorage;

    private String myDbPath;

    /**
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        myStorage = StorageFactory.getInstance().createStorage();
        myDbPath = TMP_DIR + UUID.randomUUID().toString();
    }

    /**
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        if (myStorage.isOpened()) {
            myStorage.close();
        }

        if (myDbPath != null) {
            final File dbFile = new File(myDbPath);

            if (dbFile.exists() && !dbFile.delete()) {
                LOGGER.error(MessageCodes.SBT_004, dbFile);
            }
        }
    }

    /**
     * Test method for {@link Storage#open(String, long)}.
     */
    @Test
    public final void testOpenStringLong() {
        myStorage.open(myDbPath, PAGE_POOL_SIZE);
    }

    /**
     * Test method for {@link Storage#open(IFile, long)}.
     */
    @Test
    public final void testOpenIFileLong() {
        myStorage.open(new NullFile(), Storage.INFINITE_PAGE_POOL);
    }

    /**
     * Test method for {@link Storage#open(IFile)}.
     */
    @Test
    public final void testOpenIFile() {
        myStorage.open(new NullFile());
    }

    /**
     * Test method for {@link Storage#open(String)}.
     */
    @Test
    public final void testOpenString() {
        myStorage.open(myDbPath);
    }

    /**
     * Test method for {@link Storage#open(String, long, String)}.
     */
    @Test
    public final void testOpenStringLongString() {
        myStorage.open(myDbPath, 40000, RANDOM_STRING);
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#isOpened()}.
     */
    @Test
    public final void testIsOpened() {
        myStorage.open(new NullFile());

        assertTrue(myStorage.isOpened());
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#getRoot()}.
     */
    @Test
    public final void testGetRoot() {
        myStorage.open(new NullFile());

        Indices root = (Indices) myStorage.getRoot();

        assertNull(root);

        if (root == null) {
            root = new Indices();
            root.myStringIndex = myStorage.createIndex(String.class, true);
            root.myIntIndex = myStorage.createIndex(long.class, true);

            myStorage.setRoot(root);

            root = (Indices) myStorage.getRoot();

            assertNotNull(root);
        }
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#setRoot(java.lang.Object)}.
     */
    @Test
    public final void testSetRoot() {
        final Indices root = new Indices();

        myStorage.open(new NullFile());

        root.myStringIndex = myStorage.createIndex(String.class, true);
        root.myIntIndex = myStorage.createIndex(long.class, true);

        myStorage.setRoot(root);

        assertNotNull(myStorage.getRoot());
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#commit()}.
     */
    @Test
    public final void testCommit() {
        final Record record = new Record();

        myStorage.open(new NullFile());
        myStorage.setRoot(record);

        assertNotNull(myStorage.getRoot());

        myStorage.commit();
        myStorage.rollback();

        assertNotNull(myStorage.getRoot());
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#rollback()}.
     */
    @Test
    public final void testRollback() {
        final Record record = new Record();

        myStorage.open(new NullFile());
        myStorage.setRoot(record);

        assertNotNull(myStorage.getRoot());

        myStorage.rollback();

        assertNull(myStorage.getRoot());
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#backup(java.io.OutputStream)}.
     */
    @Test
    public final void testBackupOutputStream() throws IOException {
        final Record record = new Record();
        final String key = UUID.randomUUID().toString();
        final File file = new File(TMP_DIR, key);
        final FileOutputStream fileOutStream = new FileOutputStream(file);

        record.myIntKey = 23;
        record.myStringKey = key;

        myStorage.open(new NullFile());
        myStorage.setRoot(record);

        myStorage.backup(fileOutStream);
        fileOutStream.close();

        myStorage.close();
        myStorage.open(file.getAbsolutePath());

        assertEquals(key, ((Record) myStorage.getRoot()).myStringKey);
        file.delete();
    }

    /**
     * Test method for {@link Storage#backup(String, String)}.
     */
    @Test
    public final void testBackupStringString() throws IOException {
        final Record record = new Record();
        final String key = UUID.randomUUID().toString();
        final File file = new File(TMP_DIR, key);

        record.myIntKey = 23;
        record.myStringKey = key;

        myStorage.open(new NullFile());
        myStorage.setRoot(record);

        myStorage.backup(file.getAbsolutePath(), RANDOM_STRING);

        myStorage.close();
        myStorage.open(file.getAbsolutePath(), Storage.INFINITE_PAGE_POOL, RANDOM_STRING);

        assertEquals(key, ((Record) myStorage.getRoot()).myStringKey);
        file.delete();
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#beginThreadTransaction(int)}.
     */
    @Test
    public final void testBeginExclusiveTransaction() {
        final Record record = new Record();

        myStorage.open(new NullFile());
        myStorage.beginExclusiveTransaction();
        myStorage.setRoot(record);
        myStorage.endExclusiveTransaction();
        myStorage.rollback();

        assertNotNull(myStorage.getRoot());
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#beginThreadTransaction(int)}.
     */
    @Test
    public final void testBeginCooperativeTransaction() {
        final Record record = new Record();

        myStorage.open(new NullFile());
        myStorage.beginCooperativeTransaction();
        myStorage.setRoot(record);
        myStorage.endCooperativeTransaction();
        myStorage.rollback();

        assertNotNull(myStorage.getRoot());
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#beginThreadTransaction(int)}.
     * <p>
     * WORKS
     * </p>
     */
    @Test
    public final void testBeginSerializableTransaction() {
        final Record record = new Record();

        myStorage.open(myDbPath);
        myStorage.beginSerializableTransaction();
        myStorage.setRoot(record);
        record.exclusiveLock();
        record.myStringKey = RANDOM_STRING;
        record.modify();
        myStorage.endSerializableTransaction();

        assertEquals(RANDOM_STRING, ((Record) myStorage.getRoot()).myStringKey);
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#endThreadTransaction(int)}.
     */
    @Test
    public final void testEndExclusiveTransaction() {
        final Record record = new Record();

        myStorage.open(new NullFile());
        myStorage.beginExclusiveTransaction();
        myStorage.setRoot(record);
        myStorage.endExclusiveTransaction();
        myStorage.rollback();

        assertNotNull(myStorage.getRoot());
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#endThreadTransaction(int)}.
     */
    @Test
    public final void testEndCooperativeTransactionInt() {
        final Record record = new Record();

        myStorage.open(new NullFile());
        myStorage.beginCooperativeTransaction();
        myStorage.setRoot(record);
        myStorage.endCooperativeTransaction(Integer.MAX_VALUE);
        myStorage.rollback();

        assertNotNull(myStorage.getRoot());
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#isInsideThreadTransaction()}.
     */
    @Test
    public final void testIsInsideThreadTransaction() {
        myStorage.open(new NullFile());
        myStorage.beginSerializableTransaction();
        assertTrue(myStorage.isInsideThreadTransaction());
        myStorage.endSerializableTransaction();
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#isInsideThreadTransaction()}.
     */
    @Test
    public final void testIsNotInsideThreadTransaction() {
        myStorage.open(new NullFile());
        assertFalse(myStorage.isInsideThreadTransaction());
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#rollbackThreadTransaction()}.
     */
    @Test
    public final void testRollbackExclusiveTransaction() {
        final Record record = new Record();

        myStorage.open(new NullFile());
        myStorage.beginExclusiveTransaction();
        myStorage.setRoot(record);
        myStorage.rollbackExclusiveTransaction();

        assertNull(myStorage.getRoot());
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#rollbackThreadTransaction()}.
     */
    @Test
    public final void testRollbackCooperativeTransaction() {
        final Record record = new Record();

        myStorage.open(new NullFile());
        myStorage.beginCooperativeTransaction();
        myStorage.setRoot(record);
        myStorage.rollbackCooperativeTransaction();

        assertNull(myStorage.getRoot());
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#rollbackThreadTransaction()}.
     */
    @Test
    public final void testRollbackSerializableTransaction() {
        final Record record = new Record();

        // myStorage.setProperty("sodbox.alternative.btree", Boolean.TRUE);
        myStorage.open(myDbPath);
        myStorage.beginSerializableTransaction();
        myStorage.setRoot(record);
        record.exclusiveLock();
        record.myStringKey = RANDOM_STRING;
        record.modify();
        myStorage.rollbackSerializableTransaction();

        // This rolls back the record edits but not the root element setting
        assertNotNull(myStorage.getRoot());

        assertNotEquals(RANDOM_STRING, ((Record) myStorage.getRoot()).myStringKey);
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#endSerializableTransaction()}.
     */
    @Test
    public final void testEndSerializableTransaction() {
        final Record record = new Record();

        myStorage.open(myDbPath);
        myStorage.beginSerializableTransaction();
        myStorage.setRoot(record);
        record.exclusiveLock();
        record.myStringKey = RANDOM_STRING;
        record.modify();
        myStorage.endSerializableTransaction();

        assertEquals(RANDOM_STRING, ((Record) myStorage.getRoot()).myStringKey);
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createList()}.
     */
    @Test
    public final void testCreateList() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createScalableList()}.
     */
    @Test
    public final void testCreateScalableList() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createScalableList(int)}.
     */
    @Test
    public final void testCreateScalableListInt() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createHash()}.
     */
    @Test
    public final void testCreateHash() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createHash(int, int)}.
     */
    @Test
    public final void testCreateHashIntInt() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createMap(java.lang.Class)}.
     */
    @Test
    public final void testCreateMapClass() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createMap(java.lang.Class, int)}.
     */
    @Test
    public final void testCreateMapClassInt() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createSet()}.
     */
    @Test
    public final void testCreateSet() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createBag()}.
     */
    @Test
    public final void testCreateBag() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createScalableSet()}.
     */
    @Test
    public final void testCreateScalableSet() {

    }

    /**
     * Test method for {@link Storage#createScalableSet(int)}.
     */
    @Test
    public final void testCreateScalableSetInt() {

    }

    /**
     * Test method for {@link Storage#createIndex(Class, boolean)}.
     */
    @Test
    public final void testCreateIndexClassBoolean() {

    }

    /**
     * Test method for {@link Storage#createIndex(Class[], boolean)}.
     */
    @Test
    public final void testCreateIndexClassArrayBoolean() {

    }

    /**
     * Test method for {@link Storage#createMultidimensionalIndex(MultidimensionalComparator)}.
     */
    @Test
    public final void testCreateMultidimensionalIndexMultidimensionalComparatorOfT() {

    }

    /**
     * Test method for {@link Storage#createMultidimensionalIndex(Class, String[], boolean)}.
     */
    @Test
    public final void testCreateMultidimensionalIndexClassStringArrayBoolean() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createThickIndex(java.lang.Class)}.
     */
    @Test
    public final void testCreateThickIndex() {

    }

    /**
     * Test method for
     * {@link info.freelibrary.sodbox.Storage#createFieldIndex(java.lang.Class, java.lang.String, boolean)}.
     */
    @Test
    public final void testCreateFieldIndexClassStringBoolean() {

    }

    /**
     * Test method for
     * {@link info.freelibrary.sodbox.Storage#createFieldIndex(java.lang.Class, java.lang.String, boolean, boolean)}.
     */
    @Test
    public final void testCreateFieldIndexClassStringBooleanBoolean() {

    }

    /**
     * Test method for {@link Storage#createFieldIndex(Class, String, boolean, boolean, boolean)}.
     */
    @Test
    public final void testCreateFieldIndexClassStringBooleanBooleanBoolean() {

    }

    /**
     * Test method for {@link Storage#createFieldIndex(Class, String[], boolean)}.
     */
    @Test
    public final void testCreateFieldIndexClassStringArrayBoolean() {

    }

    /**
     * Test method for {@link Storage#createFieldIndex(Class, String[], boolean, boolean)}.
     */
    @Test
    public final void testCreateFieldIndexClassStringArrayBooleanBoolean() {

    }

    /**
     * Test method for {@link Storage#createRandomAccessIndex(Class, boolean)}.
     */
    @Test
    public final void testCreateRandomAccessIndexClassBoolean() {

    }

    /**
     * Test method for {@link Storage#createRandomAccessIndex(Class[], boolean)}.
     */
    @Test
    public final void testCreateRandomAccessIndexClassArrayBoolean() {

    }

    /**
     * Test method for {@link Storage#createRandomAccessFieldIndex(Class, String, boolean)}.
     */
    @Test
    public final void testCreateRandomAccessFieldIndexClassStringBoolean() {

    }

    /**
     * Test method for {@link Storage#createRandomAccessFieldIndex(Class, String, boolean, boolean)}.
     */
    @Test
    public final void testCreateRandomAccessFieldIndexClassStringBooleanBoolean() {

    }

    /**
     * Test method for {@link Storage#createRandomAccessFieldIndex(Class, String[], boolean)}.
     */
    @Test
    public final void testCreateRandomAccessFieldIndexClassStringArrayBoolean() {

    }

    /**
     * Test method for {@link Storage#createRandomAccessFieldIndex(Class, String[], boolean, boolean)}.
     */
    @Test
    public final void testCreateRandomAccessFieldIndexClassStringArrayBooleanBoolean() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createSpatialIndex()}.
     */
    @Test
    public final void testCreateSpatialIndex() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createSpatialIndexR2()}.
     */
    @Test
    public final void testCreateSpatialIndexR2() {

    }

    /**
     * Test method for {@link Storage#createSortedCollection(PersistentComparator, boolean)}.
     */
    @Test
    public final void testCreateSortedCollection() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createLink()}.
     */
    @Test
    public final void testCreateLink() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createLink(int)}.
     */
    @Test
    public final void testCreateLinkInt() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createRelation(java.lang.Object)}.
     */
    @Test
    public final void testCreateRelation() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createBlob()}.
     */
    @Test
    public final void testCreateBlob() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createRandomAccessBlob()}.
     */
    @Test
    public final void testCreateRandomAccessBlob() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createTimeSeries(java.lang.Class, long)}.
     */
    @Test
    public final void testCreateTimeSeries() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createPatriciaTrie()}.
     */
    @Test
    public final void testCreatePatriciaTrie() {

    }

    /**
     * Test method for {@link Storage#createFullTextIndex(FullTextSearchHelper)}.
     */
    @Test
    public final void testCreateFullTextIndexFullTextSearchHelper() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#createFullTextIndex()}.
     */
    @Test
    public final void testCreateFullTextIndex() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#close()}.
     */
    @Test
    public final void testCloseStorageError() {
        try {
            myStorage.close();
        } catch (final StorageError aDetails) {
            LOGGER.debug(MessageCodes.SBT_006);
        }
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#close()}.
     */
    @Test
    public final void testClose() throws StorageError {
        myStorage.open(new NullFile());
        myStorage.close();
    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#setGcThreshold(long)}.
     */
    @Test
    public final void testSetGcThreshold() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#gc()}.
     */
    @Test
    public final void testGc() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#exportXML(java.io.Writer)}.
     */
    @Test
    public final void testExportXML() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#importXML(java.io.Reader)}.
     */
    @Test
    public final void testImportXML() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#getObjectByOID(int)}.
     */
    @Test
    public final void testGetObjectByOID() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#makePersistent(java.lang.Object)}.
     */
    @Test
    public final void testMakePersistent() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#setProperty(java.lang.String, java.lang.Object)}.
     */
    @Test
    public final void testSetProperty() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#setProperties(java.util.Properties)}.
     */
    @Test
    public final void testSetProperties() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#getProperty(java.lang.String)}.
     */
    @Test
    public final void testGetProperty() {

    }

    /**
     * Test method for {@link info.freelibrary.sodbox.Storage#getProperties()}.
     */
    @Test
    public final void testGetProperties() {

    }

    /**
     * Test method for {@link Storage#merge(java.util.Iterator[])}.
     */
    @Test
    public final void testMerge() {

    }

    /**
     * Test method for {@link Storage#join(java.util.Iterator[])}.
     */
    @Test
    public final void testJoin() {

    }

    /**
     * Test method for {@link Storage#setListener(StorageListener)}.
     */
    @Test
    public final void testSetListener() {

    }

    /**
     * Test method for {@link Storage#getListener()}.
     */
    @Test
    public final void testGetListener() {

    }

    /**
     * Test method for {@link Storage#getMemoryDump()}.
     */
    @Test
    public final void testGetMemoryDump() {

    }

    /**
     * Test method for {@link Storage#getUsedSize()}.
     */
    @Test
    public final void testGetUsedSize() {

    }

    /**
     * Test method for {@link Storage#getDatabaseSize()}.
     */
    @Test
    public final void testGetDatabaseSize() {

    }

    /**
     * Test method for {@link Storage#setClassLoader(ClassLoader)}.
     */
    @Test
    public final void testSetClassLoader() {

    }

    /**
     * Test method for {@link Storage#getClassLoader()}.
     */
    @Test
    public final void testGetClassLoader() {

    }

    /**
     * Test method for {@link Storage#registerClassLoader(INamedClassLoader)}.
     */
    @Test
    public final void testRegisterClassLoader() {

    }

    /**
     * Test method for {@link Storage#findClassLoader(String)}.
     */
    @Test
    public final void testFindClassLoader() {

    }

    /**
     * Test method for {@link Storage#registerCustomAllocator(Class, CustomAllocator)}.
     */
    @Test
    public final void testRegisterCustomAllocator() {

    }

    /**
     * Test method for {@link Storage#createBitmapAllocator(int, long, long, long)}.
     */
    @Test
    public final void testCreateBitmapAllocator() {

    }

    /**
     * Test method for {@link Storage#getTransactionContext()}.
     */
    @Test
    public final void testGetTransactionContext() {

    }

    /**
     * Test method for {@link Storage#setTransactionContext(ThreadTransactionContext)}.
     */
    @Test
    public final void testSetTransactionContext() {

    }

    /**
     * Test method for {@link Storage#setCustomSerializer(CustomSerializer)}.
     */
    @Test
    public final void testSetCustomSerializer() {

    }

    /**
     * Test method for {@link Storage#clearObjectCache()}.
     */
    @Test
    public final void testClearObjectCache() {

    }

    /**
     * Test method for {@link Storage#getDatabaseFormatVersion()}.
     */
    @Test
    public final void testGetDatabaseFormatVersion() {

    }

    /**
     * Test method for {@link Storage#store(java.lang.Object)}.
     */
    @Test
    public final void testStore() {

    }

    /**
     * Test method for {@link Storage#modify(java.lang.Object)}.
     */
    @Test
    public final void testModify() {

    }

    /**
     * Test method for {@link Storage#load(Object)}.
     */
    @Test
    public final void testLoad() {

    }

    /**
     * Test method for {@link Storage#deallocate(Object)}.
     */
    @Test
    public final void testDeallocate() {

    }

    /**
     * Test method for {@link Storage#getOid(Object)}.
     */
    @Test
    public final void testGetOid() {

    }

    /**
     * Test method for {@link Storage#setRecursiveLoading(Class, boolean)}.
     */
    @Test
    public final void testSetRecursiveLoading() {

    }

    /**
     * Test method for {@link Storage#deallocateObject(Object)}.
     */
    @Test
    public final void testDeallocateObject() {

    }

    /**
     * Test method for {@link Storage#storeObject(Object)}.
     */
    @Test
    public final void testStoreObject() {

    }

    /**
     * Test method for {@link Storage#storeFinalizedObject(Object)}.
     */
    @Test
    public final void testStoreFinalizedObject() {

    }

    /**
     * Test method for {@link Storage#modifyObject(Object)}.
     */
    @Test
    public final void testModifyObject() {

    }

    /**
     * Test method for {@link Storage#loadObject(Object)}.
     */
    @Test
    public final void testLoadObject() {

    }

    /**
     * Test method for {@link Storage#lockObject(Object)}.
     */
    @Test
    public final void testLockObject() {

    }

    /**
     * Test method for {@link Storage#decache(Object)}.
     */
    @Test
    public final void testDecacheObject() {

    }

}

class Record extends PersistentResource {

    String myStringKey;

    long myIntKey;

}

class Indices extends Persistent {

    Index myStringIndex;

    Index myIntIndex;

}
