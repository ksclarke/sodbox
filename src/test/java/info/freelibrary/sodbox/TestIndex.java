
package info.freelibrary.sodbox;

import java.io.File;

import info.freelibrary.util.Logger;
import info.freelibrary.util.LoggerFactory;

/**
 * Creates a test index.
 */
public final class TestIndex {

    static final int RECORD_COUNT = 100000;

    static int PAGE_POOL_SIZE = 32 * 1024 * 1024;

    private static final Logger LOGGER = LoggerFactory.getLogger(TestIndex.class, Constants.MESSAGES);

    private static final String DB_FILE_PATH = System.getProperty("java.io.tmpdir") + File.separator + "test.dbs";

    private TestIndex() {
    }

    /**
     * Runs the main program.
     *
     * @param aArgsArray Arguments to the main program
     */
    @SuppressWarnings("uncommentedmain")
    public static void main(final String[] aArgsArray) {
        final Storage storage = StorageFactory.getInstance().createStorage();

        storage.open(DB_FILE_PATH, PAGE_POOL_SIZE);

        Indices root = (Indices) storage.getRoot();

        if (root == null) {
            root = new Indices();
            root.myStringIndex = storage.createIndex(String.class, true);
            root.myIntIndex = storage.createIndex(long.class, true);
            storage.setRoot(root);
        }

        final Index intIndex = root.myIntIndex;
        final Index stringIndex = root.myStringIndex;

        long start = System.currentTimeMillis();
        long key = 1999;
        int index;

        for (index = 0; index < RECORD_COUNT; index++) {
            final Record record = new Record();

            key = (3141592621L * key + 2718281829L) % 1000000007L;
            record.myIntKey = key;
            record.myStringKey = Long.toString(key);
            intIndex.put(new Key(record.myIntKey), record);
            stringIndex.put(new Key(record.myStringKey), record);
        }

        storage.commit();

        LOGGER.info(MessageCodes.SBT_001, RECORD_COUNT, System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        key = 1999;

        for (index = 0; index < RECORD_COUNT; index++) {
            key = (3141592621L * key + 2718281829L) % 1000000007L;

            final Record record1 = (Record) intIndex.get(new Key(key));
            final Record record2 = (Record) stringIndex.get(new Key(Long.toString(key)));

            Assert.that(record1 != null && record1 == record2);
        }

        LOGGER.info(MessageCodes.SBT_002, RECORD_COUNT * 2, System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        key = 1999;

        for (index = 0; index < RECORD_COUNT; index++) {
            key = (3141592621L * key + 2718281829L) % 1000000007L;

            final Record record = (Record) intIndex.get(new Key(key));
            final Record removed = (Record) intIndex.remove(new Key(key));

            Assert.that(removed == record);

            stringIndex.remove(new Key(Long.toString(key)));
            record.deallocate();
        }

        LOGGER.info(MessageCodes.SBT_003, RECORD_COUNT, System.currentTimeMillis() - start);

        storage.close();

        if (!new File(DB_FILE_PATH).delete()) {
            LOGGER.error(MessageCodes.SBT_004, DB_FILE_PATH);
        }
    }
}
