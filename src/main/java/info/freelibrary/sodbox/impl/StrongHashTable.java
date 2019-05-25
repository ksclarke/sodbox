
package info.freelibrary.sodbox.impl;

public class StrongHashTable implements OidHashTable {

    static final float LOAD_FACTOR = 0.75f;

    static final int MODIFIED_BUFFER_SIZE = 1024;

    Entry myTable[];

    int myCount;

    int myThreshold;

    boolean isRehashDisabled;

    StorageImpl myStorage;

    Object[] myModified;

    long myModifiedCount;

    /**
     * Creates a <code>StringHashTable</code>.
     *
     * @param aStorageImpl A storage implementation
     * @param aInitialCapacity An initial capacity
     */
    public StrongHashTable(final StorageImpl aStorageImpl, final int aInitialCapacity) {
        myStorage = aStorageImpl;
        myThreshold = (int) (aInitialCapacity * LOAD_FACTOR);

        if (aInitialCapacity != 0) {
            myTable = new Entry[aInitialCapacity];
        }

        myModified = new Object[MODIFIED_BUFFER_SIZE];
    }

    @Override
    public synchronized boolean remove(final int aOid) {
        final Entry tab[] = myTable;
        final int index = (aOid & 0x7FFFFFFF) % tab.length;

        for (Entry entry = tab[index], prev = null; entry != null; prev = entry, entry = entry.myNextEntry) {
            if (entry.myOid == aOid) {
                entry.myObj = null;
                myCount -= 1;

                if (prev != null) {
                    prev.myNextEntry = entry.myNextEntry;
                } else {
                    tab[index] = entry.myNextEntry;
                }

                return true;
            }
        }

        return false;
    }

    @Override
    public synchronized void put(final int aOid, final Object aObj) {
        Entry tab[] = myTable;
        int index = (aOid & 0x7FFFFFFF) % tab.length;

        for (Entry entry = tab[index]; entry != null; entry = entry.myNextEntry) {
            if (entry.myOid == aOid) {
                entry.myObj = aObj;
                return;
            }
        }

        if (myCount >= myThreshold && !isRehashDisabled) {
            // Rehash the table if the threshold is exceeded
            rehash();
            tab = myTable;
            index = (aOid & 0x7FFFFFFF) % tab.length;
        }

        // Creates the new entry.
        tab[index] = new Entry(aOid, aObj, tab[index]);
        myCount++;
    }

    @Override
    public synchronized Object get(final int aOid) {
        final Entry tab[] = myTable;
        final int index = (aOid & 0x7FFFFFFF) % tab.length;

        for (Entry entry = tab[index]; entry != null; entry = entry.myNextEntry) {
            if (entry.myOid == aOid) {
                return entry.myObj;
            }
        }

        return null;
    }

    void rehash() {
        final int oldCapacity = myTable.length;
        final Entry oldMap[] = myTable;

        int index;

        final int newCapacity = oldCapacity * 2 + 1;
        final Entry newMap[] = new Entry[newCapacity];

        myThreshold = (int) (newCapacity * LOAD_FACTOR);
        myTable = newMap;

        for (index = oldCapacity; --index >= 0;) {
            for (Entry old = oldMap[index]; old != null;) {
                final Entry entry = old;

                old = old.myNextEntry;

                final int newIndex = (entry.myOid & 0x7FFFFFFF) % newCapacity;

                entry.myNextEntry = newMap[newIndex];
                newMap[newIndex] = entry;
            }
        }
    }

    @Override
    public synchronized void flush() {
        long count;

        do {
            count = myModifiedCount;

            if (myModifiedCount < MODIFIED_BUFFER_SIZE) {
                final Object[] mod = myModified;

                for (int index = (int) myModifiedCount; --index >= 0;) {
                    final Object obj = mod[index];

                    if (myStorage.isModified(obj)) {
                        myStorage.store(obj);
                    }
                }
            } else {
                final Entry tab[] = myTable;

                isRehashDisabled = true;

                for (int index = 0; index < tab.length; index++) {
                    for (Entry entry = tab[index]; entry != null; entry = entry.myNextEntry) {
                        if (myStorage.isModified(entry.myObj)) {
                            myStorage.store(entry.myObj);
                        }
                    }
                }

                isRehashDisabled = false;

                if (myCount >= myThreshold) {
                    // Rehash the table if the threshold is exceeded
                    rehash();
                }
            }
        } while (count != myModifiedCount);

        myModifiedCount = 0;
    }

    @Override
    public synchronized void clear() {
        final Entry tab[] = myTable;

        for (int index = 0; index < tab.length; index++) {
            tab[index] = null;
        }

        myCount = 0;
        myModifiedCount = 0;
    }

    @Override
    public synchronized void invalidate() {
        for (int index = 0; index < myTable.length; index++) {
            for (Entry entry = myTable[index]; entry != null; entry = entry.myNextEntry) {
                if (myStorage.isModified(entry.myObj)) {
                    myStorage.invalidate(entry.myObj);
                }
            }
        }

        myModifiedCount = 0;
    }

    @Override
    public synchronized void reload() {
        isRehashDisabled = true;

        for (int index = 0; index < myTable.length; index++) {
            Entry entry;

            final Entry nextEntry;
            final Entry prevEntry;

            for (entry = myTable[index]; entry != null; entry = entry.myNextEntry) {
                myStorage.invalidate(entry.myObj);
                try {
                    myStorage.load(entry.myObj);
                } catch (final Exception x) {
                    // ignore errors caused by attempt to load object which was created in rolled back transaction
                }
            }
        }

        isRehashDisabled = false;

        if (myCount >= myThreshold) {
            // Rehash the table if the threshold is exceeded
            rehash();
        }
    }

    @Override
    public synchronized void setDirty(final Object aObj) {
        if (myModifiedCount < MODIFIED_BUFFER_SIZE) {
            myModified[(int) myModifiedCount] = aObj;
        }

        myModifiedCount += 1;
    }

    @Override
    public void clearDirty(final Object aObj) {
    }

    @Override
    public int size() {
        return myCount;
    }

    /**
     * Pre-process.
     */
    public void preprocess() {
    }

    static class Entry {

        Entry myNextEntry;

        Object myObj;

        int myOid;

        Entry(final int aOid, final Object aObj, final Entry aChain) {
            myNextEntry = aChain;
            myOid = aOid;
            myObj = aObj;
        }
    }
}
