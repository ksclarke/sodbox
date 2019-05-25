
package info.freelibrary.sodbox.impl;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

public class PinWeakHashTable implements OidHashTable {

    static final float LOAD_FACTOR = 0.75f;

    Entry myTable[];

    int myCount;

    int myThreshold;

    long myModifiedCount;

    boolean hasDisabledRehash;

    StorageImpl myStorage;

    /**
     * A PinWeakHashTable.
     *
     * @param aStorage
     * @param aInitialCapacity
     */
    public PinWeakHashTable(final StorageImpl aStorage, final int aInitialCapacity) {
        myStorage = aStorage;
        myThreshold = (int) (aInitialCapacity * LOAD_FACTOR);
        myTable = new Entry[aInitialCapacity];
    }

    @Override
    public synchronized boolean remove(final int aOID) {
        final Entry tab[] = myTable;
        final int index = (aOID & 0x7FFFFFFF) % tab.length;

        for (Entry entry = tab[index], prev = null; entry != null; prev = entry, entry = entry.myNext) {
            if (entry.myOID == aOID) {
                if (prev != null) {
                    prev.myNext = entry.myNext;
                } else {
                    tab[index] = entry.myNext;
                }

                entry.clear();
                myCount -= 1;

                return true;
            }
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    protected Reference createReference(final Object aObject) {
        return new WeakReference(aObject);
    }

    @Override
    public synchronized void put(final int aOID, final Object aObject) {
        final Reference ref = createReference(aObject);

        Entry tab[] = myTable;
        int index = (aOID & 0x7FFFFFFF) % tab.length;

        for (Entry entry = tab[index]; entry != null; entry = entry.myNext) {
            if (entry.myOID == aOID) {
                entry.myRef = ref;
                return;
            }
        }

        if (myCount >= myThreshold && !hasDisabledRehash) {
            // Rehash the table if the threshold is exceeded
            rehash();
            tab = myTable;
            index = (aOID & 0x7FFFFFFF) % tab.length;
        }

        // Creates the new entry.
        tab[index] = new Entry(aOID, ref, tab[index]);
        myCount += 1;
    }

    @Override
    public synchronized Object get(final int aOID) {
        final Entry tab[] = myTable;
        final int index = (aOID & 0x7FFFFFFF) % tab.length;

        for (Entry entry = tab[index]; entry != null; entry = entry.myNext) {
            if (entry.myOID == aOID) {
                if (entry.myPin != null) {
                    return entry.myPin;
                }

                return entry.myRef.get();
            }
        }

        return null;
    }

    @Override
    public synchronized void reload() {
        hasDisabledRehash = true;

        for (int index = 0; index < myTable.length; index++) {
            Entry entry;

            for (entry = myTable[index]; entry != null; entry = entry.myNext) {
                final Object obj = entry.myPin;

                if (obj != null) {
                    myStorage.invalidate(obj);

                    try {
                        myStorage.load(obj);
                    } catch (final Exception details) {
                        // ignore errors caused by attempt to load object which was created in rollback'ed transaction
                    }
                }
            }
        }

        hasDisabledRehash = false;

        if (myCount >= myThreshold) {
            // Rehash the table if the threshold is exceeded
            rehash();
        }
    }

    @Override
    public synchronized void flush() {
        long modCount;

        hasDisabledRehash = true;

        do {
            modCount = myModifiedCount;

            for (int index = 0; index < myTable.length; index++) {
                for (Entry entry = myTable[index]; entry != null; entry = entry.myNext) {
                    final Object obj = entry.myPin;

                    if (obj != null) {
                        myStorage.store(obj);
                        entry.myPin = null;
                    }
                }
            }
        } while (modCount != myModifiedCount);

        hasDisabledRehash = false;

        if (myCount >= myThreshold) {
            // Rehash the table if the threshold is exceeded
            rehash();
        }

        return;
    }

    @Override
    public synchronized void invalidate() {
        for (int index = 0; index < myTable.length; index++) {
            for (Entry entry = myTable[index]; entry != null; entry = entry.myNext) {
                final Object obj = entry.myPin;

                if (obj != null) {
                    entry.myPin = null;
                    myStorage.invalidate(obj);
                }
            }
        }
    }

    @Override
    public synchronized void clear() {
        final Entry tab[] = myTable;

        for (int index = 0; index < tab.length; index++) {
            tab[index] = null;
        }

        myCount = 0;
    }

    void rehash() {
        final int oldCapacity = myTable.length;
        final Entry oldMap[] = myTable;

        int index;

        for (index = oldCapacity; --index >= 0;) {
            Entry previous;
            Entry entry;
            Entry next;

            for (previous = null, entry = oldMap[index]; entry != null; entry = next) {
                next = entry.myNext;

                final Object obj = entry.myRef.get();

                if ((obj == null || myStorage.isDeleted(obj)) && entry.myPin == null) {
                    myCount -= 1;
                    entry.clear();

                    if (previous == null) {
                        oldMap[index] = next;
                    } else {
                        previous.myNext = next;
                    }
                } else {
                    previous = entry;
                }
            }
        }

        if (myCount <= myThreshold >>> 1) {
            return;
        }

        final int newCapacity = oldCapacity * 2 + 1;
        final Entry newMap[] = new Entry[newCapacity];

        myThreshold = (int) (newCapacity * LOAD_FACTOR);
        myTable = newMap;

        for (index = oldCapacity; --index >= 0;) {
            for (Entry old = oldMap[index]; old != null;) {
                final Entry entry = old;

                old = old.myNext;

                final int mapIndex = (entry.myOID & 0x7FFFFFFF) % newCapacity;

                entry.myNext = newMap[mapIndex];
                newMap[mapIndex] = entry;
            }
        }
    }

    @Override
    public synchronized void setDirty(final Object aObject) {
        final int oid = myStorage.getOid(aObject);
        final Entry tab[] = myTable;
        final int index = (oid & 0x7FFFFFFF) % tab.length;

        myModifiedCount += 1;

        for (Entry entry = tab[index]; entry != null; entry = entry.myNext) {
            if (entry.myOID == oid) {
                entry.myPin = aObject;
                return;
            }
        }
    }

    @Override
    public synchronized void clearDirty(final Object aObject) {
        final int oid = myStorage.getOid(aObject);
        final Entry tab[] = myTable;
        final int index = (oid & 0x7FFFFFFF) % tab.length;

        for (Entry entry = tab[index]; entry != null; entry = entry.myNext) {
            if (entry.myOID == oid) {
                entry.myPin = null;
                return;
            }
        }
    }

    @Override
    public int size() {
        return myCount;
    }

    static class Entry {

        Entry myNext;

        Reference myRef;

        int myOID;

        Object myPin;

        Entry(final int aOID, final Reference aRef, final Entry aChain) {
            myNext = aChain;
            myOID = aOID;
            myRef = aRef;
        }

        void clear() {
            myRef.clear();
            myRef = null;
            myPin = null;
            myNext = null;
        }
    }

}
