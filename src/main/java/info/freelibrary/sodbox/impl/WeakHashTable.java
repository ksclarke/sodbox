
package info.freelibrary.sodbox.impl;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

public class WeakHashTable implements OidHashTable {

    static final float LOAD_FACTOR = 0.75f;

    Entry myTable[];

    int myCount;

    int myThreshold;

    long myModifiedCount;

    boolean myRehashDisabled;

    StorageImpl myStorage;

    /**
     * Creates a weak hash table.
     *
     * @param aStorage A database storage
     * @param aInitialCapacity An initial capacity of the table
     */
    public WeakHashTable(final StorageImpl aStorage, final int aInitialCapacity) {
        myStorage = aStorage;
        myThreshold = (int) (aInitialCapacity * LOAD_FACTOR);
        myTable = new Entry[aInitialCapacity];
    }

    @Override
    public synchronized boolean remove(final int aOid) {
        final Entry tab[] = myTable;
        final int index = (aOid & 0x7FFFFFFF) % tab.length;

        for (Entry entry = tab[index], prev = null; entry != null; prev = entry, entry = entry.myNextEntry) {
            if (entry.myOid == aOid) {
                if (prev != null) {
                    prev.myNextEntry = entry.myNextEntry;
                } else {
                    tab[index] = entry.myNextEntry;
                }

                entry.clear();
                myCount -= 1;

                return true;
            }
        }
        return false;
    }

    protected Reference createReference(final Object aObj) {
        return new WeakReference(aObj);
    }

    @Override
    public synchronized void put(final int aOid, final Object aObj) {
        final Reference ref = createReference(aObj);

        Entry tab[] = myTable;
        int index = (aOid & 0x7FFFFFFF) % tab.length;

        for (Entry entry = tab[index]; entry != null; entry = entry.myNextEntry) {
            if (entry.myOid == aOid) {
                entry.myRef = ref;

                return;
            }
        }

        if (myCount >= myThreshold && !myRehashDisabled) {
            // Rehash the table if the threshold is exceeded
            rehash();
            tab = myTable;
            index = (aOid & 0x7FFFFFFF) % tab.length;
        }

        // Creates the new entry.
        tab[index] = new Entry(aOid, ref, tab[index]);
        myCount += 1;
    }

    @Override
    public Object get(final int aOid) {
        while (true) {
            cs:
            synchronized (this) {
                final Entry tab[] = myTable;
                final int index = (aOid & 0x7FFFFFFF) % tab.length;

                for (Entry entry = tab[index]; entry != null; entry = entry.myNextEntry) {
                    if (entry.myOid == aOid) {
                        final Object obj = entry.myRef.get();

                        if (obj == null) {
                            if (entry.myDirty != 0) {
                                break cs;
                            }
                        } else if (myStorage.isDeleted(obj)) {
                            entry.myRef.clear();
                            return null;
                        }

                        return obj;
                    }
                }

                return null;
            }

            System.runFinalization();
        }
    }

    @Override
    public void flush() {
        while (true) {
            cs:
            synchronized (this) {
                myRehashDisabled = true;
                long n;

                do {
                    n = myModifiedCount;

                    for (int index = 0; index < myTable.length; index++) {
                        for (Entry entry = myTable[index]; entry != null; entry = entry.myNextEntry) {
                            final Object obj = entry.myRef.get();

                            if (obj != null) {
                                if (myStorage.isModified(obj)) {
                                    myStorage.store(obj);
                                }
                            } else if (entry.myDirty != 0) {
                                break cs;
                            }
                        }
                    }
                } while (n != myModifiedCount);

                myRehashDisabled = false;

                if (myCount >= myThreshold) {
                    // Rehash the table if the threshold is exceeded
                    rehash();
                }
                return;
            }

            System.runFinalization();
        }
    }

    @Override
    public void invalidate() {
        while (true) {
            cs:
            synchronized (this) {
                for (int index = 0; index < myTable.length; index++) {
                    for (Entry entry = myTable[index]; entry != null; entry = entry.myNextEntry) {
                        final Object obj = entry.myRef.get();

                        if (obj != null) {
                            if (myStorage.isModified(obj)) {
                                entry.myDirty = 0;
                                myStorage.invalidate(obj);
                            }
                        } else if (entry.myDirty != 0) {
                            break cs;
                        }
                    }
                }

                return;
            }

            System.runFinalization();
        }
    }

    @Override
    public synchronized void reload() {
        myRehashDisabled = true;

        for (int index = 0; index < myTable.length; index++) {
            Entry entry;

            for (entry = myTable[index]; entry != null; entry = entry.myNextEntry) {
                final Object obj = entry.myRef.get();

                if (obj != null) {
                    myStorage.invalidate(obj);
                    try {
                        myStorage.load(obj);
                    } catch (final Exception x) {
                        // Ignore errors caused by attempt to load object which was created in rollbacked transaction
                    }
                }
            }
        }

        myRehashDisabled = false;

        if (myCount >= myThreshold) {
            // Rehash the table if the threshold is exceeded
            rehash();
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
            Entry entry;
            Entry nextEntry;
            Entry previousEntry;

            for (previousEntry = null, entry = oldMap[index]; entry != null; entry = nextEntry) {
                nextEntry = entry.myNextEntry;

                final Object obj = entry.myRef.get();

                if ((obj == null || myStorage.isDeleted(obj)) && entry.myDirty == 0) {
                    myCount -= 1;
                    entry.clear();

                    if (previousEntry == null) {
                        oldMap[index] = nextEntry;
                    } else {
                        previousEntry.myNextEntry = nextEntry;
                    }
                } else {
                    previousEntry = entry;
                }
            }
        }

        if (myCount <= (myThreshold >>> 1)) {
            return;
        }

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
    public synchronized void setDirty(final Object aObj) {
        final int oid = myStorage.getOid(aObj);
        final Entry tab[] = myTable;
        final int index = (oid & 0x7FFFFFFF) % tab.length;

        myModifiedCount += 1;

        for (Entry entry = tab[index]; entry != null; entry = entry.myNextEntry) {
            if (entry.myOid == oid) {
                entry.myDirty += 1;

                return;
            }
        }
    }

    @Override
    public synchronized void clearDirty(final Object aObj) {
        final int oid = myStorage.getOid(aObj);
        final Entry tab[] = myTable;
        final int index = (oid & 0x7FFFFFFF) % tab.length;

        for (Entry entry = tab[index]; entry != null; entry = entry.myNextEntry) {
            if (entry.myOid == oid) {
                if (entry.myDirty > 0) {
                    entry.myDirty -= 1;
                }

                return;
            }
        }
    }

    @Override
    public int size() {
        return myCount;
    }

    static class Entry {

        Entry myNextEntry;

        Reference myRef;

        int myOid;

        int myDirty;

        Entry(final int aOid, final Reference aRef, final Entry aChain) {
            myNextEntry = aChain;
            myOid = aOid;
            myRef = aRef;
        }

        void clear() {
            myRef.clear();
            myRef = null;
            myDirty = 0;
            myNextEntry = null;
        }
    }
}
