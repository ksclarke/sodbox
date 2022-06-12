
package info.freelibrary.sodbox.impl;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

public class LruObjectCache implements OidHashTable {

    static final float LOAD_FACTOR = 0.75f;

    static final int DEFAULT_INIT_SIZE = 1319;

    static Runtime runtime = Runtime.getRuntime();

    Entry myTable[];

    int myCount;

    int myThreshold;

    int myPinLimit;

    int myPinnedCount;

    long myModifiedCount;

    Entry myPinList;

    boolean hasDisabledRehash;

    StorageImpl myStorage;

    /**
     * Creates LRU object cache.
     *
     * @param aStorage A storage
     * @param aSize A cache size
     */
    public LruObjectCache(final StorageImpl aStorage, final int aSize) {
        final int initialCapacity = aSize == 0 ? DEFAULT_INIT_SIZE : aSize;

        myStorage = aStorage;
        myThreshold = (int) (initialCapacity * LOAD_FACTOR);
        myTable = new Entry[initialCapacity];
        myPinList = new Entry(0, null, null);
        myPinLimit = aSize;
        myPinList.myLRU = myPinList.myMRU = myPinList;
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
                unpinObject(entry);
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

    private void unpinObject(final Entry aEntry) {
        if (aEntry.myPin != null) {
            aEntry.unpin();
            myPinnedCount -= 1;
        }
    }

    private void pinObject(final Entry aEntry, final Object aObject) {
        if (myPinLimit != 0) {
            if (aEntry.myPin != null) {
                aEntry.unlink();
            } else {
                if (myPinnedCount == myPinLimit) {
                    myPinList.myLRU.unpin();
                } else {
                    myPinnedCount += 1;
                }
            }

            aEntry.linkAfter(myPinList, aObject);
        }
    }

    @Override
    public synchronized void put(final int aOID, final Object aObject) {
        final Reference ref = createReference(aObject);

        Entry tab[] = myTable;
        int index = (aOID & 0x7FFFFFFF) % tab.length;

        for (Entry e = tab[index]; e != null; e = e.myNext) {
            if (e.myOID == aOID) {
                e.myRef = ref;
                pinObject(e, aObject);

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
        pinObject(tab[index], aObject);
        myCount++;
    }

    @Override
    public Object get(final int aOID) {
        while (true) {
            cs:
            synchronized (this) {
                final Entry tab[] = myTable;
                final int index = (aOID & 0x7FFFFFFF) % tab.length;

                for (Entry entry = tab[index]; entry != null; entry = entry.myNext) {
                    if (entry.myOID == aOID) {
                        final Object obj = entry.myRef.get();

                        if (obj == null) {
                            if (entry.myDirty != 0) {
                                break cs;
                            }
                        } else {
                            if (myStorage.isDeleted(obj)) {
                                entry.myRef.clear();
                                unpinObject(entry);

                                return null;
                            }

                            pinObject(entry, obj);
                        }

                        return obj;
                    }
                }

                return null;
            }

            runtime.runFinalization();
        }
    }

    @Override
    public void flush() {
        while (true) {
            cs:
            synchronized (this) {
                long modifiedCount;

                hasDisabledRehash = true;

                do {
                    modifiedCount = myModifiedCount;

                    for (int index = 0; index < myTable.length; index++) {
                        Entry entry;
                        Entry next;
                        Entry previous;

                        for (entry = myTable[index], previous = null; entry != null; entry = next) {
                            final Object obj = entry.myRef.get();

                            next = entry.myNext;

                            if (obj != null) {
                                if (myStorage.isModified(obj)) {
                                    myStorage.store(obj);
                                }

                                previous = entry;
                            } else if (entry.myDirty != 0) {
                                break cs;
                            } else {
                                myCount -= 1;
                                entry.clear();

                                if (previous == null) {
                                    myTable[index] = next;
                                } else {
                                    previous.myNext = next;
                                }
                            }
                        }
                    }
                } while (modifiedCount != myModifiedCount);

                hasDisabledRehash = false;

                if (myCount >= myThreshold) {
                    // Rehash the table if the threshold is exceeded
                    rehash();
                }

                return;
            }

            runtime.runFinalization();
        }
    }

    @Override
    public synchronized void reload() {
        hasDisabledRehash = true;

        for (int index = 0; index < myTable.length; index++) {
            Entry entry;

            for (entry = myTable[index]; entry != null; entry = entry.myNext) {
                final Object obj = entry.myRef.get();

                if (obj != null) {
                    myStorage.invalidate(obj);

                    try {
                        // System.out.println("Reload object " + db.getOid(obj));
                        myStorage.load(obj);
                    } catch (final Exception details) {
                        // FIXME
                        // ignore errors caused by attempt to load object which was created in rollbacked transaction
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
    public void invalidate() {
        while (true) {
            cs:
            synchronized (this) {
                for (int index = 0; index < myTable.length; index++) {
                    for (Entry entry = myTable[index]; entry != null; entry = entry.myNext) {
                        final Object obj = entry.myRef.get();

                        if (obj != null) {
                            if (myStorage.isModified(obj)) {
                                entry.myDirty = 0;
                                unpinObject(entry);
                                myStorage.invalidate(obj);
                            }
                        } else if (entry.myDirty != 0) {
                            break cs;
                        }
                    }
                }

                return;
            }

            runtime.runFinalization();
        }
    }

    @Override
    public synchronized void clear() {
        final Entry tab[] = myTable;

        for (int index = 0; index < tab.length; index++) {
            tab[index] = null;
        }

        myCount = 0;
        myPinnedCount = 0;
        myPinList.myLRU = myPinList.myMRU = myPinList;
    }

    void rehash() {
        final int oldCapacity = myTable.length;
        final Entry oldMap[] = myTable;

        int index;

        for (index = oldCapacity; --index >= 0;) {
            Entry entry;
            Entry next;
            Entry previous;

            for (previous = null, entry = oldMap[index]; entry != null; entry = next) {
                final Object obj;

                next = entry.myNext;
                obj = entry.myRef.get();

                if ((obj == null || myStorage.isDeleted(obj)) && entry.myDirty == 0) {
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
                final int mapIndex;

                old = old.myNext;
                mapIndex = (entry.myOID & 0x7FFFFFFF) % newCapacity;

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
                entry.myDirty += 1;
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

        Entry myNext;

        Reference myRef;

        int myOID;

        int myDirty;

        Entry myLRU;

        Entry myMRU;

        Object myPin;

        Entry(final int aOID, final Reference aRef, final Entry aChain) {
            myNext = aChain;
            myOID = aOID;
            myRef = aRef;
        }

        void unlink() {
            myLRU.myMRU = myMRU;
            myMRU.myLRU = myLRU;
        }

        void unpin() {
            unlink();

            myLRU = myMRU = null;
            myPin = null;
        }

        void linkAfter(final Entry aHead, final Object aObject) {
            myMRU = aHead.myMRU;
            myMRU.myLRU = this;
            aHead.myMRU = this;
            myLRU = aHead;
            myPin = aObject;
        }

        void clear() {
            myRef.clear();
            myRef = null;
            myDirty = 0;
            myNext = null;
        }
    }
}
