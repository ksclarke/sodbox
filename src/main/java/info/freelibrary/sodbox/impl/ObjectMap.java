
package info.freelibrary.sodbox.impl;

import java.lang.ref.WeakReference;

import info.freelibrary.sodbox.Persistent;

class ObjectMap {

    final float myLoadFactor = 0.75f;

    Entry[] myTable;

    int myCount;

    int myThreshold;

    ObjectMap(final int aInitialCapacity) {
        myThreshold = (int) (aInitialCapacity * myLoadFactor);
        myTable = new Entry[aInitialCapacity];
    }

    synchronized boolean remove(final Object aObject) {
        final Entry[] tab = myTable;
        final int hashcode = (int) ((0xFFFFFFFFL & System.identityHashCode(aObject)) % tab.length);

        for (Entry entry = tab[hashcode], prev = null; entry != null; entry = entry.myNext) {
            final Object target = entry.myWeakRef.get();

            if (target == null) {
                if (prev != null) {
                    prev.myNext = entry.myNext;
                } else {
                    tab[hashcode] = entry.myNext;
                }

                entry.clear();
                myCount -= 1;
            } else if (target == aObject) {
                if (prev != null) {
                    prev.myNext = entry.myNext;
                } else {
                    tab[hashcode] = entry.myNext;
                }

                entry.clear();
                myCount -= 1;

                return true;
            } else {
                prev = entry;
            }
        }

        return false;
    }

    Entry put(final Object aObject) {
        Entry[] tab = myTable;
        int hashcode = (int) ((0xFFFFFFFFL & System.identityHashCode(aObject)) % tab.length);

        for (Entry entry = tab[hashcode], prev = null; entry != null; entry = entry.myNext) {
            final Object target = entry.myWeakRef.get();

            if (target == null) {
                if (prev != null) {
                    prev.myNext = entry.myNext;
                } else {
                    tab[hashcode] = entry.myNext;
                }

                entry.clear();
                myCount -= 1;
            } else if (target == aObject) {
                return entry;
            } else {
                prev = entry;
            }
        }

        if (myCount >= myThreshold) {
            // Rehash the table if the threshold is exceeded
            rehash();
            tab = myTable;
            hashcode = (int) ((0xFFFFFFFFL & System.identityHashCode(aObject)) % tab.length);
        }

        // Creates the new entry.
        myCount++;

        return tab[hashcode] = new Entry(aObject, tab[hashcode]);
    }

    synchronized void setOid(final Object aObject, final int aOID) {
        put(aObject).myOID = aOID;
    }

    synchronized void setState(final Object aObject, final int aState) {
        final Entry entry = put(aObject);

        entry.myState = aState;

        if ((aState & Persistent.DIRTY) != 0) {
            entry.myPin = aObject;
        } else {
            entry.myPin = null;
        }
    }

    Entry get(final Object aObject) {
        if (aObject != null) {
            final Entry[] tab = myTable;
            final int hashcode = (int) ((0xFFFFFFFFL & System.identityHashCode(aObject)) % tab.length);

            for (Entry entry = tab[hashcode]; entry != null; entry = entry.myNext) {
                final Object target = entry.myWeakRef.get();

                if (target == aObject) {
                    return entry;
                }
            }
        }

        return null;
    }

    synchronized int getOid(final Object aObject) {
        final Entry entry = get(aObject);

        return entry != null ? entry.myOID : 0;
    }

    synchronized int getState(final Object aObject) {
        final Entry entry = get(aObject);

        return entry != null ? entry.myState : Persistent.DELETED;
    }

    void rehash() {
        final int oldCapacity = myTable.length;
        final Entry[] oldMap = myTable;

        int index;

        for (index = oldCapacity; --index >= 0;) {
            Entry entry;
            Entry next;
            Entry previous;

            for (previous = null, entry = oldMap[index]; entry != null; entry = next) {
                next = entry.myNext;

                if (entry.myWeakRef.get() == null) {
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

        if (myCount <= myThreshold >> 1) {
            return;
        }

        final int newCapacity = oldCapacity * 2 + 1;
        final Entry[] newMap = new Entry[newCapacity];

        myThreshold = (int) (newCapacity * myLoadFactor);
        myTable = newMap;

        for (index = oldCapacity; --index >= 0;) {
            for (Entry old = oldMap[index]; old != null;) {
                final Entry entry = old;
                final Object target;

                old = old.myNext;
                target = entry.myWeakRef.get();

                if (target != null) {
                    final int hashcode = (int) ((0xFFFFFFFFL & System.identityHashCode(target)) % newMap.length);

                    entry.myNext = newMap[hashcode];
                    newMap[hashcode] = entry;
                } else {
                    entry.clear();
                    myCount -= 1;
                }
            }
        }
    }

    static class Entry {

        Entry myNext;

        WeakReference myWeakRef;

        Object myPin;

        int myOID;

        int myState;

        @SuppressWarnings("unchecked")
        Entry(final Object aObject, final Entry aChain) {
            myWeakRef = new WeakReference(aObject);
            myNext = aChain;
        }

        void clear() {
            myWeakRef.clear();
            myWeakRef = null;
            myState = 0;
            myNext = null;
            myPin = null;
        }
    }

}
