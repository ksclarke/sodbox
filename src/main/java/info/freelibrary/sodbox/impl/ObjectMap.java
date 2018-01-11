
package info.freelibrary.sodbox.impl;

import java.lang.ref.WeakReference;

import info.freelibrary.sodbox.PinnedPersistent;

class ObjectMap {

    static class Entry {

        Entry next;

        WeakReference wref;

        Object pin;

        int oid;

        int state;

        Entry(final Object obj, final Entry chain) {
            wref = new WeakReference(obj);
            next = chain;
        }

        void clear() {
            wref.clear();
            wref = null;
            state = 0;
            next = null;
            pin = null;
        }
    }

    Entry[] table;

    final float loadFactor = 0.75f;

    int count;

    int threshold;

    ObjectMap(final int initialCapacity) {
        threshold = (int) (initialCapacity * loadFactor);
        table = new Entry[initialCapacity];
    }

    Entry get(final Object obj) {
        if (obj != null) {
            final Entry[] tab = table;
            final int hashcode = (int) ((0xFFFFFFFFL & System.identityHashCode(obj)) % tab.length);
            for (Entry e = tab[hashcode]; e != null; e = e.next) {
                final Object target = e.wref.get();
                if (target == obj) {
                    return e;
                }
            }
        }
        return null;
    }

    synchronized int getOid(final Object obj) {
        final Entry e = get(obj);
        return e != null ? e.oid : 0;
    }

    synchronized int getState(final Object obj) {
        final Entry e = get(obj);
        return e != null ? e.state : PinnedPersistent.DELETED;
    }

    Entry put(final Object obj) {
        Entry[] tab = table;
        int hashcode = (int) ((0xFFFFFFFFL & System.identityHashCode(obj)) % tab.length);
        for (Entry e = tab[hashcode], prev = null; e != null; e = e.next) {
            final Object target = e.wref.get();
            if (target == null) {
                if (prev != null) {
                    prev.next = e.next;
                } else {
                    tab[hashcode] = e.next;
                }
                e.clear();
                count -= 1;
            } else if (target == obj) {
                return e;
            } else {
                prev = e;
            }
        }
        if (count >= threshold) {
            // Rehash the table if the threshold is exceeded
            rehash();
            tab = table;
            hashcode = (int) ((0xFFFFFFFFL & System.identityHashCode(obj)) % tab.length);
        }

        // Creates the new entry.
        count++;
        return tab[hashcode] = new Entry(obj, tab[hashcode]);
    }

    void rehash() {
        final int oldCapacity = table.length;
        final Entry[] oldMap = table;
        int i;
        for (i = oldCapacity; --i >= 0;) {
            Entry e, next, prev;
            for (prev = null, e = oldMap[i]; e != null; e = next) {
                next = e.next;
                if (e.wref.get() == null) {
                    count -= 1;
                    e.clear();
                    if (prev == null) {
                        oldMap[i] = next;
                    } else {
                        prev.next = next;
                    }
                } else {
                    prev = e;
                }
            }
        }

        if (count <= threshold >> 1) {
            return;
        }
        final int newCapacity = oldCapacity * 2 + 1;
        final Entry[] newMap = new Entry[newCapacity];

        threshold = (int) (newCapacity * loadFactor);
        table = newMap;

        for (i = oldCapacity; --i >= 0;) {
            for (Entry old = oldMap[i]; old != null;) {
                final Entry e = old;
                old = old.next;
                final Object target = e.wref.get();
                if (target != null) {
                    final int hashcode = (int) ((0xFFFFFFFFL & System.identityHashCode(target)) % newMap.length);
                    e.next = newMap[hashcode];
                    newMap[hashcode] = e;
                } else {
                    e.clear();
                    count -= 1;
                }
            }
        }
    }

    synchronized boolean remove(final Object obj) {
        final Entry[] tab = table;
        final int hashcode = (int) ((0xFFFFFFFFL & System.identityHashCode(obj)) % tab.length);
        for (Entry e = tab[hashcode], prev = null; e != null; e = e.next) {
            final Object target = e.wref.get();
            if (target == null) {
                if (prev != null) {
                    prev.next = e.next;
                } else {
                    tab[hashcode] = e.next;
                }
                e.clear();
                count -= 1;
            } else if (target == obj) {
                if (prev != null) {
                    prev.next = e.next;
                } else {
                    tab[hashcode] = e.next;
                }
                e.clear();
                count -= 1;
                return true;
            } else {
                prev = e;
            }
        }
        return false;
    }

    synchronized void setOid(final Object obj, final int oid) {
        final Entry e = put(obj);
        e.oid = oid;
    }

    synchronized void setState(final Object obj, final int state) {
        final Entry e = put(obj);
        e.state = state;
        if ((state & PinnedPersistent.DIRTY) != 0) {
            e.pin = obj;
        } else {
            e.pin = null;
        }
    }
}
