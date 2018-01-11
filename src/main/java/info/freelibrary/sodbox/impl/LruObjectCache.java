
package info.freelibrary.sodbox.impl;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

public class LruObjectCache implements OidHashTable {

    static class Entry {

        Entry next;

        Reference ref;

        int oid;

        int dirty;

        Entry lru;

        Entry mru;

        Object pin;

        Entry(final int oid, final Reference ref, final Entry chain) {
            next = chain;
            this.oid = oid;
            this.ref = ref;
        }

        void clear() {
            ref.clear();
            ref = null;
            dirty = 0;
            next = null;
        }

        void linkAfter(final Entry head, final Object obj) {
            mru = head.mru;
            mru.lru = this;
            head.mru = this;
            lru = head;
            pin = obj;
        }

        void unlink() {
            lru.mru = mru;
            mru.lru = lru;
        }

        void unpin() {
            unlink();
            lru = mru = null;
            pin = null;
        }
    }

    static final float loadFactor = 0.75f;

    static final int defaultInitSize = 1319;

    static Runtime runtime = Runtime.getRuntime();

    Entry table[];

    int count;

    int threshold;

    int pinLimit;

    int nPinned;

    long nModified;

    Entry pinList;

    boolean disableRehash;

    StorageImpl db;

    public LruObjectCache(final StorageImpl db, final int size) {
        this.db = db;
        final int initialCapacity = size == 0 ? defaultInitSize : size;
        threshold = (int) (initialCapacity * loadFactor);
        table = new Entry[initialCapacity];
        pinList = new Entry(0, null, null);
        pinLimit = size;
        pinList.lru = pinList.mru = pinList;
    }

    @Override
    public synchronized void clear() {
        final Entry tab[] = table;
        for (int i = 0; i < tab.length; i++) {
            tab[i] = null;
        }
        count = 0;
        nPinned = 0;
        pinList.lru = pinList.mru = pinList;
    }

    @Override
    public synchronized void clearDirty(final Object obj) {
        final int oid = db.getOid(obj);
        final Entry tab[] = table;
        final int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                if (e.dirty > 0) {
                    e.dirty -= 1;
                }
                return;
            }
        }
    }

    protected Reference createReference(final Object obj) {
        return new WeakReference(obj);
    }

    @Override
    public void flush() {
        while (true) {
            cs:
            synchronized (this) {
                disableRehash = true;
                long n;
                do {
                    n = nModified;
                    for (int i = 0; i < table.length; i++) {
                        Entry e, next, prev;
                        for (e = table[i], prev = null; e != null; e = next) {
                            final Object obj = e.ref.get();
                            next = e.next;
                            if (obj != null) {
                                if (db.isModified(obj)) {
                                    db.store(obj);
                                }
                                prev = e;
                            } else if (e.dirty != 0) {
                                break cs;
                            } else {
                                count -= 1;
                                e.clear();
                                if (prev == null) {
                                    table[i] = next;
                                } else {
                                    prev.next = next;
                                }
                            }
                        }
                    }
                } while (n != nModified);
                disableRehash = false;
                if (count >= threshold) {
                    // Rehash the table if the threshold is exceeded
                    rehash();
                }
                return;
            }
            runtime.runFinalization();
        }
    }

    @Override
    public Object get(final int oid) {
        while (true) {
            cs:
            synchronized (this) {
                final Entry tab[] = table;
                final int index = (oid & 0x7FFFFFFF) % tab.length;
                for (Entry e = tab[index]; e != null; e = e.next) {
                    if (e.oid == oid) {
                        final Object obj = e.ref.get();
                        if (obj == null) {
                            if (e.dirty != 0) {
                                break cs;
                            }
                        } else {
                            if (db.isDeleted(obj)) {
                                e.ref.clear();
                                unpinObject(e);
                                return null;
                            }
                            pinObject(e, obj);
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
    public void invalidate() {
        while (true) {
            cs:
            synchronized (this) {
                for (int i = 0; i < table.length; i++) {
                    for (Entry e = table[i]; e != null; e = e.next) {
                        final Object obj = e.ref.get();
                        if (obj != null) {
                            if (db.isModified(obj)) {
                                e.dirty = 0;
                                unpinObject(e);
                                db.invalidate(obj);
                            }
                        } else if (e.dirty != 0) {
                            break cs;
                        }
                    }
                }
                return;
            }
            runtime.runFinalization();
        }
    }

    private final void pinObject(final Entry e, final Object obj) {
        if (pinLimit != 0) {
            if (e.pin != null) {
                e.unlink();
            } else {
                if (nPinned == pinLimit) {
                    pinList.lru.unpin();
                } else {
                    nPinned += 1;
                }
            }
            e.linkAfter(pinList, obj);
        }
    }

    @Override
    public synchronized void put(final int oid, final Object obj) {
        final Reference ref = createReference(obj);
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                e.ref = ref;
                pinObject(e, obj);
                return;
            }
        }
        if (count >= threshold && !disableRehash) {
            // Rehash the table if the threshold is exceeded
            rehash();
            tab = table;
            index = (oid & 0x7FFFFFFF) % tab.length;
        }

        // Creates the new entry.
        tab[index] = new Entry(oid, ref, tab[index]);
        pinObject(tab[index], obj);
        count++;
    }

    void rehash() {
        final int oldCapacity = table.length;
        final Entry oldMap[] = table;
        int i;
        for (i = oldCapacity; --i >= 0;) {
            Entry e, next, prev;
            for (prev = null, e = oldMap[i]; e != null; e = next) {
                next = e.next;
                final Object obj = e.ref.get();
                if ((obj == null || db.isDeleted(obj)) && e.dirty == 0) {
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

        if (count <= threshold >>> 1) {
            return;
        }
        final int newCapacity = oldCapacity * 2 + 1;
        final Entry newMap[] = new Entry[newCapacity];

        threshold = (int) (newCapacity * loadFactor);
        table = newMap;

        for (i = oldCapacity; --i >= 0;) {
            for (Entry old = oldMap[i]; old != null;) {
                final Entry e = old;
                old = old.next;

                final int index = (e.oid & 0x7FFFFFFF) % newCapacity;
                e.next = newMap[index];
                newMap[index] = e;
            }
        }
    }

    @Override
    public synchronized void reload() {
        disableRehash = true;
        for (final Entry element : table) {
            Entry e;
            for (e = element; e != null; e = e.next) {
                final Object obj = e.ref.get();
                if (obj != null) {
                    db.invalidate(obj);
                    try {
                        // System.out.println("Reload object " + db.getOid(obj));
                        db.load(obj);
                    } catch (final Exception x) {
                        // x.printStackTrace();
                        // ignore errors caused by attempt to load object which was created in rollbacked transaction
                    }
                }
            }
        }
        disableRehash = false;
        if (count >= threshold) {
            // Rehash the table if the threshold is exceeded
            rehash();
        }
    }

    @Override
    public synchronized boolean remove(final int oid) {
        final Entry tab[] = table;
        final int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index], prev = null; e != null; prev = e, e = e.next) {
            if (e.oid == oid) {
                if (prev != null) {
                    prev.next = e.next;
                } else {
                    tab[index] = e.next;
                }
                e.clear();
                unpinObject(e);
                count -= 1;
                return true;
            }
        }
        return false;
    }

    @Override
    public synchronized void setDirty(final Object obj) {
        final int oid = db.getOid(obj);
        final Entry tab[] = table;
        final int index = (oid & 0x7FFFFFFF) % tab.length;
        nModified += 1;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                e.dirty += 1;
                return;
            }
        }
    }

    @Override
    public int size() {
        return count;
    }

    private final void unpinObject(final Entry e) {
        if (e.pin != null) {
            e.unpin();
            nPinned -= 1;
        }
    }
}
