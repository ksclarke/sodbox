
package info.freelibrary.sodbox.impl;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

public class WeakHashTable implements OidHashTable {

    static class Entry {

        Entry next;

        Reference ref;

        int oid;

        int dirty;

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
    }

    static final float loadFactor = 0.75f;

    Entry table[];

    int count;

    int threshold;

    long nModified;

    boolean disableRehash;

    StorageImpl db;

    public WeakHashTable(final StorageImpl db, final int initialCapacity) {
        this.db = db;
        threshold = (int) (initialCapacity * loadFactor);
        table = new Entry[initialCapacity];
    }

    @Override
    public synchronized void clear() {
        final Entry tab[] = table;
        for (int i = 0; i < tab.length; i++) {
            tab[i] = null;
        }
        count = 0;
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
                        for (Entry e = table[i]; e != null; e = e.next) {
                            final Object obj = e.ref.get();
                            if (obj != null) {
                                if (db.isModified(obj)) {
                                    db.store(obj);
                                }
                            } else if (e.dirty != 0) {
                                break cs;
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
            System.runFinalization();
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
                        } else if (db.isDeleted(obj)) {
                            e.ref.clear();
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
                                db.invalidate(obj);
                            }
                        } else if (e.dirty != 0) {
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
    public synchronized void put(final int oid, final Object obj) {
        final Reference ref = createReference(obj);
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                e.ref = ref;
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
        count += 1;
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
                        db.load(obj);
                    } catch (final Exception x) {
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
}
