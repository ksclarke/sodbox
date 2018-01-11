
package info.freelibrary.sodbox.impl;

public class StrongHashTable implements OidHashTable {

    static class Entry {

        Entry next;

        Object obj;

        int oid;

        Entry(final int oid, final Object obj, final Entry chain) {
            next = chain;
            this.oid = oid;
            this.obj = obj;
        }
    }

    static final float loadFactor = 0.75f;

    static final int MODIFIED_BUFFER_SIZE = 1024;

    Entry table[];

    int count;

    int threshold;

    boolean disableRehash;

    StorageImpl db;

    Object[] modified;

    long nModified;

    public StrongHashTable(final StorageImpl db, final int initialCapacity) {
        this.db = db;
        threshold = (int) (initialCapacity * loadFactor);
        if (initialCapacity != 0) {
            table = new Entry[initialCapacity];
        }
        modified = new Object[MODIFIED_BUFFER_SIZE];
    }

    @Override
    public synchronized void clear() {
        final Entry tab[] = table;
        for (int i = 0; i < tab.length; i++) {
            tab[i] = null;
        }
        count = 0;
        nModified = 0;
    }

    @Override
    public void clearDirty(final Object obj) {
    }

    @Override
    public synchronized void flush() {
        long n;
        do {
            n = nModified;
            if (nModified < MODIFIED_BUFFER_SIZE) {
                final Object[] mod = modified;
                for (int i = (int) nModified; --i >= 0;) {
                    final Object obj = mod[i];
                    if (db.isModified(obj)) {
                        db.store(obj);
                    }
                }
            } else {
                final Entry tab[] = table;
                disableRehash = true;
                for (final Entry element : tab) {
                    for (Entry e = element; e != null; e = e.next) {
                        if (db.isModified(e.obj)) {
                            db.store(e.obj);
                        }
                    }
                }
                disableRehash = false;
                if (count >= threshold) {
                    // Rehash the table if the threshold is exceeded
                    rehash();
                }
            }
        } while (n != nModified);
        nModified = 0;
    }

    @Override
    public synchronized Object get(final int oid) {
        final Entry tab[] = table;
        final int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                return e.obj;
            }
        }
        return null;
    }

    @Override
    public synchronized void invalidate() {
        for (final Entry element : table) {
            for (Entry e = element; e != null; e = e.next) {
                if (db.isModified(e.obj)) {
                    db.invalidate(e.obj);
                }
            }
        }
        nModified = 0;
    }

    public void preprocess() {
    }

    @Override
    public synchronized void put(final int oid, final Object obj) {
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                e.obj = obj;
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
        tab[index] = new Entry(oid, obj, tab[index]);
        count++;
    }

    void rehash() {
        final int oldCapacity = table.length;
        final Entry oldMap[] = table;
        int i;

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
                db.invalidate(e.obj);
                try {
                    db.load(e.obj);
                } catch (final Exception x) {
                    // ignore errors caused by attempt to load object which was created in rollbacked transaction
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
                e.obj = null;
                count -= 1;
                if (prev != null) {
                    prev.next = e.next;
                } else {
                    tab[index] = e.next;
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public synchronized void setDirty(final Object obj) {
        if (nModified < MODIFIED_BUFFER_SIZE) {
            modified[(int) nModified] = obj;
        }
        nModified += 1;
    }

    @Override
    public int size() {
        return count;
    }
}
