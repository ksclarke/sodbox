
package info.freelibrary.sodbox.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import info.freelibrary.sodbox.IPersistentSet;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.Storage;

class ScalableSet<T> extends PersistentCollection<T> implements IPersistentSet<T> {

    static final int BTREE_THRESHOLD = 128;

    Link<T> link;

    IPersistentSet<T> set;

    ScalableSet() {
    }

    ScalableSet(final StorageImpl storage, final int initialSize) {
        super(storage);
        if (initialSize <= BTREE_THRESHOLD) {
            link = storage.<T>createLink(initialSize);
        } else {
            set = storage.<T>createSet();
        }
    }

    @Override
    public boolean add(final T obj) {
        if (link != null) {
            int i = binarySearch(obj);
            final int n = link.size();
            if (i < n && link.getRaw(i).equals(obj)) {
                return false;
            }
            if (n == BTREE_THRESHOLD) {
                set = getStorage().<T>createSet();
                for (i = 0; i < n; i++) {
                    ((IPersistentSet) set).add(link.getRaw(i));
                }
                link = null;
                modify();
                set.add(obj);
            } else {
                modify();
                link.insert(i, obj);
            }
            return true;
        } else {
            return set.add(obj);
        }
    }

    private int binarySearch(final T obj) {
        int l = 0, r = link.size();
        final Storage storage = getStorage();
        final int oid = storage.getOid(obj);
        while (l < r) {
            final int m = l + r >> 1;
            if (storage.getOid(link.getRaw(m)) > oid) {
                l = m + 1;
            } else {
                r = m;
            }
        }
        return r;
    }

    @Override
    public void clear() {
        if (link != null) {
            link.clear();
            modify();
        } else {
            set.clear();
        }
    }

    @Override
    public boolean contains(final Object o) {
        return link != null ? link.contains(o) : set.contains(o);
    }

    @Override
    public void deallocate() {
        if (set != null) {
            set.deallocate();
        }
        super.deallocate();
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Set)) {
            return false;
        }
        final Collection c = (Collection) o;
        if (c.size() != size()) {
            return false;
        }
        return containsAll(c);
    }

    @Override
    public int hashCode() {
        int h = 0;
        final Iterator<T> i = iterator();
        while (i.hasNext()) {
            h += getStorage().getOid(i.next());
        }
        return h;
    }

    @Override
    public boolean isEmpty() {
        return size() != 0;
    }

    @Override
    public Iterator<T> iterator() {
        return link != null ? link.iterator() : set.iterator();
    }

    @Override
    public IterableIterator<T> join(final Iterator<T> with) {
        return with == null ? (IterableIterator<T>) iterator() : new JoinSetIterator<T>(getStorage(), iterator(),
                with);
    }

    @Override
    public boolean remove(final Object o) {
        if (link != null) {
            if (link.remove(o)) {
                modify();
                return true;
            }
            return false;
        } else {
            return set.remove(o);
        }
    }

    @Override
    public int size() {
        return link != null ? link.size() : set.size();
    }

    @Override
    public Object[] toArray() {
        return link != null ? link.toArray() : set.toArray();
    }

    @Override
    public <E> E[] toArray(final E a[]) {
        return link != null ? link.<E>toArray(a) : set.<E>toArray(a);
    }
}
