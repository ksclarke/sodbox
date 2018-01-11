
package info.freelibrary.sodbox.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import info.freelibrary.sodbox.IPersistentSet;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.Storage;

class JoinSetIterator<T> extends IterableIterator<T> implements PersistentIterator {

    private final PersistentIterator i1;

    private final PersistentIterator i2;

    private int currOid;

    private final Storage storage;

    JoinSetIterator(final Storage storage, final Iterator<T> left, final Iterator<T> right) {
        this.storage = storage;
        i1 = (PersistentIterator) left;
        i2 = (PersistentIterator) right;
    }

    @Override
    public boolean hasNext() {
        if (currOid == 0) {
            int oid1, oid2 = 0;
            while ((oid1 = i1.nextOid()) != 0) {
                while (oid1 > oid2) {
                    if ((oid2 = i2.nextOid()) == 0) {
                        return false;
                    }
                }
                if (oid1 == oid2) {
                    currOid = oid1;
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return (T) storage.getObjectByOID(currOid);
    }

    @Override
    public int nextOid() {
        return hasNext() ? currOid : 0;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

class PersistentSet<T> extends Btree<T> implements IPersistentSet<T> {

    PersistentSet() {
    }

    PersistentSet(final boolean unique) {
        type = ClassDescriptor.tpObject;
        this.unique = unique;
    }

    @Override
    public boolean add(final T obj) {
        return put(new Key(obj), obj);
    }

    @Override
    public boolean contains(final Object o) {
        final Key key = new Key(o);
        final Iterator i = iterator(key, key, ASCENT_ORDER);
        return i.hasNext();
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
        final Iterator i = iterator();
        while (i.hasNext()) {
            h += getStorage().getOid(i.next());
        }
        return h;
    }

    @Override
    public boolean isEmpty() {
        return nElems == 0;
    }

    @Override
    public IterableIterator<T> join(final Iterator<T> with) {
        return with == null ? (IterableIterator<T>) iterator() : new JoinSetIterator<T>(getStorage(), iterator(),
                with);
    }

    @Override
    public boolean remove(final Object o) {
        final T obj = (T) o;
        return removeIfExists(new BtreeKey(checkKey(new Key(obj)), getStorage().getOid(obj)));
    }

    @Override
    public <E> E[] toArray(final E[] arr) {
        return (E[]) super.toArray((T[]) arr);
    }
}
