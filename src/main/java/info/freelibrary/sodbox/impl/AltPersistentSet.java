
package info.freelibrary.sodbox.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import info.freelibrary.sodbox.IPersistentSet;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;

class AltPersistentSet<T> extends AltBtree<T> implements IPersistentSet<T> {

    AltPersistentSet() {
        type = ClassDescriptor.tpObject;
        unique = true;
    }

    AltPersistentSet(final boolean unique) {
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
        return removeIfExists(new BtreeKey(checkKey(new Key(obj)), obj));
    }

    @Override
    public <E> E[] toArray(final E[] arr) {
        return (E[]) super.toArray((T[]) arr);
    }
}
