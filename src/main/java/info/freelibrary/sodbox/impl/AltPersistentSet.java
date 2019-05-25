
package info.freelibrary.sodbox.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import info.freelibrary.sodbox.IPersistentSet;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;

class AltPersistentSet<T> extends AltBtree<T> implements IPersistentSet<T> {

    AltPersistentSet() {
        myType = ClassDescriptor.TP_OBJECT;
        myUnique = true;
    }

    AltPersistentSet(final boolean aUnique) {
        myType = ClassDescriptor.TP_OBJECT;
        myUnique = aUnique;
    }

    @Override
    public boolean isEmpty() {
        return myNumOfElems == 0;
    }

    @Override
    public boolean contains(final Object aObject) {
        final Key key = new Key(aObject);
        final Iterator i = iterator(key, key, ASCENT_ORDER);
        return i.hasNext();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E> E[] toArray(final E[] aArray) {
        return (E[]) super.toArray((T[]) aArray);
    }

    @Override
    public boolean add(final T aObject) {
        return put(new Key(aObject), aObject);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(final Object aObject) {
        final T obj = (T) aObject;
        return removeIfExists(new BtreeKey(checkKey(new Key(obj)), obj));
    }

    @Override
    public boolean equals(final Object aObject) {
        if (aObject == this) {
            return true;
        }

        if (!(aObject instanceof Set)) {
            return false;
        }

        final Collection c = (Collection) aObject;

        if (c.size() != size()) {
            return false;
        }

        return containsAll(c);
    }

    @Override
    public int hashCode() {
        final Iterator i = iterator();

        int hashCode = 0;

        while (i.hasNext()) {
            hashCode += getStorage().getOid(i.next());
        }

        return hashCode;
    }

    @Override
    public IterableIterator<T> join(final Iterator<T> aWith) {
        return aWith == null ? (IterableIterator<T>) iterator() : new JoinSetIterator<>(getStorage(), iterator(),
                aWith);
    }

}
