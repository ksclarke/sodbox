
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

    private final PersistentIterator myFirstIterator;

    private final PersistentIterator mySecondIterator;

    private int myCurrentOID;

    private final Storage myStorage;

    JoinSetIterator(final Storage aStorage, final Iterator<T> aLeft, final Iterator<T> aRight) {
        myStorage = aStorage;
        myFirstIterator = (PersistentIterator) aLeft;
        mySecondIterator = (PersistentIterator) aRight;
    }

    @Override
    public boolean hasNext() {
        if (myCurrentOID == 0) {
            int secondIOD = 0;
            int firstOID;

            while ((firstOID = myFirstIterator.nextOID()) != 0) {
                while (firstOID > secondIOD) {
                    if ((secondIOD = mySecondIterator.nextOID()) == 0) {
                        return false;
                    }
                }
                if (firstOID == secondIOD) {
                    myCurrentOID = firstOID;
                    return true;
                }
            }

            return false;
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return (T) myStorage.getObjectByOID(myCurrentOID);
    }

    @Override
    public int nextOID() {
        return hasNext() ? myCurrentOID : 0;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

class PersistentSet<T> extends Btree<T> implements IPersistentSet<T> {

    PersistentSet(final boolean aUnique) {
        myType = ClassDescriptor.TP_OBJECT;
        isUniqueKeyIndex = aUnique;
    }

    PersistentSet() {
    }

    @Override
    public boolean isEmpty() {
        return myNumOfElems == 0;
    }

    @Override
    public boolean contains(final Object aObject) {
        final Key key = new Key(aObject);
        final Iterator iterator = iterator(key, key, ASCENT_ORDER);

        return iterator.hasNext();
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
        return removeIfExists(new BtreeKey(checkKey(new Key(obj)), getStorage().getOid(obj)));
    }

    @Override
    public boolean equals(final Object aObject) {
        if (aObject == this) {
            return true;
        }

        if (!(aObject instanceof Set)) {
            return false;
        }

        final Collection collection = (Collection) aObject;

        if (collection.size() != size()) {
            return false;
        }

        return containsAll(collection);
    }

    @Override
    public int hashCode() {
        int hashCode = 0;

        final Iterator iterator = iterator();

        while (iterator.hasNext()) {
            hashCode += getStorage().getOid(iterator.next());
        }

        return hashCode;
    }

    @Override
    public IterableIterator<T> join(final Iterator<T> aWith) {
        return aWith == null ? (IterableIterator<T>) iterator() : new JoinSetIterator<>(getStorage(), iterator(),
                aWith);
    }

}
