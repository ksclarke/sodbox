
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

    Link<T> myLink;

    IPersistentSet<T> mySet;

    ScalableSet(final StorageImpl aStorage, final int aInitialSize) {
        super(aStorage);

        if (aInitialSize <= BTREE_THRESHOLD) {
            myLink = aStorage.<T>createLink(aInitialSize);
        } else {
            mySet = aStorage.<T>createSet();
        }
    }

    ScalableSet() {
    }

    @Override
    public boolean isEmpty() {
        return size() != 0;
    }

    @Override
    public int size() {
        return myLink != null ? myLink.size() : mySet.size();
    }

    @Override
    public void clear() {
        if (myLink != null) {
            myLink.clear();
            modify();
        } else {
            mySet.clear();
        }
    }

    @Override
    public boolean contains(final Object aObj) {
        return myLink != null ? myLink.contains(aObj) : mySet.contains(aObj);
    }

    @Override
    public Object[] toArray() {
        return myLink != null ? myLink.toArray() : mySet.toArray();
    }

    @Override
    public <E> E[] toArray(final E aArray[]) {
        return myLink != null ? myLink.<E>toArray(aArray) : mySet.<E>toArray(aArray);
    }

    @Override
    public Iterator<T> iterator() {
        return myLink != null ? myLink.iterator() : mySet.iterator();
    }

    private int binarySearch(final T aObj) {
        final Storage storage = getStorage();
        final int oid = storage.getOid(aObj);

        int left = 0;
        int right = myLink.size();

        while (left < right) {
            final int index = (left + right) >> 1;

            if (storage.getOid(myLink.getRaw(index)) > oid) {
                left = index + 1;
            } else {
                right = index;
            }
        }

        return right;
    }

    @Override
    public boolean add(final T aObj) {
        if (myLink != null) {
            final int count = myLink.size();

            int index = binarySearch(aObj);

            if (index < count && myLink.getRaw(index).equals(aObj)) {
                return false;
            }

            if (count == BTREE_THRESHOLD) {
                mySet = getStorage().<T>createSet();

                for (index = 0; index < count; index++) {
                    ((IPersistentSet) mySet).add(myLink.getRaw(index));
                }

                myLink = null;
                modify();
                mySet.add(aObj);
            } else {
                modify();
                myLink.insert(index, aObj);
            }

            return true;
        } else {
            return mySet.add(aObj);
        }
    }

    @Override
    public boolean remove(final Object aObj) {
        if (myLink != null) {
            if (myLink.remove(aObj)) {
                modify();
                return true;
            }

            return false;
        } else {
            return mySet.remove(aObj);
        }
    }

    @Override
    public int hashCode() {
        final Iterator<T> iterator = iterator();

        int hash = 0;

        while (iterator.hasNext()) {
            hash += getStorage().getOid(iterator.next());
        }

        return hash;
    }

    @Override
    public boolean equals(final Object aObj) {
        final Collection collection;

        if (aObj == this) {
            return true;
        }

        if (!(aObj instanceof Set)) {
            return false;
        }

        collection = (Collection) aObj;

        if (collection.size() != size()) {
            return false;
        }

        return containsAll(collection);
    }

    @Override
    public void deallocate() {
        if (mySet != null) {
            mySet.deallocate();
        }

        super.deallocate();
    }

    @Override
    public IterableIterator<T> join(final Iterator<T> aIterator) {
        return aIterator == null ? (IterableIterator<T>) iterator() : new JoinSetIterator<>(getStorage(), iterator(),
                aIterator);
    }
}
