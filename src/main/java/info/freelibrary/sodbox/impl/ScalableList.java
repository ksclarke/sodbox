
package info.freelibrary.sodbox.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import info.freelibrary.sodbox.IPersistentList;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.Storage;

class ScalableList<E> extends PersistentCollection<E> implements IPersistentList<E> {

    static final int BTREE_THRESHOLD = 128;

    Link<E> mySmall;

    IPersistentList<E> myLarge;

    ScalableList(final Storage aStorage, final int aInitialSize) {
        super(aStorage);

        if (aInitialSize <= BTREE_THRESHOLD) {
            mySmall = aStorage.<E>createLink(aInitialSize);
        } else {
            myLarge = aStorage.<E>createList();
        }
    }

    ScalableList() {
    }

    @Override
    public E get(final int aIndex) {
        return mySmall != null ? mySmall.get(aIndex) : myLarge.get(aIndex);
    }

    @Override
    public E set(final int aIndex, final E aObj) {
        return mySmall != null ? mySmall.set(aIndex, aObj) : myLarge.set(aIndex, aObj);
    }

    @Override
    public boolean isEmpty() {
        return mySmall != null ? mySmall.isEmpty() : myLarge.isEmpty();
    }

    @Override
    public int size() {
        return mySmall != null ? mySmall.size() : myLarge.size();
    }

    @Override
    public boolean contains(final Object aObj) {
        return mySmall != null ? mySmall.contains(aObj) : myLarge.contains(aObj);
    }

    @Override
    public <T> T[] toArray(final T aArray[]) {
        return mySmall != null ? mySmall.<T>toArray(aArray) : myLarge.<T>toArray(aArray);
    }

    @Override
    public Object[] toArray() {
        return mySmall != null ? mySmall.toArray() : myLarge.toArray();
    }

    @Override
    public boolean add(final E aObj) {
        add(size(), aObj);
        return true;
    }

    @Override
    public void add(final int aIndex, final E aObj) {
        if (mySmall != null) {
            if (mySmall.size() == BTREE_THRESHOLD) {
                myLarge = getStorage().<E>createList();
                myLarge.addAll(mySmall);
                myLarge.add(aIndex, aObj);

                modify();

                mySmall = null;
            } else {
                mySmall.add(aIndex, aObj);
            }
        } else {
            myLarge.add(aIndex, aObj);
        }
    }

    @Override
    public E remove(final int aIndex) {
        return mySmall != null ? mySmall.remove(aIndex) : myLarge.remove(aIndex);
    }

    @Override
    public void clear() {
        if (myLarge != null) {
            myLarge.clear();
        } else {
            mySmall.clear();
        }
    }

    @Override
    public int indexOf(final Object aObj) {
        return mySmall != null ? mySmall.indexOf(aObj) : myLarge.indexOf(aObj);
    }

    @Override
    public int lastIndexOf(final Object aObj) {
        return mySmall != null ? mySmall.lastIndexOf(aObj) : myLarge.lastIndexOf(aObj);
    }

    @Override
    public boolean addAll(final int aIndex, final Collection<? extends E> aCollection) {
        final Iterator<? extends E> iterator = aCollection.iterator();

        boolean modified = false;
        int index = aIndex;

        while (iterator.hasNext()) {
            add(index++, iterator.next());
            modified = true;
        }

        return modified;
    }

    @Override
    public Iterator<E> iterator() {
        return mySmall != null ? mySmall.iterator() : myLarge.iterator();
    }

    @Override
    public ListIterator<E> listIterator() {
        return listIterator(0);
    }

    @Override
    public ListIterator<E> listIterator(final int aIndex) {
        return mySmall != null ? mySmall.listIterator(aIndex) : myLarge.listIterator(aIndex);
    }

    @Override
    public List<E> subList(final int aFromIndex, final int aToIndex) {
        return mySmall != null ? mySmall.subList(aFromIndex, aToIndex) : myLarge.subList(aFromIndex, aToIndex);
    }

}
