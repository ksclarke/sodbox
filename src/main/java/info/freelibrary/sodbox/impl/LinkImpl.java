
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

import info.freelibrary.sodbox.EmbeddedLink;
import info.freelibrary.sodbox.ICloneable;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.PersistentIterator;

public class LinkImpl<T> implements EmbeddedLink<T>, ICloneable {

    Object[] myArray;

    int myUsed;

    transient Object myOwner;

    transient StorageImpl myStorage;

    LinkImpl() {
    }

    /**
     * Creates a link from the supplied storage and initial size.
     *
     * @param aStorage A storage
     * @param aInitSize An initial size
     */
    public LinkImpl(final StorageImpl aStorage, final int aInitSize) {
        myStorage = aStorage;
        myArray = new Object[aInitSize];
    }

    /**
     * Creates a link from the supplied storage, object array, and owner.
     *
     * @param aStorage A storage
     * @param aArray An array
     * @param aOwner An owner
     */
    public LinkImpl(final StorageImpl aStorage, final T[] aArray, final Object aOwner) {
        myStorage = aStorage;
        myArray = aArray;
        myOwner = aOwner;
        myUsed = aArray.length;
    }

    /**
     * Creates a link from the supplied storage, object array, and owner.
     *
     * @param aStorage A storage
     * @param aLink A link
     * @param aOwner An owner
     */
    public LinkImpl(final StorageImpl aStorage, final Link aLink, final Object aOwner) {
        myStorage = aStorage;
        myUsed = aLink.size();
        myArray = new Object[myUsed];
        myOwner = aOwner;

        System.arraycopy(myArray, 0, aLink.toRawArray(), 0, myUsed);
    }

    private void modify() {
        if (myOwner != null) {
            myStorage.modify(myOwner);
        }
    }

    @Override
    public int size() {
        return myUsed;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public void setSize(final int aNewSize) {
        if (aNewSize < myUsed) {
            for (int i = myUsed; --i >= aNewSize; myArray[i] = null) {
            }
        } else {
            reserveSpace(aNewSize - myUsed);
        }
        myUsed = aNewSize;
        modify();
    }

    @Override
    public T get(final int aIndex) {
        if (aIndex < 0 || aIndex >= myUsed) {
            throw new IndexOutOfBoundsException();
        }

        return loadElem(aIndex);
    }

    @Override
    public Object getRaw(final int aIndex) {
        if (aIndex < 0 || aIndex >= myUsed) {
            throw new IndexOutOfBoundsException();
        }

        return myArray[aIndex];
    }

    @Override
    public void pin() {
        for (int i = 0, n = myUsed; i < n; i++) {
            myArray[i] = loadElem(i);
        }
    }

    @Override
    public void unpin() {
        for (int i = 0, n = myUsed; i < n; i++) {
            final Object elem = myArray[i];

            if (elem != null && myStorage.isLoaded(elem)) {
                myArray[i] = new PersistentStub(myStorage, myStorage.getOid(elem));
            }
        }
    }

    @Override
    public T set(final int aIndex, final T aObject) {
        if (aIndex < 0 || aIndex >= myUsed) {
            throw new IndexOutOfBoundsException();
        }

        final T previous = loadElem(aIndex);

        myArray[aIndex] = aObject;
        modify();

        return previous;
    }

    @Override
    public void setObject(final int aIndex, final T aObject) {
        if (aIndex < 0 || aIndex >= myUsed) {
            throw new IndexOutOfBoundsException();
        }

        myArray[aIndex] = aObject;
        modify();
    }

    @Override
    public boolean isEmpty() {
        return myUsed == 0;
    }

    protected void removeRange(final int aFromIndex, final int aToIndex) {
        int size = myUsed;
        final int numMoved = size - aToIndex;

        System.arraycopy(myArray, aToIndex, myArray, aFromIndex, numMoved);

        // Let gc do its work
        final int newSize = size - (aToIndex - aFromIndex);

        while (size != newSize) {
            myArray[--size] = null;
        }

        myUsed = size;
        modify();
    }

    @Override
    public void removeObject(final int aIndex) {
        if (aIndex < 0 || aIndex >= myUsed) {
            throw new IndexOutOfBoundsException();
        }

        myUsed -= 1;

        System.arraycopy(myArray, aIndex + 1, myArray, aIndex, myUsed - aIndex);

        myArray[myUsed] = null;
        modify();
    }

    @Override
    public T remove(final int aIndex) {
        if (aIndex < 0 || aIndex >= myUsed) {
            throw new IndexOutOfBoundsException();
        }

        final T obj = loadElem(aIndex);

        myUsed -= 1;

        System.arraycopy(myArray, aIndex + 1, myArray, aIndex, myUsed - aIndex);

        myArray[myUsed] = null;
        modify();

        return obj;
    }

    void reserveSpace(final int aLength) {
        if (myUsed + aLength > myArray.length) {
            final Object[] newArr = new Object[myUsed + aLength > myArray.length * 2 ? myUsed + aLength
                    : myArray.length * 2];

            System.arraycopy(myArray, 0, newArr, 0, myUsed);

            myArray = newArr;
        }
    }

    @Override
    public void add(final int aIndex, final T aObject) {
        insert(aIndex, aObject);
    }

    @Override
    public void insert(final int aIndex, final T aObject) {
        if (aIndex < 0 || aIndex > myUsed) {
            throw new IndexOutOfBoundsException();
        }

        reserveSpace(1);

        System.arraycopy(myArray, aIndex, myArray, aIndex + 1, myUsed - aIndex);

        myArray[aIndex] = aObject;
        myUsed += 1;
        modify();
    }

    @Override
    public boolean add(final T aObject) {
        reserveSpace(1);
        myArray[myUsed++] = aObject;
        modify();

        return true;
    }

    @Override
    public void addAll(final T[] aArray) {
        addAll(aArray, 0, aArray.length);
    }

    @Override
    public boolean addAll(final int aIndex, final Collection<? extends T> aCollection) {
        final Iterator<? extends T> iterator = aCollection.iterator();

        int index = aIndex;
        boolean modified = false;

        while (iterator.hasNext()) {
            add(index++, iterator.next());
            modified = true;
        }

        return modified;
    }

    @Override
    public void addAll(final T[] aArray, final int aFromIndex, final int aLength) {
        reserveSpace(aLength);

        System.arraycopy(aArray, aFromIndex, myArray, myUsed, aLength);

        myUsed += aLength;
        modify();
    }

    @Override
    public boolean addAll(final Link<T> aLink) {
        final int count = aLink.size();

        reserveSpace(count);

        for (int i = 0, j = myUsed; i < count; i++, j++) {
            myArray[j] = aLink.getRaw(i);
        }

        myUsed += count;
        modify();

        return true;
    }

    @Override
    public Object[] toRawArray() {
        return myArray;
    }

    @Override
    public Object[] toArray() {
        final Object[] a = new Object[myUsed];

        for (int index = myUsed; --index >= 0;) {
            a[index] = loadElem(index);
        }

        return a;
    }

    @SuppressWarnings({ "hiding", "unchecked" })
    @Override
    public <T> T[] toArray(final T[] aArray) {
        T[] array = aArray;

        if (array.length < myUsed) {
            array = (T[]) Array.newInstance(array.getClass().getComponentType(), myUsed);
        }

        for (int index = myUsed; --index >= 0;) {
            array[index] = (T) loadElem(index);
        }

        if (array.length > myUsed) {
            array[myUsed] = null;
        }

        return array;
    }

    @Override
    public boolean contains(final Object aObject) {
        return indexOf(aObject) >= 0;
    }

    @Override
    public boolean containsObject(final T aObject) {
        return indexOfObject(aObject) >= 0;
    }

    @Override
    public int lastIndexOfObject(final Object aObject) {
        final Object[] a = myArray;
        final int oid = myStorage.getOid(aObject);

        if (oid != 0) {
            for (int index = myUsed; --index >= 0;) {
                if (myStorage.getOid(a[index]) == oid) {
                    return index;
                }
            }
        } else {
            for (int index = myUsed; --index >= 0;) {
                if (a[index] == aObject) {
                    return index;
                }
            }
        }

        return -1;
    }

    @Override
    public int indexOfObject(final Object aObject) {
        final Object[] array = myArray;
        final int oid = myStorage.getOid(aObject);

        if (oid != 0) {
            for (int index = 0, n = myUsed; index < n; index++) {
                if (myStorage.getOid(array[index]) == oid) {
                    return index;
                }
            }
        } else {
            for (int index = 0, n = myUsed; index < n; index++) {
                if (array[index] == aObject) {
                    return index;
                }
            }
        }

        return -1;
    }

    @Override
    public int indexOf(final Object aObject) {
        if (aObject == null) {
            for (int index = 0, n = myUsed; index < n; index++) {
                if (myArray[index] == null) {
                    return index;
                }
            }
        } else {
            for (int index = 0, n = myUsed; index < n; index++) {
                if (aObject.equals(loadElem(index))) {
                    return index;
                }
            }
        }

        return -1;
    }

    @Override
    public int lastIndexOf(final Object aObject) {
        if (aObject == null) {
            for (int index = myUsed; --index >= 0;) {
                if (myArray[index] == null) {
                    return index;
                }
            }
        } else {
            for (int index = myUsed; --index >= 0;) {
                if (aObject.equals(loadElem(index))) {
                    return index;
                }
            }
        }

        return -1;
    }

    @Override
    public boolean containsElement(final int aIndex, final T aObject) {
        final Object element = myArray[aIndex];
        return element == aObject || element != null && myStorage.getOid(element) == myStorage.getOid(aObject);
    }

    @Override
    public void deallocateMembers() {
        for (int index = myUsed; --index >= 0;) {
            myStorage.deallocate(myArray[index]);
            myArray[index] = null;
        }

        myUsed = 0;
        modify();
    }

    @Override
    public void clear() {
        for (int index = myUsed; --index >= 0;) {
            myArray[index] = null;
        }

        myUsed = 0;
        modify();
    }

    @Override
    public List<T> subList(final int aFromIndex, final int aToIndex) {
        return new SubList<>(this, aFromIndex, aToIndex);
    }

    @Override
    public boolean remove(final Object aObject) {
        final int index = indexOf(aObject);

        if (index >= 0) {
            remove(index);

            return true;
        }

        return false;
    }

    @Override
    public boolean containsAll(final Collection<?> aCollection) {
        final Iterator<?> iterator = aCollection.iterator();

        while (iterator.hasNext()) {
            if (!contains(iterator.next())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean addAll(final Collection<? extends T> aCollection) {
        boolean modified = false;

        final Iterator<? extends T> iterator = aCollection.iterator();

        while (iterator.hasNext()) {
            if (add(iterator.next())) {
                modified = true;
            }
        }

        return modified;
    }

    @Override
    public boolean removeAll(final Collection<?> aCollection) {
        boolean modified = false;

        final Iterator<?> iterator = iterator();

        while (iterator.hasNext()) {
            if (aCollection.contains(iterator.next())) {
                iterator.remove();
                modified = true;
            }
        }

        return modified;
    }

    @Override
    public boolean retainAll(final Collection<?> aCollection) {
        boolean modified = false;

        final Iterator<T> iterator = iterator();

        while (iterator.hasNext()) {
            if (!aCollection.contains(iterator.next())) {
                iterator.remove();
                modified = true;
            }
        }

        return modified;
    }

    @Override
    public Iterator<T> iterator() {
        return new LinkIterator(0);
    }

    @Override
    public ListIterator<T> listIterator(final int aIndex) {
        return new LinkIterator(aIndex);
    }

    @Override
    public ListIterator<T> listIterator() {
        return listIterator(0);
    }

    @SuppressWarnings("unchecked")
    private T loadElem(final int aIndex) {
        Object element = myArray[aIndex];

        if (element != null && myStorage.isRaw(element)) {
            element = myStorage.lookupObject(myStorage.getOid(element), null);
        }

        return (T) element;
    }

    @Override
    public void setOwner(final Object aObject) {
        myOwner = aObject;
    }

    @Override
    public Object getOwner() {
        return myOwner;
    }

    static class SubList<T> extends AbstractList<T> implements RandomAccess {

        private final LinkImpl<T> myList;

        private final int myOffset;

        private int mySize;

        SubList(final LinkImpl<T> aList, final int aFromIndex, final int aToIndex) {
            if (aFromIndex < 0) {
                throw new IndexOutOfBoundsException("fromIndex = " + aFromIndex);
            }

            if (aToIndex > aList.size()) {
                throw new IndexOutOfBoundsException("toIndex = " + aToIndex);
            }

            if (aFromIndex > aToIndex) {
                throw new IllegalArgumentException("fromIndex(" + aFromIndex + ") > toIndex(" + aToIndex + ")");
            }

            myList = aList;
            myOffset = aFromIndex;
            mySize = aToIndex - aFromIndex;
        }

        @Override
        public T set(final int aIndex, final T aElement) {
            rangeCheck(aIndex);

            return myList.set(aIndex + myOffset, aElement);
        }

        @Override
        public T get(final int aIndex) {
            rangeCheck(aIndex);

            return myList.get(aIndex + myOffset);
        }

        @Override
        public int size() {
            return mySize;
        }

        @Override
        public void add(final int aIndex, final T aElement) {
            if (aIndex < 0 || aIndex > mySize) {
                throw new IndexOutOfBoundsException();
            }

            myList.add(aIndex + myOffset, aElement);
            mySize++;
        }

        @Override
        public T remove(final int aIndex) {
            final T result;

            rangeCheck(aIndex);
            result = myList.remove(aIndex + myOffset);
            mySize--;

            return result;
        }

        @Override
        protected void removeRange(final int aFromIndex, final int aToIndex) {
            myList.removeRange(aFromIndex + myOffset, aToIndex + myOffset);
            mySize -= aToIndex - aFromIndex;
        }

        @Override
        public boolean addAll(final Collection<? extends T> aCollection) {
            return addAll(mySize, aCollection);
        }

        @Override
        public boolean addAll(final int aIndex, final Collection<? extends T> aCollection) {
            if (aIndex < 0 || aIndex > mySize) {
                throw new IndexOutOfBoundsException("Index:" + aIndex + ", Size:" + mySize);
            }

            final int cSize = aCollection.size();

            if (cSize == 0) {
                return false;
            }

            myList.addAll(myOffset + aIndex, aCollection);
            mySize += cSize;

            return true;
        }

        @Override
        public Iterator<T> iterator() {
            return listIterator();
        }

        @Override
        public ListIterator<T> listIterator(final int aIndex) {
            if (aIndex < 0 || aIndex > mySize) {
                throw new IndexOutOfBoundsException("Index:  " + aIndex + ", Size:  " + mySize);
            }

            return new ListIterator<T>() {

                private final ListIterator<T> myIterator = myList.listIterator(aIndex + myOffset);

                @Override
                public boolean hasNext() {
                    return nextIndex() < mySize;
                }

                @Override
                public T next() {
                    if (hasNext()) {
                        return myIterator.next();
                    } else {
                        throw new NoSuchElementException();
                    }
                }

                @Override
                public boolean hasPrevious() {
                    return previousIndex() >= 0;
                }

                @Override
                public T previous() {
                    if (hasPrevious()) {
                        return myIterator.previous();
                    } else {
                        throw new NoSuchElementException();
                    }
                }

                @Override
                public int nextIndex() {
                    return myIterator.nextIndex() - myOffset;
                }

                @Override
                public int previousIndex() {
                    return myIterator.previousIndex() - myOffset;
                }

                @Override
                public void remove() {
                    myIterator.remove();
                    mySize--;
                }

                @Override
                public void set(final T aObject) {
                    myIterator.set(aObject);
                }

                @Override
                public void add(final T aObject) {
                    myIterator.add(aObject);
                    mySize++;
                }
            };
        }

        @Override
        public List<T> subList(final int aFromIndex, final int aToIndex) {
            return new SubList<>(myList, myOffset + aFromIndex, myOffset + aToIndex);
        }

        private void rangeCheck(final int aIndex) {
            if (aIndex < 0 || aIndex >= mySize) {
                throw new IndexOutOfBoundsException("Index: " + aIndex + ",Size: " + mySize);
            }
        }
    }

    class LinkIterator implements PersistentIterator, ListIterator<T> {

        private int myIndex;

        private int myLast;

        LinkIterator(final int aIndex) {
            myIndex = aIndex;
            myLast = -1;
        }

        @Override
        public boolean hasNext() {
            return myIndex < size();
        }

        @Override
        public T next() throws NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            myLast = myIndex;

            return get(myIndex++);
        }

        @Override
        public int nextIndex() {
            return myIndex;
        }

        @Override
        public boolean hasPrevious() {
            return myIndex > 0;
        }

        @Override
        public T previous() throws NoSuchElementException {
            if (!hasPrevious()) {
                throw new NoSuchElementException();
            }

            return get(myLast = --myIndex);
        }

        @Override
        public int previousIndex() {
            return myIndex - 1;
        }

        @Override
        public int nextOID() throws NoSuchElementException {
            if (!hasNext()) {
                return 0;
            }

            return myStorage.getOid(getRaw(myIndex++));
        }

        @Override
        public void remove() {
            if (myLast < 0) {
                throw new IllegalStateException();
            }

            removeObject(myLast);

            if (myLast < myIndex) {
                myIndex -= 1;
            }
        }

        @Override
        public void set(final T aObject) {
            if (myLast < 0) {
                throw new IllegalStateException();
            }

            setObject(myLast, aObject);
        }

        @Override
        public void add(final T aObject) {
            insert(myIndex++, aObject);
            myLast = -1;
        }
    }

}
