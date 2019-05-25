
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.PersistentComparator;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.SortedCollection;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

public class Ttree<T> extends PersistentCollection<T> implements SortedCollection<T> {

    /**
     * Get all objects in the index as array ordered by index key.
     *
     * @return array of objects in the index ordered by key value
     */
    static final Object[] EMPTY_SELECTION = new Object[0];

    private PersistentComparator<T> myComparator;

    private boolean isUniqueKey;

    private TtreePage myRoot;

    private int myMemberCount;

    @SuppressWarnings("unused")
    private Ttree() {
    }

    Ttree(final Storage aStorage, final PersistentComparator<T> aComparator, final boolean aUniqueKey) {
        super(aStorage);

        myComparator = aComparator;
        isUniqueKey = aUniqueKey;
    }

    /**
     * Get comparator used in this collection
     *
     * @return collection comparator
     */
    @Override
    public PersistentComparator<T> getComparator() {
        return myComparator;
    }

    @Override
    public boolean recursiveLoading() {
        return false;
    }

    @Override
    public T get(final Object aKey) {
        if (myRoot != null) {
            final ArrayList list = new ArrayList();

            myRoot.find(myComparator, aKey, 1, aKey, 1, list);

            if (list.size() > 1) {
                throw new StorageError(StorageError.KEY_NOT_UNIQUE);
            } else if (list.size() == 0) {
                return null;
            } else {
                return (T) list.get(0);
            }
        }

        return null;
    }

    @Override
    public ArrayList<T> getList(final Object aFrom, final Object aTo) {
        final ArrayList list = new ArrayList();

        if (myRoot != null) {
            myRoot.find(myComparator, aFrom, 1, aTo, 1, list);
        }

        return list;
    }

    @Override
    public ArrayList<T> getList(final Object aFrom, final boolean aFromInclusive, final Object aTo,
            final boolean aToInclusive) {
        final ArrayList list = new ArrayList();

        if (myRoot != null) {
            myRoot.find(myComparator, aFrom, aFromInclusive ? 1 : 0, aTo, aToInclusive ? 1 : 0, list);
        }

        return list;
    }

    @Override
    public Object[] get(final Object aFrom, final Object aTo) {
        return getList(aFrom, aTo).toArray();
    }

    @Override
    public Object[] get(final Object aFrom, final boolean aFromInclusive, final Object aTo,
            final boolean aToInclusive) {
        return getList(aFrom, aFromInclusive, aTo, aToInclusive).toArray();
    }

    /**
     * Add a new member to the collection.
     *
     * @param aObj A new member
     * @return <code>true</code> if object is successfully added to the index; <code>false</code> if collection was
     *         declared as unique and there is already member with the same key in the collection.
     */
    @Override
    public boolean add(final T aObj) {
        final TtreePage newRoot;

        if (myRoot == null) {
            newRoot = new TtreePage(getStorage(), aObj);
        } else {
            final TtreePage.PageReference pageRef = new TtreePage.PageReference(myRoot);

            if (myRoot.insert(myComparator, aObj, isUniqueKey, pageRef) == TtreePage.NOT_UNIQUE) {
                return false;
            }

            newRoot = pageRef.myPage;
        }

        myRoot = newRoot;
        myMemberCount += 1;
        modify();

        return true;
    }

    /**
     * Check if collection contains the supplied member.
     *
     * @return <code>true</code> if specified member belongs to the collection
     */
    @Override
    public boolean containsObject(final T aMember) {
        return (myRoot != null && aMember != null) ? myRoot.containsObject(myComparator, aMember) : false;
    }

    @Override
    public boolean contains(final Object aMember) {
        return (myRoot != null && aMember != null) ? myRoot.contains(myComparator, aMember) : false;
    }

    @Override
    public boolean containsKey(final Object aKey) {
        return (myRoot != null && aKey != null) ? myRoot.containsKey(myComparator, aKey) : false;
    }

    /**
     * Remove a member from the collection.
     *
     * @param aObj member to be removed
     * @return <code>true</code> in case of success; <code>false</code> if there is no such key in the collection
     */
    @Override
    public boolean remove(final Object aObj) {
        if (myRoot != null) {
            final TtreePage.PageReference pageRef = new TtreePage.PageReference(myRoot);

            if (myRoot.remove(myComparator, aObj, pageRef) != TtreePage.NOT_FOUND) {
                myRoot = pageRef.myPage;
                myMemberCount -= 1;

                modify();

                return true;
            }
        }

        return false;
    }

    /**
     * Get the number of objects in the collection.
     *
     * @return The number of objects in the collection
     */
    @Override
    public int size() {
        return myMemberCount;
    }

    /**
     * Remove all objects from the collection.
     */
    @Override
    public void clear() {
        if (myRoot != null) {
            myRoot.prune();
            myRoot = null;
            myMemberCount = 0;

            modify();
        }
    }

    /**
     * T-Tree destructor.
     */
    @Override
    public void deallocate() {
        if (myRoot != null) {
            myRoot.prune();
        }

        super.deallocate();
    }

    @Override
    public Object[] toArray() {
        final Object[] array;

        if (myRoot == null) {
            return EMPTY_SELECTION;
        }

        array = new Object[myMemberCount];
        myRoot.toArray(array, 0);

        return array;
    }

    /**
     * Get all objects in the index as array ordered by index key. The runtime type of the returned array is that of
     * the specified array. If the index fits in the specified array, it is returned therein. Otherwise, a new array
     * is allocated with the runtime type of the specified array and the size of this index.
     * <p>
     * If this index fits in the specified array with room to spare (i.e., the array has more elements than this
     * index), the element in the array immediately following the end of the index is set to <tt>null</tt>. This is
     * useful in determining the length of this index <i>only</i> if the caller knows that this index does not contain
     * any <tt>null</tt> elements.)
     * <p>
     *
     * @return array of objects in the index ordered by key value
     */
    @Override
    public <E> E[] toArray(final E[] aArray) {
        final E[] array;

        if (aArray.length < myMemberCount) {
            array = (E[]) Array.newInstance(aArray.getClass().getComponentType(), myMemberCount);
        } else {
            array = aArray;
        }

        if (myRoot != null) {
            myRoot.toArray(array, 0);
        }

        if (array.length > myMemberCount) {
            array[myMemberCount] = null;
        }

        return array;
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(null, null);
    }

    @Override
    public IterableIterator<T> iterator(final Object aFrom, final Object aTo) {
        return iterator(aFrom, true, aTo, true);
    }

    @Override
    public IterableIterator<T> iterator(final Object aFrom, final boolean aFromInclusive, final Object aTo,
            final boolean aToInclusive) {
        final ArrayList list = new ArrayList();

        if (myRoot != null) {
            myRoot.find(myComparator, aFrom, aFromInclusive ? 1 : 0, aTo, aToInclusive ? 1 : 0, list);
        }

        return new TtreeIterator(list);
    }

    class TtreeIterator<T> extends IterableIterator<T> implements PersistentIterator {

        boolean isRemoved;

        ArrayList myList;

        int myIndex;

        TtreeIterator(final ArrayList aList) {
            myList = aList;
            myIndex = -1;
        }

        @Override
        public T next() {
            if (myIndex + 1 >= myList.size()) {
                throw new NoSuchElementException();
            }

            isRemoved = false;

            return (T) myList.get(++myIndex);
        }

        @Override
        public int nextOID() {
            if (myIndex + 1 >= myList.size()) {
                return 0;
            }

            isRemoved = false;

            return getStorage().getOid(myList.get(++myIndex));
        }

        @Override
        public void remove() {
            if (isRemoved || myIndex < 0 || myIndex >= myList.size()) {
                throw new IllegalStateException();
            }

            Ttree.this.remove(myList.get(myIndex));

            myList.remove(myIndex--);
            isRemoved = true;
        }

        @Override
        public boolean hasNext() {
            return myIndex + 1 < myList.size();
        }
    }
}
