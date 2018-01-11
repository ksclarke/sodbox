
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

    class TtreeIterator<T> extends IterableIterator<T> implements PersistentIterator {

        boolean removed;

        ArrayList list;

        int i;

        TtreeIterator(final ArrayList list) {
            this.list = list;
            i = -1;
        }

        @Override
        public boolean hasNext() {
            return i + 1 < list.size();
        }

        @Override
        public T next() {
            if (i + 1 >= list.size()) {
                throw new NoSuchElementException();
            }

            removed = false;

            return (T) list.get(++i);
        }

        @Override
        public int nextOid() {
            if (i + 1 >= list.size()) {
                return 0;
            }

            removed = false;

            return getStorage().getOid(list.get(++i));
        }

        @Override
        public void remove() {
            if (removed || i < 0 || i >= list.size()) {
                throw new IllegalStateException();
            }

            Ttree.this.remove(list.get(i));

            list.remove(i--);
            removed = true;
        }
    }

    /**
     * Get all objects in the index as array ordered by index key.
     *
     * @return array of objects in the index ordered by key value
     */
    static final Object[] emptySelection = new Object[0];

    private PersistentComparator<T> comparator;

    private boolean unique;

    private TtreePage root;

    private int nMembers;

    Ttree(final Storage db, final PersistentComparator<T> comparator, final boolean unique) {
        super(db);
        this.comparator = comparator;
        this.unique = unique;
    }

    /**
     * Add new member to collection
     *
     * @param obj new member
     * @return <code>true</code> if object is successfully added in the index, <code>false</code> if collection was
     *         declared as unique and there is already member with such value of the key in the collection.
     */
    @Override
    public boolean add(final T obj) {
        TtreePage newRoot;

        if (root == null) {
            newRoot = new TtreePage(getStorage(), obj);
        } else {
            final TtreePage.PageReference ref = new TtreePage.PageReference(root);

            if (root.insert(comparator, obj, unique, ref) == TtreePage.NOT_UNIQUE) {
                return false;
            }

            newRoot = ref.pg;
        }

        root = newRoot;
        nMembers += 1;
        modify();

        return true;
    }

    /**
     * Remove all objects from the collection
     */
    @Override
    public void clear() {
        if (root != null) {
            root.prune();
            root = null;
            nMembers = 0;
            modify();
        }
    }

    @Override
    public boolean contains(final Object member) {
        return root != null && member != null ? root.contains(comparator, member) : false;
    }

    @Override
    public boolean containsKey(final Object key) {
        return root != null && key != null ? root.containsKey(comparator, key) : false;
    }

    /**
     * Check if collection contains a specified member.
     *
     * @return <code>true</code> if specified member belongs to the collection
     */
    @Override
    public boolean containsObject(final T member) {
        return root != null && member != null ? root.containsObject(comparator, member) : false;
    }

    /**
     * T-Tree destructor
     */
    @Override
    public void deallocate() {
        if (root != null) {
            root.prune();
        }

        super.deallocate();
    }

    @Override
    public T get(final Object key) {
        if (root != null) {
            final ArrayList list = new ArrayList();

            root.find(comparator, key, 1, key, 1, list);

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
    public Object[] get(final Object from, final boolean fromInclusive, final Object till,
            final boolean tillInclusive) {
        return getList(from, fromInclusive, till, tillInclusive).toArray();
    }

    @Override
    public Object[] get(final Object from, final Object till) {
        return getList(from, till).toArray();
    }

    /**
     * Get comparator used in this collection
     *
     * @return collection comparator
     */
    @Override
    public PersistentComparator<T> getComparator() {
        return comparator;
    }

    @Override
    public ArrayList<T> getList(final Object from, final boolean fromInclusive, final Object till,
            final boolean tillInclusive) {
        final ArrayList list = new ArrayList();
        if (root != null) {
            root.find(comparator, from, fromInclusive ? 1 : 0, till, tillInclusive ? 1 : 0, list);
        }
        return list;
    }

    @Override
    public ArrayList<T> getList(final Object from, final Object till) {
        final ArrayList list = new ArrayList();
        if (root != null) {
            root.find(comparator, from, 1, till, 1, list);
        }
        return list;
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(null, null);
    }

    @Override
    public IterableIterator<T> iterator(final Object from, final boolean fromInclusive, final Object till,
            final boolean tillInclusive) {
        final ArrayList list = new ArrayList();

        if (root != null) {
            root.find(comparator, from, fromInclusive ? 1 : 0, till, tillInclusive ? 1 : 0, list);
        }

        return new TtreeIterator(list);
    }

    @Override
    public IterableIterator<T> iterator(final Object from, final Object till) {
        return iterator(from, true, till, true);
    }

    @Override
    public boolean recursiveLoading() {
        return false;
    }

    /**
     * Remove member from collection
     *
     * @param obj member to be removed
     * @return <code>true</code> in case of success, <code>false</code> if there is no such key in the collection
     */
    @Override
    public boolean remove(final Object obj) {
        if (root != null) {
            final TtreePage.PageReference ref = new TtreePage.PageReference(root);

            if (root.remove(comparator, obj, ref) != TtreePage.NOT_FOUND) {
                root = ref.pg;
                nMembers -= 1;
                modify();
                return true;
            }
        }

        return false;
    }

    /**
     * Get number of objects in the collection
     *
     * @return number of objects in the collection
     */
    @Override
    public int size() {
        return nMembers;
    }

    @Override
    public Object[] toArray() {
        if (root == null) {
            return emptySelection;
        }

        final Object[] arr = new Object[nMembers];
        root.toArray(arr, 0);
        return arr;
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
    public <E> E[] toArray(E[] arr) {
        if (arr.length < nMembers) {
            arr = (E[]) Array.newInstance(arr.getClass().getComponentType(), nMembers);
        }

        if (root != null) {
            root.toArray(arr, 0);
        }

        if (arr.length > nMembers) {
            arr[nMembers] = null;
        }

        return arr;
    }

}
