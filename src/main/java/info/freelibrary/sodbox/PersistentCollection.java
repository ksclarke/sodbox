
package info.freelibrary.sodbox;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public abstract class PersistentCollection<T> extends PersistentResource implements Collection<T> {

    /**
     * Default constructor
     */
    public PersistentCollection() {
    }

    /**
     * Constructor of the collection associated with the specified storage
     *
     * @param aStorage storage associated with the collection
     */
    public PersistentCollection(final Storage aStorage) {
        super(aStorage);
    }

    /**
     * Returns <tt>true</tt> if this collection contains all of the elements in the specified collection. This
     * implementation iterates over the specified collection, checking each element returned by the iterator in turn
     * to see if it's contained in this collection. If all elements are so contained <tt>true</tt> is returned,
     * otherwise <tt>false</tt>.
     *
     * @param aCollection collection to be checked for containment in this collection.
     * @return <tt>true</tt> if this collection contains all of the elements in the specified collection.
     * @throws NullPointerException if the specified collection is null.
     * @see #contains(Object)
     */
    @Override
    public boolean containsAll(final Collection<?> aCollection) {
        final Iterator<?> e = aCollection.iterator();

        while (e.hasNext()) {
            if (!contains(e.next())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Adds all of the elements in the specified collection to this collection (optional operation). The behavior of
     * this operation is undefined if the specified collection is modified while the operation is in progress. (This
     * implies that the behavior of this call is undefined if the specified collection is this collection, and this
     * collection is nonempty). This implementation iterates over the specified collection, and adds each object
     * returned by the iterator to this collection, in turn. Note that this implementation will throw an
     * <tt>UnsupportedOperationException</tt> unless <tt>add</tt> is overridden (assuming the specified collection is
     * non-empty).
     *
     * @param aCollection collection whose elements are to be added to this collection.
     * @return <tt>true</tt> if this collection changed as a result of the call.
     * @throws UnsupportedOperationException if this collection does not support the <tt>addAll</tt> method.
     * @throws NullPointerException if the specified collection is null.
     * @see #add(Object)
     */
    @Override
    public boolean addAll(final Collection<? extends T> aCollection) {
        boolean modified = false;
        final Iterator<? extends T> e = aCollection.iterator();

        while (e.hasNext()) {
            if (add(e.next())) {
                modified = true;
            }
        }

        return modified;
    }

    /**
     * Removes from this collection all of its elements that are contained in the specified collection (optional
     * operation). This implementation iterates over this collection, checking each element returned by the iterator
     * in turn to see if it's contained in the specified collection. If it's so contained, it's removed from this
     * collection with the iterator's <tt>remove</tt> method. Note that this implementation will throw an
     * <tt>UnsupportedOperationException</tt> if the iterator returned by the <tt>iterator</tt> method does not
     * implement the <tt>remove</tt> method and this collection contains one or more elements in common with the
     * specified collection.
     *
     * @param aCollection elements to be removed from this collection.
     * @return <tt>true</tt> if this collection changed as a result of the call.
     * @throws UnsupportedOperationException if the <tt>removeAll</tt> method is not supported by this collection.
     * @throws NullPointerException if the specified collection is null.
     * @see #remove(Object)
     * @see #contains(Object)
     */
    @Override
    public boolean removeAll(final Collection<?> aCollection) {
        boolean modified = false;
        final Iterator<?> i = aCollection.iterator();

        while (i.hasNext()) {
            modified |= remove(i.next());
        }

        return modified;
    }

    /**
     * Retains only the elements in this collection that are contained in the specified collection (optional
     * operation). In other words, removes from this collection all of its elements that are not contained in the
     * specified collection. This implementation iterates over this collection, checking each element returned by the
     * iterator in turn to see if it's contained in the specified collection. If it's not so contained, it's removed
     * from this collection with the iterator's <tt>remove</tt> method. Note that this implementation will throw an
     * <tt>UnsupportedOperationException</tt> if the iterator returned by the <tt>iterator</tt> method does not
     * implement the <tt>remove</tt> method and this collection contains one or more elements not present in the
     * specified collection.
     *
     * @param aCollection elements to be retained in this collection.
     * @return <tt>true</tt> if this collection changed as a result of the call.
     * @throws UnsupportedOperationException if the <tt>retainAll</tt> method is not supported by this Collection.
     * @throws NullPointerException if the specified collection is null.
     * @see #remove(Object)
     * @see #contains(Object)
     */
    @Override
    public boolean retainAll(final Collection<?> aCollection) {
        final ArrayList<T> toBeRemoved = new ArrayList<>();
        final Iterator<T> i = iterator();

        while (i.hasNext()) {
            final T o = i.next();

            if (!aCollection.contains(o)) {
                toBeRemoved.add(o);
            }
        }

        final int n = toBeRemoved.size();

        for (int j = 0; j < n; j++) {
            remove(toBeRemoved.get(j));
        }

        return n != 0;
    }

    /**
     * Returns <tt>true</tt> if this collection contains the specified element. More formally, returns <tt>true</tt>
     * if and only if this collection contains at least one element <tt>e</tt> such that
     * <tt>(o==null ? e==null : o.equals(e))</tt>. This implementation iterates over the elements in the collection,
     * checking each element in turn for equality with the specified element.
     *
     * @param aObject object to be checked for containment in this collection.
     * @return <tt>true</tt> if this collection contains the specified element.
     */
    @Override
    public boolean contains(final Object aObject) {
        final Iterator<T> e = iterator();

        if (aObject == null) {
            while (e.hasNext()) {
                if (e.next() == null) {
                    return true;
                }
            }
        } else {
            while (e.hasNext()) {
                if (aObject.equals(e.next())) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Removes a single instance of the specified element from this collection, if it is present (optional operation).
     * More formally, removes an element <tt>e</tt> such that <tt>(o==null ? e==null :
     * o.equals(e))</tt>, if the collection contains one or more such elements. Returns <tt>true</tt> if the
     * collection contained the specified element (or equivalently, if the collection changed as a result of the
     * call). This implementation iterates over the collection looking for the specified element. If it finds the
     * element, it removes the element from the collection using the iterator's remove method. Note that this
     * implementation throws an <tt>UnsupportedOperationException</tt> if the iterator returned by this collection's
     * iterator method does not implement the <tt>remove</tt> method and this collection contains the specified
     * object.
     *
     * @param aObject element to be removed from this collection, if present.
     * @return <tt>true</tt> if the collection contained the specified element.
     * @throws UnsupportedOperationException if the <tt>remove</tt> method is not supported by this collection.
     */
    @Override
    public boolean remove(final Object aObject) {
        final Iterator<T> e = iterator();

        if (aObject == null) {
            while (e.hasNext()) {
                if (e.next() == null) {
                    e.remove();

                    return true;
                }
            }
        } else {
            while (e.hasNext()) {
                if (aObject.equals(e.next())) {
                    e.remove();

                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Ensures that this collection contains the specified element (optional operation). Returns <tt>true</tt> if the
     * collection changed as a result of the call. (Returns <tt>false</tt> if this collection does not permit
     * duplicates and already contains the specified element.) Collections that support this operation may place
     * limitations on what elements may be added to the collection. In particular, some collections will refuse to add
     * <tt>null</tt> elements, and others will impose restrictions on the type of elements that may be added.
     * Collection classes should clearly specify in their documentation any restrictions on what elements may be
     * added. This implementation always throws an <tt>UnsupportedOperationException</tt>.
     *
     * @param aObject element whose presence in this collection is to be ensured.
     * @return <tt>true</tt> if the collection changed as a result of the call.
     * @throws UnsupportedOperationException if the <tt>add</tt> method is not supported by this collection.
     * @throws NullPointerException if this collection does not permit <tt>null</tt> elements, and the specified
     *         element is <tt>null</tt>.
     * @throws ClassCastException if the class of the specified element prevents it from being added to this
     *         collection.
     * @throws IllegalArgumentException if some aspect of this element prevents it from being added to this
     *         collection.
     */
    @Override
    public boolean add(final T aObject) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns <tt>true</tt> if this collection contains no elements. This implementation returns
     * <tt>size() == 0</tt>.
     *
     * @return <tt>true</tt> if this collection contains no elements.
     */
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Deallocate members of the collection.
     */
    public void deallocateMembers() {
        final Iterator<T> i = iterator();

        while (i.hasNext()) {
            myStorage.deallocate(i.next());
        }

        clear();
    }

}
