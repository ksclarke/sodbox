
package info.freelibrary.sodbox;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Double linked list.
 */
public class L2List extends L2ListElem {

    class L2ListIterator implements PersistentIterator, Iterator<L2ListElem> {

        private L2ListElem curr;

        private int counter;

        L2ListIterator() {
            curr = L2List.this;
            counter = updateCounter;
        }

        @Override
        public boolean hasNext() {
            if (counter != updateCounter) {
                throw new IllegalStateException();
            }

            return curr.next != L2List.this;
        }

        @Override
        public L2ListElem next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            curr = curr.next;

            return curr;
        }

        @Override
        public int nextOid() {
            if (!hasNext()) {
                return 0;
            }

            curr = curr.next;

            return curr.getOid();
        }

        @Override
        public void remove() {
            if (counter != updateCounter || curr == L2List.this) {
                throw new IllegalStateException();
            }

            L2List.this.remove(curr);

            counter = updateCounter;
            curr = curr.prev;
        }
    }

    private int nElems;

    private int updateCounter;

    /**
     * Add object to the list.
     *
     * @param obj object added to the list
     * @return always returns <code>true</code>
     */
    public synchronized boolean add(final Object obj) {
        append((L2ListElem) obj);
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
     * @param c collection whose elements are to be added to this collection.
     * @return <tt>true</tt> if this collection changed as a result of the call.
     * @throws UnsupportedOperationException if this collection does not support the <tt>addAll</tt> method.
     * @throws NullPointerException if the specified collection is null.
     * @see #add(Object)
     */
    public synchronized boolean addAll(final Collection<?> c) {
        final Iterator<?> e = c.iterator();

        while (e.hasNext()) {
            add(e.next());
        }

        return true;
    }

    /**
     * Insert element at the end of the list.
     */
    public synchronized void append(final L2ListElem elem) {
        modify();
        prev.modify();
        elem.modify();
        elem.next = this;
        elem.prev = prev;
        prev.next = elem;
        prev = elem;
        nElems += 1;
        updateCounter += 1;
    }

    /**
     * Make list empty.
     */
    public synchronized void clear() {
        modify();
        next = prev = this;
        nElems = 0;
        updateCounter += 1;
    }

    /**
     * Check if object is in collection.
     *
     * @param o object to be searched in the collection
     * @return <code>true</code> if there is an object in the collection which is equals to the specified object
     */
    public synchronized boolean contains(final Object o) {
        for (L2ListElem e = next; e != this; e = e.next) {
            if (e.equals(o)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns <tt>true</tt> if this collection contains all of the elements in the specified collection. This
     * implementation iterates over the specified collection, checking each element returned by the iterator in turn
     * to see if it's contained in this collection. If all elements are so contained <tt>true</tt> is returned,
     * otherwise <tt>false</tt>.
     *
     * @param c collection to be checked for containment in this collection.
     * @return <tt>true</tt> if this collection contains all of the elements in the specified collection.
     * @throws NullPointerException if the specified collection is null.
     * @see #contains(Object)
     */
    public synchronized boolean containsAll(final Collection<?> c) {
        final Iterator<?> e = c.iterator();

        while (e.hasNext()) {
            if (!contains(e.next())) {
                return false;
            }
        }

        return true;
    }

    public void deallocateMembers() {
        final Iterator<?> i = iterator();

        while (i.hasNext()) {
            ((IPersistent) i.next()).deallocate();
        }

        clear();
    }

    /**
     * Get list head element.
     *
     * @return list head element or null if list is empty
     */
    public synchronized L2ListElem head() {
        return next != this ? next : null;
    }

    /**
     * Check if list is empty.
     *
     * @return <code>true</code> if list is empty
     */
    public synchronized boolean isEmpty() {
        return next == this;
    }

    /**
     * Get list iterator. This iterator supports remove() method put concurrent modifications of the list during
     * iteration are not possible.
     *
     * @return list iterator
     */
    public synchronized Iterator<?> iterator() {
        return new L2ListIterator();
    }

    /**
     * Insert element at the beginning of the list.
     */
    public synchronized void prepend(final L2ListElem elem) {
        modify();
        next.modify();
        elem.modify();
        elem.next = next;
        elem.prev = this;
        next.prev = elem;
        next = elem;
        nElems += 1;
        updateCounter += 1;
    }

    /**
     * Remove element from the list.
     */
    public synchronized void remove(final L2ListElem elem) {
        modify();
        elem.prev.modify();
        elem.next.modify();
        elem.next.prev = elem.prev;
        elem.prev.next = elem.next;
        nElems -= 1;
        updateCounter += 1;
    }

    /**
     * Remove object from the list.
     *
     * @param o object to be removed from the list
     * @return always returns <code>true</code>
     */
    public synchronized boolean remove(final Object o) {
        remove((L2ListElem) o);
        return true;
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
     * @param c elements to be removed from this collection.
     * @return <tt>true</tt> if this collection changed as a result of the call.
     * @throws UnsupportedOperationException if the <tt>removeAll</tt> method is not supported by this collection.
     * @throws NullPointerException if the specified collection is null.
     * @see #remove(Object)
     * @see #contains(Object)
     */
    public synchronized boolean removeAll(final Collection<?> c) {
        boolean modified = false;
        final Iterator<?> e = iterator();

        while (e.hasNext()) {
            if (c.contains(e.next())) {
                e.remove();
                modified = true;
            }
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
     * @param c elements to be retained in this collection.
     * @return <tt>true</tt> if this collection changed as a result of the call.
     * @throws UnsupportedOperationException if the <tt>retainAll</tt> method is not supported by this Collection.
     * @throws NullPointerException if the specified collection is null.
     * @see #remove(Object)
     * @see #contains(Object)
     */
    public synchronized boolean retainAll(final Collection<?> c) {
        boolean modified = false;
        final Iterator<?> e = iterator();

        while (e.hasNext()) {
            if (!c.contains(e.next())) {
                e.remove();
                modified = true;
            }
        }

        return modified;
    }

    /**
     * Get size of the list.
     *
     * @return number of elements in the list
     */
    public int size() {
        return nElems;
    }

    /**
     * Get list tail element.
     *
     * @return list tail element or null if list is empty
     */
    public synchronized L2ListElem tail() {
        return prev != this ? prev : null;
    }

    /**
     * Get array of the list elements.
     *
     * @return array with list elements
     */
    public synchronized Object[] toArray() {
        final L2ListElem[] arr = new L2ListElem[nElems];
        L2ListElem e = this;

        for (int i = 0; i < arr.length; i++) {
            arr[i] = e = e.next;
        }

        return arr;
    }

    /**
     * Returns an array containing all of the elements in this list in the correct order; the runtime type of the
     * returned array is that of the specified array. If the list fits in the specified array, it is returned therein.
     * Otherwise, a new array is allocated with the runtime type of the specified array and the size of this list. If
     * the list fits in the specified array with room to spare (i.e., the array has more elements than the list), the
     * element in the array immediately following the end of the collection is set to <tt>null</tt>. This is useful in
     * determining the length of the list <i>only</i> if the caller knows that the list does not contain any
     * <tt>null</tt> elements.
     *
     * @param a the array into which the elements of the list are to be stored, if it is big enough; otherwise, a new
     *        array of the same runtime type is allocated for this purpose.
     * @return an array containing the elements of the list.
     * @throws ArrayStoreException if the runtime type of a is not a super type of the runtime type of every element
     *         in this list.
     */
    public synchronized Object[] toArray(Object a[]) {
        final int size = nElems;

        if (a.length < size) {
            a = (Object[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
        }

        L2ListElem e = this;

        for (int i = 0; i < size; i++) {
            a[i] = e = e.next;
        }

        if (a.length > size) {
            a[size] = null;
        }

        return a;
    }

}
