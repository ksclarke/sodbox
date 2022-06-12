
package info.freelibrary.sodbox;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Interface combining both Iterable and Iterator functionality.
 */
public abstract class IterableIterator<T> implements Iterable<T>, Iterator<T> {

    /**
     * This class itself is iterator.
     */
    @Override
    public Iterator<T> iterator() {
        return this;
    }

    /**
     * Get first selected object. This method can be used when single selected object is needed. Please notice that
     * this method doesn't check if selection contain more than one object.
     *
     * @return first selected object or null if selection is empty
     */
    public T first() {
        return hasNext() ? next() : null;
    }

    /**
     * Get number of selected objects.
     *
     * @return selection size
     */
    public int size() {
        int count = 0;

        for (final Iterator<T> iterator = this.iterator(); iterator.hasNext();) {
            iterator.next();
            count += 1;
        }

        return count;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Convert selection to array list.
     *
     * @return array list with the selected objects
     */
    public ArrayList<T> toList() {
        final ArrayList<T> list = new ArrayList<>();
        for (final T obj : this) {
            list.add(obj);
        }
        return list;
    }

}
