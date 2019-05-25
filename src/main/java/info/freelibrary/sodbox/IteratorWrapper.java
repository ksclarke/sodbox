
package info.freelibrary.sodbox;

import java.util.Iterator;

public class IteratorWrapper<T> extends IterableIterator<T> {

    private final Iterator<T> myIterator;

    /**
     * Creates an iterator wrapper from the supplied iterator.
     *
     * @param aIterator An iterator to wrap
     */
    public IteratorWrapper(final Iterator<T> aIterator) {
        myIterator = aIterator;
    }

    @Override
    public boolean hasNext() {
        return myIterator.hasNext();
    }

    @Override
    public T next() {
        return myIterator.next();
    }

    @Override
    public void remove() {
        myIterator.remove();
    }

}
