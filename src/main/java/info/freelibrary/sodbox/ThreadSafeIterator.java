
package info.freelibrary.sodbox;

import java.util.Iterator;

/**
 * This class is used to make it possible to iterate collection without locking it and when connection can be
 * currently updated by other threads. Please notice that if other threads are deleting elements from the tree, then
 * deleting current element of the iterator can still cause the problem. Also this iterator should be used only inside
 * one thread - sharing iterator between multiple threads will not work correctly.
 */
public class ThreadSafeIterator<T> extends IterableIterator<T> {

    private final IResource myCollection;

    private final Iterator<T> myIterator;

    private T myNext;

    /**
     * Creates a thread safe iterator.
     *
     * @param aCollection A collection over which to iterate
     * @param aIterator An iterator for the collection
     */
    public ThreadSafeIterator(final IResource aCollection, final Iterator<T> aIterator) {
        myCollection = aCollection;
        myIterator = aIterator;
    }

    @Override
    public boolean hasNext() {
        final boolean result;

        if (myNext == null) {
            myCollection.sharedLock();

            if (myIterator.hasNext()) {
                myNext = myIterator.next();
                result = true;
            } else {
                result = false;
            }

            myCollection.unlock();
        } else {
            result = true;
        }

        return result;
    }

    @Override
    public T next() {
        T obj = myNext;

        if (obj == null) {
            myCollection.sharedLock();
            obj = myIterator.next();
            myCollection.unlock();
        } else {
            myNext = null;
        }

        return obj;
    }

    @Override
    public void remove() {
        myCollection.exclusiveLock();
        myIterator.remove();
        myCollection.unlock();
        myNext = null;
    }

}
