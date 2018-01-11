
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

    private T next;

    public ThreadSafeIterator(final IResource collection, final Iterator<T> iterator) {
        myCollection = collection;
        myIterator = iterator;
    }

    @Override
    public boolean hasNext() {
        boolean result;

        if (next == null) {
            myCollection.sharedLock();

            if (myIterator.hasNext()) {
                next = myIterator.next();
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
        T obj = next;

        if (obj == null) {
            myCollection.sharedLock();
            obj = myIterator.next();
            myCollection.unlock();
        } else {
            next = null;
        }

        return obj;
    }

    @Override
    public void remove() {
        myCollection.exclusiveLock();
        myIterator.remove();
        myCollection.unlock();
        next = null;
    }

}
