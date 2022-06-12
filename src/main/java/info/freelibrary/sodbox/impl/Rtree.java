
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.Rectangle;
import info.freelibrary.sodbox.SpatialIndex;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

public class Rtree<T> extends PersistentCollection<T> implements SpatialIndex<T> {

    private int myHeight;

    private int myCount;

    private RtreePage myRoot;

    private transient int myUpdateCounter;

    Rtree() {
    }

    @Override
    public void put(final Rectangle aRect, final T aObj) {
        final Storage storage = getStorage();

        if (myRoot == null) {
            myRoot = new RtreePage(storage, aObj, aRect);
            myHeight = 1;
        } else {
            final RtreePage page = myRoot.insert(storage, aRect, aObj, myHeight);

            if (page != null) {
                myRoot = new RtreePage(storage, myRoot, page);
                myHeight += 1;
            }
        }

        myUpdateCounter += 1;
        myCount += 1;

        modify();
    }

    @Override
    public int size() {
        return myCount;
    }

    @Override
    public void remove(final Rectangle aRect, final T aObj) {
        if (myRoot == null) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }

        final ArrayList reinsertList = new ArrayList();

        int reinsertLevel = myRoot.remove(aRect, aObj, myHeight, reinsertList);

        if (reinsertLevel < 0) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }

        for (int index = reinsertList.size(); --index >= 0;) {
            final RtreePage page = (RtreePage) reinsertList.get(index);

            for (int jndex = 0, n = page.myIndex; jndex < n; jndex++) {
                final RtreePage splitPage = myRoot.insert(getStorage(), page.myRectangle[jndex], page.myBranch.get(
                        jndex), myHeight - reinsertLevel);

                // Root split
                if (splitPage != null) {
                    myRoot = new RtreePage(getStorage(), myRoot, splitPage);
                    myHeight += 1;
                }
            }

            reinsertLevel -= 1;
            page.deallocate();
        }

        if (myRoot.myIndex == 1 && myHeight > 1) {
            final RtreePage newRoot = (RtreePage) myRoot.myBranch.get(0);

            myRoot.deallocate();
            myRoot = newRoot;
            myHeight -= 1;
        }

        myCount -= 1;
        myUpdateCounter += 1;

        modify();
    }

    @Override
    public Object[] get(final Rectangle aRect) {
        return getList(aRect).toArray();
    }

    @Override
    public ArrayList<T> getList(final Rectangle aRect) {
        final ArrayList<T> result = new ArrayList<>();

        if (myRoot != null) {
            myRoot.find(aRect, result, myHeight);
        }

        return result;
    }

    @Override
    public Object[] toArray() {
        return get(getWrappingRectangle());
    }

    @Override
    public <E> E[] toArray(final E[] aArray) {
        return getList(getWrappingRectangle()).toArray(aArray);
    }

    @Override
    public Rectangle getWrappingRectangle() {
        if (myRoot != null) {
            return myRoot.cover();
        }

        return null;
    }

    @Override
    public void clear() {
        if (myRoot != null) {
            myRoot.purge(myHeight);
            myRoot = null;
        }

        myHeight = 0;
        myCount = 0;

        modify();
    }

    @Override
    public void deallocate() {
        clear();
        super.deallocate();
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(getWrappingRectangle());
    }

    @Override
    public IterableIterator<Map.Entry<Rectangle, T>> entryIterator() {
        return entryIterator(getWrappingRectangle());
    }

    @Override
    public IterableIterator<T> iterator(final Rectangle aRect) {
        return new RtreeIterator<>(aRect);
    }

    @Override
    public IterableIterator<Map.Entry<Rectangle, T>> entryIterator(final Rectangle aRect) {
        return new RtreeEntryIterator(aRect);
    }

    @Override
    public IterableIterator<T> neighborIterator(final int aX, final int aY) {
        return new NeighborIterator(aX, aY);
    }

    class RtreeIterator<E> extends IterableIterator<E> implements PersistentIterator {

        RtreePage[] myPageStack;

        int[] myPositionStack;

        int myCounter;

        Rectangle myRect;

        RtreeIterator(final Rectangle aRect) {
            myCounter = myUpdateCounter;

            if (myHeight == 0) {
                return;
            }

            myRect = aRect;
            myPageStack = new RtreePage[myHeight];
            myPositionStack = new int[myHeight];

            if (!gotoFirstItem(0, myRoot)) {
                myPageStack = null;
                myPositionStack = null;
            }
        }

        @Override
        public boolean hasNext() {
            if (myCounter != myUpdateCounter) {
                throw new ConcurrentModificationException();
            }

            return myPageStack != null;
        }

        protected Object current(final int aStackPosition) {
            return myPageStack[aStackPosition].myBranch.get(myPositionStack[aStackPosition]);
        }

        @Override
        public E next() {
            final E current;

            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            current = (E) current(myHeight - 1);

            if (!gotoNextItem(myHeight - 1)) {
                myPageStack = null;
                myPositionStack = null;
            }

            return current;
        }

        @Override
        public int nextOID() {
            if (!hasNext()) {
                return 0;
            }

            final int oid = getStorage().getOid(myPageStack[myHeight - 1].myBranch.getRaw(myPositionStack[myHeight -
                    1]));

            if (!gotoNextItem(myHeight - 1)) {
                myPageStack = null;
                myPositionStack = null;
            }

            return oid;
        }

        private boolean gotoFirstItem(final int aStackPosition, final RtreePage aPage) {
            for (int index = 0, n = aPage.myIndex; index < n; index++) {
                if (myRect.intersects(aPage.myRectangle[index])) {
                    if (aStackPosition + 1 == myHeight || gotoFirstItem(aStackPosition + 1, (RtreePage) aPage.myBranch
                            .get(index))) {
                        myPageStack[aStackPosition] = aPage;
                        myPositionStack[aStackPosition] = index;

                        return true;
                    }
                }
            }

            return false;
        }

        private boolean gotoNextItem(final int aStackPosition) {
            final RtreePage page = myPageStack[aStackPosition];

            for (int index = myPositionStack[aStackPosition], n = page.myIndex; ++index < n;) {
                if (myRect.intersects(page.myRectangle[index])) {
                    if (aStackPosition + 1 == myHeight || gotoFirstItem(aStackPosition + 1, (RtreePage) page.myBranch
                            .get(index))) {
                        myPageStack[aStackPosition] = page;
                        myPositionStack[aStackPosition] = index;

                        return true;
                    }
                }
            }

            myPageStack[aStackPosition] = null;

            return aStackPosition > 0 ? gotoNextItem(aStackPosition - 1) : false;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static class RtreeEntry<T> implements Map.Entry<Rectangle, T> {

        RtreePage myPage;

        int myPosition;

        RtreeEntry(final RtreePage aPage, final int aPosition) {
            myPage = aPage;
            myPosition = aPosition;
        }

        @Override
        public Rectangle getKey() {
            return myPage.myRectangle[myPosition];
        }

        @Override
        public T getValue() {
            return (T) myPage.myBranch.get(myPosition);
        }

        @Override
        public T setValue(final T aValue) {
            throw new UnsupportedOperationException();
        }
    }

    class RtreeEntryIterator extends RtreeIterator<Map.Entry<Rectangle, T>> {

        RtreeEntryIterator(final Rectangle aRect) {
            super(aRect);
        }

        @Override
        protected Object current(final int aStackPosition) {
            return new RtreeEntry(myPageStack[aStackPosition], myPositionStack[aStackPosition]);
        }
    }

    static class Neighbor {

        Object myChild;

        Neighbor myNext;

        int myLevel;

        double myDistance;

        Neighbor(final Object aChild, final double aDistance, final int aLevel) {
            myChild = aChild;
            myDistance = aDistance;
            myLevel = aLevel;
        }
    }

    class NeighborIterator<E> extends IterableIterator<E> implements PersistentIterator {

        Neighbor myList;

        int myCounter;

        int myX;

        int myY;

        NeighborIterator(final int aX, final int aY) {
            myX = aX;
            myY = aY;

            myCounter = myUpdateCounter;

            if (myHeight == 0) {
                return;
            }

            myList = new Neighbor(myRoot, myRoot.cover().distance(aX, aY), myHeight);
        }

        void insert(final Neighbor aNode) {
            Neighbor prev = null;
            Neighbor next = myList;

            final double distance = aNode.myDistance;

            while (next != null && next.myDistance < distance) {
                prev = next;
                next = prev.myNext;
            }

            aNode.myNext = next;

            if (prev == null) {
                myList = aNode;
            } else {
                prev.myNext = aNode;
            }
        }

        @Override
        public boolean hasNext() {
            if (myCounter != myUpdateCounter) {
                throw new ConcurrentModificationException();
            }

            while (true) {
                final Neighbor neighbor = myList;

                if (neighbor == null) {
                    return false;
                }

                if (neighbor.myLevel == 0) {
                    return true;
                }

                myList = neighbor.myNext;

                final RtreePage page = (RtreePage) neighbor.myChild;

                for (int index = 0, n = page.myIndex; index < n; index++) {
                    insert(new Neighbor(page.myBranch.get(index), page.myRectangle[index].distance(myX, myY),
                            neighbor.myLevel - 1));
                }
            }
        }

        @Override
        public E next() {
            final Neighbor neighbor;

            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            neighbor = myList;
            myList = neighbor.myNext;

            Assert.that(neighbor.myLevel == 0);

            return (E) neighbor.myChild;
        }

        @Override
        public int nextOID() {
            return getStorage().getOid(next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
