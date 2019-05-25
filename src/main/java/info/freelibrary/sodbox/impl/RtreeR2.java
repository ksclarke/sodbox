
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
import info.freelibrary.sodbox.RectangleR2;
import info.freelibrary.sodbox.SpatialIndexR2;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

public class RtreeR2<T> extends PersistentCollection<T> implements SpatialIndexR2<T> {

    private int myHeight;

    private int myCount;

    private RtreeR2Page myRoot;

    private transient int myUpdateCounter;

    RtreeR2() {
    }

    RtreeR2(final Storage aStorage) {
        super(aStorage);
    }

    @Override
    public void put(final RectangleR2 aRect, final T aObj) {
        final Storage db = getStorage();

        if (myRoot == null) {
            myRoot = new RtreeR2Page(db, aObj, aRect);
            myHeight = 1;
        } else {
            final RtreeR2Page page = myRoot.insert(db, aRect, aObj, myHeight);

            if (page != null) {
                myRoot = new RtreeR2Page(db, myRoot, page);
                myHeight += 1;
            }
        }

        myCount += 1;
        myUpdateCounter += 1;

        modify();
    }

    @Override
    public int size() {
        return myCount;
    }

    @Override
    public void remove(final RectangleR2 aRect, final T aObj) {
        if (myRoot == null) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }

        final ArrayList reinsertList = new ArrayList();
        int reinsertLevel = myRoot.remove(aRect, aObj, myHeight, reinsertList);

        if (reinsertLevel < 0) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }

        for (int index1 = reinsertList.size(); --index1 >= 0;) {
            final RtreeR2Page page1 = (RtreeR2Page) reinsertList.get(index1);

            for (int index2 = 0, n = page1.myCount; index2 < n; index2++) {
                final RtreeR2Page page2 = myRoot.insert(getStorage(), page1.myRectR2[index2], page1.myBranch.get(
                        index2), myHeight - reinsertLevel);

                if (page2 != null) {
                    // root split
                    myRoot = new RtreeR2Page(getStorage(), myRoot, page2);
                    myHeight += 1;
                }
            }

            reinsertLevel -= 1;
            page1.deallocate();
        }

        if (myRoot.myCount == 1 && myHeight > 1) {
            final RtreeR2Page newRoot = (RtreeR2Page) myRoot.myBranch.get(0);

            myRoot.deallocate();
            myRoot = newRoot;
            myHeight -= 1;
        }

        myCount -= 1;
        myUpdateCounter += 1;

        modify();
    }

    @Override
    public Object[] get(final RectangleR2 aRect) {
        final ArrayList result = new ArrayList();

        if (myRoot != null) {
            myRoot.find(aRect, result, myHeight);
        }

        return result.toArray();
    }

    @Override
    public ArrayList<T> getList(final RectangleR2 aRect) {
        final ArrayList<T> result = new ArrayList<>();

        if (myRoot != null) {
            myRoot.find(aRect, result, myHeight);
        }

        return result;
    }

    @Override
    public RectangleR2 getWrappingRectangle() {
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
        myUpdateCounter += 1;

        modify();
    }

    @Override
    public void deallocate() {
        clear();
        super.deallocate();
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
    public Iterator<T> iterator() {
        return iterator(getWrappingRectangle());
    }

    @Override
    public IterableIterator<Map.Entry<RectangleR2, T>> entryIterator() {
        return entryIterator(getWrappingRectangle());
    }

    @Override
    public IterableIterator<T> iterator(final RectangleR2 aRect) {
        return new RtreeIterator<>(aRect);
    }

    @Override
    public IterableIterator<Map.Entry<RectangleR2, T>> entryIterator(final RectangleR2 aRect) {
        return new RtreeEntryIterator(aRect);
    }

    @Override
    public IterableIterator<T> neighborIterator(final double aX, final double aY) {
        return new NeighborIterator(aX, aY);
    }

    class RtreeIterator<E> extends IterableIterator<E> implements PersistentIterator {

        RtreeR2Page[] myPageStack;

        int[] myPosStack;

        int myCounter;

        RectangleR2 myRect;

        RtreeIterator(final RectangleR2 aRect) {
            myCounter = myUpdateCounter;

            if (myHeight == 0) {
                return;
            }

            myRect = aRect;
            myPageStack = new RtreeR2Page[myHeight];
            myPosStack = new int[myHeight];

            if (!gotoFirstItem(0, myRoot)) {
                myPageStack = null;
                myPosStack = null;
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
            return myPageStack[aStackPosition].myBranch.get(myPosStack[aStackPosition]);
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final E current = (E) current(myHeight - 1);

            if (!gotoNextItem(myHeight - 1)) {
                myPageStack = null;
                myPosStack = null;
            }

            return current;
        }

        @Override
        public int nextOID() {
            if (!hasNext()) {
                return 0;
            }

            final int oid = getStorage().getOid(myPageStack[myHeight - 1].myBranch.getRaw(myPosStack[myHeight - 1]));

            if (!gotoNextItem(myHeight - 1)) {
                myPageStack = null;
                myPosStack = null;
            }

            return oid;
        }

        private boolean gotoFirstItem(final int aStackPage, final RtreeR2Page aPage) {
            for (int index = 0, n = aPage.myCount; index < n; index++) {
                if (myRect.intersects(aPage.myRectR2[index])) {
                    if (aStackPage + 1 == myHeight || gotoFirstItem(aStackPage + 1, (RtreeR2Page) aPage.myBranch.get(
                            index))) {
                        myPageStack[aStackPage] = aPage;
                        myPosStack[aStackPage] = index;

                        return true;
                    }
                }
            }

            return false;
        }

        private boolean gotoNextItem(final int aStackPage) {
            final RtreeR2Page page = myPageStack[aStackPage];

            for (int index = myPosStack[aStackPage], n = page.myCount; ++index < n;) {
                if (myRect.intersects(page.myRectR2[index])) {
                    if (aStackPage + 1 == myHeight || gotoFirstItem(aStackPage + 1, (RtreeR2Page) page.myBranch.get(
                            index))) {
                        myPageStack[aStackPage] = page;
                        myPosStack[aStackPage] = index;

                        return true;
                    }
                }
            }

            myPageStack[aStackPage] = null;

            return (aStackPage > 0) ? gotoNextItem(aStackPage - 1) : false;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    static class RtreeEntry<T> implements Map.Entry<RectangleR2, T> {

        RtreeR2Page myPage;

        int myPos;

        RtreeEntry(final RtreeR2Page aPage, final int aPos) {
            myPage = aPage;
            myPos = aPos;
        }

        @Override
        public RectangleR2 getKey() {
            return myPage.myRectR2[myPos];
        }

        @Override
        public T getValue() {
            return (T) myPage.myBranch.get(myPos);
        }

        @Override
        public T setValue(final T aValue) {
            throw new UnsupportedOperationException();
        }

    }

    class RtreeEntryIterator extends RtreeIterator<Map.Entry<RectangleR2, T>> {

        RtreeEntryIterator(final RectangleR2 aRect) {
            super(aRect);
        }

        @Override
        protected Object current(final int aStackPos) {
            return new RtreeEntry(myPageStack[aStackPos], myPosStack[aStackPos]);
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

        double myX;

        double myY;

        NeighborIterator(final double aX, final double aY) {
            myX = aX;
            myY = aY;
            myCounter = myUpdateCounter;

            if (myHeight == 0) {
                return;
            }

            myList = new Neighbor(myRoot, myRoot.cover().distance(aX, aY), myHeight);
        }

        void insert(final Neighbor aNode) {
            final double distance = aNode.myDistance;

            Neighbor prev = null;
            Neighbor next = myList;

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

                final RtreeR2Page page = (RtreeR2Page) neighbor.myChild;

                myList = neighbor.myNext;

                for (int index = 0, count = page.myCount; index < count; index++) {
                    insert(new Neighbor(page.myBranch.get(index), page.myRectR2[index].distance(myX, myY),
                            neighbor.myLevel - 1));
                }
            }
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final Neighbor neighbor = myList;

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
