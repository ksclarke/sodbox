
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.IPersistentList;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.Storage;

class PersistentListImpl<E> extends PersistentCollection<E> implements IPersistentList<E> {

    static final int LEAF_PAGE_ITEM_COUNT = (Page.PAGE_SIZE - ObjectHeader.SIZE_OF - 8) / 4;

    static final int INTERMEDIATE_PAGE_ITEM_COUNT = (Page.PAGE_SIZE - ObjectHeader.SIZE_OF - 12) / 8;

    int myElementCount;

    ListPage myRoot;

    transient int myModCount;

    PersistentListImpl() {
    }

    PersistentListImpl(final Storage aStorage) {
        super(aStorage);
        myRoot = new ListPage(aStorage);
    }

    @Override
    public List<E> subList(final int aFromIndex, final int aToIndex) {
        return new SubList<>(this, aFromIndex, aToIndex);
    }

    protected void removeRange(final int aFromIndex, final int aToIndex) {
        int toIndex = aToIndex;

        while (aFromIndex < toIndex) {
            remove(aFromIndex);
            toIndex -= 1;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public E get(final int aIndex) {
        if (aIndex < 0 || aIndex >= myElementCount) {
            throw new IndexOutOfBoundsException("index= " + aIndex + ", size= " + myElementCount);
        }

        return (E) myRoot.get(aIndex);
    }

    @SuppressWarnings("unchecked")
    E getPosition(final TreePosition aPosition, final int aIndex) {
        if (aIndex < 0 || aIndex >= myElementCount) {
            throw new IndexOutOfBoundsException("index = " + aIndex + ", size = " + myElementCount);
        }

        if (aPosition.myPage != null && aIndex >= aPosition.myIndex && aIndex < aPosition.myIndex +
                aPosition.myPage.myItemCount) {
            return (E) aPosition.myPage.myItems.get(aIndex - aPosition.myIndex);
        }

        aPosition.myIndex = aIndex;

        return (E) myRoot.getPosition(aPosition, aIndex);
    }

    Object getRawPosition(final TreePosition aPosition, final int aIndex) {
        if (aIndex < 0 || aIndex >= myElementCount) {
            throw new IndexOutOfBoundsException(" index=" + aIndex + ",  size=" + myElementCount);
        }

        if (aPosition.myPage != null && aIndex >= aPosition.myIndex && aIndex < aPosition.myIndex +
                aPosition.myPage.myItemCount) {
            return aPosition.myPage.myItems.getRaw(aIndex - aPosition.myIndex);
        }

        aPosition.myIndex = aIndex;

        return myRoot.getRawPosition(aPosition, aIndex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public E set(final int aIndex, final E aObject) {
        if (aIndex < 0 || aIndex >= myElementCount) {
            throw new IndexOutOfBoundsException("index =" + aIndex + ", size =" + myElementCount);
        }

        return (E) myRoot.set(aIndex, aObject);
    }

    @Override
    public Object[] toArray() {
        final int total = myElementCount;
        final Object[] array = new Object[total];
        final Iterator<E> iterator = listIterator(0);

        for (int index = 0; index < total; index++) {
            array[index] = iterator.next();
        }

        return array;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T[] toArray(final T[] aArray) {
        final int total = myElementCount;
        final T[] array;

        if (aArray.length < total) {
            array = (T[]) Array.newInstance(aArray.getClass().getComponentType(), total);
        } else {
            array = aArray;
        }

        final Iterator<E> iterator = listIterator(0);

        for (int index = 0; index < total; index++) {
            array[index] = (T) iterator.next();
        }

        return array;
    }

    @Override
    public boolean isEmpty() {
        return myElementCount == 0;
    }

    @Override
    public int size() {
        return myElementCount;
    }

    @Override
    public boolean contains(final Object aObject) {
        final Iterator iterator = iterator();

        if (aObject == null) {
            while (iterator.hasNext()) {
                if (iterator.next() == null) {
                    return true;
                }
            }
        } else {
            while (iterator.hasNext()) {
                if (aObject.equals(iterator.next())) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public boolean add(final E aObject) {
        add(myElementCount, aObject);
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(final int aIndex, final E aObject) {
        if (aIndex < 0 || aIndex > myElementCount) {
            throw new IndexOutOfBoundsException(" index= " + aIndex + ",  size= " + myElementCount);
        }

        final ListPage overflow = myRoot.add(aIndex, aObject);

        if (overflow != null) {
            final ListIntermediatePage page = new ListIntermediatePage(getStorage());

            page.setItem(0, overflow);
            page.myItems.setObject(1, myRoot);
            page.myChildCount[1] = Integer.MAX_VALUE;
            page.myItemCount = 2;

            myRoot = page;
        }

        myElementCount += 1;
        myModCount += 1;
        modify();
    }

    @SuppressWarnings("unchecked")
    @Override
    public E remove(final int aIndex) {
        if (aIndex < 0 || aIndex >= myElementCount) {
            throw new IndexOutOfBoundsException("index=" + aIndex + ", size=" + myElementCount);
        }

        final E obj = (E) myRoot.remove(aIndex);

        if (myRoot.myItemCount == 1 && myRoot instanceof ListIntermediatePage) {
            final ListPage newRoot = (ListPage) myRoot.myItems.get(0);

            myRoot.deallocate();
            myRoot = newRoot;
        }

        myElementCount -= 1;
        myModCount += 1;
        modify();

        return obj;
    }

    @Override
    public void clear() {
        myModCount += 1;
        myRoot.prune();
        myRoot = new ListPage(getStorage());
        myElementCount = 0;
        modify();
    }

    @Override
    public int indexOf(final Object aObject) {
        final ListIterator<E> iterator = listIterator();

        if (aObject == null) {
            while (iterator.hasNext()) {
                if (iterator.next() == null) {
                    return iterator.previousIndex();
                }
            }
        } else {
            while (iterator.hasNext()) {
                if (aObject.equals(iterator.next())) {
                    return iterator.previousIndex();
                }
            }
        }

        return -1;
    }

    @Override
    public int lastIndexOf(final Object aObject) {
        final ListIterator<E> iterator = listIterator(size());

        if (aObject == null) {
            while (iterator.hasPrevious()) {
                if (iterator.previous() == null) {
                    return iterator.nextIndex();
                }
            }
        } else {
            while (iterator.hasPrevious()) {
                if (aObject.equals(iterator.previous())) {
                    return iterator.nextIndex();
                }
            }
        }

        return -1;
    }

    @Override
    public boolean addAll(final int aIndex, final Collection<? extends E> aCollection) {
        final Iterator<? extends E> iterator = aCollection.iterator();

        boolean modified = false;
        int index = aIndex;

        while (iterator.hasNext()) {
            add(index++, iterator.next());
            modified = true;
        }

        return modified;
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    @Override
    public ListIterator<E> listIterator() {
        return listIterator(0);
    }

    @Override
    public ListIterator<E> listIterator(final int aIndex) {
        if (aIndex < 0 || aIndex > size()) {
            throw new IndexOutOfBoundsException(" Index: " + aIndex);
        }

        return new ListItr(aIndex);
    }

    static class TreePosition {

        /**
         * B-Tree page where element is located
         */
        ListPage myPage;

        /**
         * Index of first element at the page
         */
        int myIndex;
    }

    static class ListPage extends Persistent {

        int myItemCount;

        Link myItems;

        ListPage() {
        }

        ListPage(final Storage aStorage) {
            super(aStorage);

            final int max = getMaxItems();

            myItems = aStorage.createLink(max);
            myItems.setSize(max);
        }

        Object get(final int aIndex) {
            return myItems.get(aIndex);
        }

        Object getPosition(final TreePosition aPosition, final int aIndex) {
            aPosition.myPage = this;
            aPosition.myIndex -= aIndex;

            return myItems.get(aIndex);
        }

        Object getRawPosition(final TreePosition aPosition, final int aIndex) {
            aPosition.myPage = this;
            aPosition.myIndex -= aIndex;

            return myItems.getRaw(aIndex);
        }

        @SuppressWarnings("unchecked")
        Object set(final int aIndex, final Object aObject) {
            return myItems.set(aIndex, aObject);
        }

        @SuppressWarnings("unchecked")
        void clear(final int aIndex, final int aLength) {
            int length = aLength;
            int index = aIndex;

            while (--length >= 0) {
                myItems.setObject(index++, null);
            }
        }

        void prune() {
            deallocate();
        }

        void copy(final int aDestOffset, final ListPage aSrcPage, final int aSrcOffset, final int aLength) {
            System.arraycopy(aSrcPage.myItems.toRawArray(), aSrcOffset, myItems.toRawArray(), aDestOffset, aLength);
        }

        int getMaxItems() {
            return LEAF_PAGE_ITEM_COUNT;
        }

        @SuppressWarnings("unchecked")
        void setItem(final int aIndex, final Object aObject) {
            myItems.setObject(aIndex, aObject);
        }

        int size() {
            return myItemCount;
        }

        ListPage clonePage() {
            return new ListPage(getStorage());
        }

        @SuppressWarnings("unchecked")
        Object remove(final int aIndex) {
            final Object obj = myItems.get(aIndex);

            myItemCount -= 1;
            copy(aIndex, this, aIndex + 1, myItemCount - aIndex);
            myItems.setObject(myItemCount, null);
            modify();

            return obj;
        }

        boolean underflow() {
            return myItemCount < getMaxItems() / 3;
        }

        ListPage add(final int aIndex, final Object aObject) {
            final int max = getMaxItems();

            modify();

            if (myItemCount < max) {
                copy(aIndex + 1, this, aIndex, myItemCount - aIndex);
                setItem(aIndex, aObject);
                myItemCount += 1;
                return null;
            } else {
                final ListPage b = clonePage();
                final int m = (max + 1) / 2;

                if (aIndex < m) {
                    b.copy(0, this, 0, aIndex);
                    b.copy(aIndex + 1, this, aIndex, m - aIndex - 1);
                    copy(0, this, m - 1, max - m + 1);
                    b.setItem(aIndex, aObject);
                } else {
                    b.copy(0, this, 0, m);
                    copy(0, this, m, aIndex - m);
                    copy(aIndex - m + 1, this, aIndex, max - aIndex);
                    setItem(aIndex - m, aObject);
                }

                clear(max - m + 1, m - 1);
                myItemCount = max - m + 1;
                b.myItemCount = m;

                return b;
            }
        }
    }

    static class ListIntermediatePage extends ListPage {

        int[] myChildCount;

        ListIntermediatePage() {
        }

        ListIntermediatePage(final Storage aStorage) {
            super(aStorage);
            myChildCount = new int[INTERMEDIATE_PAGE_ITEM_COUNT];
        }

        @Override
        Object getPosition(final TreePosition aPosition, final int aIndex) {
            int index = aIndex;
            int secondaryIndex;

            for (secondaryIndex = 0; index >= myChildCount[secondaryIndex]; secondaryIndex++) {
                index -= myChildCount[secondaryIndex];
            }

            return ((ListPage) myItems.get(secondaryIndex)).getPosition(aPosition, index);
        }

        @Override
        Object getRawPosition(final TreePosition aPosition, final int aIndex) {
            int index = aIndex;
            int secondaryIndex;

            for (secondaryIndex = 0; index >= myChildCount[secondaryIndex]; secondaryIndex++) {
                index -= myChildCount[secondaryIndex];
            }

            return ((ListPage) myItems.get(secondaryIndex)).getRawPosition(aPosition, index);
        }

        @Override
        Object get(final int aIndex) {
            int index = aIndex;
            int secondaryIndex;

            for (secondaryIndex = 0; index >= myChildCount[secondaryIndex]; secondaryIndex++) {
                index -= myChildCount[secondaryIndex];
            }

            return ((ListPage) myItems.get(secondaryIndex)).get(index);
        }

        @Override
        Object set(final int aIndex, final Object aObject) {
            int index = aIndex;
            int secondaryIndex;

            for (secondaryIndex = 0; index >= myChildCount[secondaryIndex]; secondaryIndex++) {
                index -= myChildCount[secondaryIndex];
            }

            return ((ListPage) myItems.get(secondaryIndex)).set(index, aObject);
        }

        @Override
        ListPage add(final int aIndex, final Object aObject) {
            final ListPage page;

            int index = aIndex;
            int secondaryIndex;
            ListPage overflow;

            for (secondaryIndex = 0; index >= myChildCount[secondaryIndex]; secondaryIndex++) {
                index -= myChildCount[secondaryIndex];
            }

            page = (ListPage) myItems.get(secondaryIndex);
            overflow = page.add(index, aObject);

            if (overflow != null) {
                countChildren(secondaryIndex, page);
                overflow = super.add(secondaryIndex, overflow);
            } else {
                modify();

                if (myChildCount[secondaryIndex] != Integer.MAX_VALUE) {
                    myChildCount[secondaryIndex] += 1;
                }
            }

            return overflow;
        }

        @Override
        Object remove(final int aIndex) {
            int index = aIndex;
            int secondaryIndex;

            for (secondaryIndex = 0; index >= myChildCount[secondaryIndex]; secondaryIndex++) {
                index -= myChildCount[secondaryIndex];
            }

            final ListPage page = (ListPage) myItems.get(secondaryIndex);
            final Object obj = page.remove(index);

            modify();

            if (page.underflow()) {
                handlePageUnderflow(page, secondaryIndex);
            } else {
                if (myChildCount[secondaryIndex] != Integer.MAX_VALUE) {
                    myChildCount[secondaryIndex] -= 1;
                }
            }

            return obj;
        }

        void countChildren(final int aIndex, final ListPage aPage) {
            if (myChildCount[aIndex] != Integer.MAX_VALUE) {
                myChildCount[aIndex] = aPage.size();
            }
        }

        @Override
        void prune() {
            for (int index = 0; index < myItemCount; index++) {
                ((ListPage) myItems.get(index)).prune();
            }

            deallocate();
        }

        @SuppressWarnings({ "unchecked" })
        void handlePageUnderflow(final ListPage aFirstPage, final int aPageIndex) {
            final int firstPageCount = aFirstPage.myItemCount;
            final int maxItems = aFirstPage.getMaxItems();

            if (aPageIndex + 1 < myItemCount) { // a greater page exists
                final ListPage secondPage = (ListPage) myItems.get(aPageIndex + 1);
                final int secondPageCount = secondPage.myItemCount;

                Assert.that(secondPageCount >= firstPageCount);

                if (firstPageCount + secondPageCount > maxItems) {
                    // reallocation of nodes between first and second pages
                    final int index = secondPageCount - (firstPageCount + secondPageCount >> 1);

                    secondPage.modify();
                    aFirstPage.copy(firstPageCount, secondPage, 0, index);
                    secondPage.copy(0, secondPage, index, secondPageCount - index);
                    secondPage.clear(secondPageCount - index, index);
                    secondPage.myItemCount -= index;
                    aFirstPage.myItemCount += index;
                    myChildCount[aPageIndex] = aFirstPage.size();
                    countChildren(aPageIndex + 1, secondPage);
                } else { // merge second page to first page
                    aFirstPage.copy(firstPageCount, secondPage, 0, secondPageCount);
                    aFirstPage.myItemCount += secondPageCount;
                    myItemCount -= 1;
                    myChildCount[aPageIndex] = myChildCount[aPageIndex + 1];
                    copy(aPageIndex + 1, this, aPageIndex + 2, myItemCount - aPageIndex - 1);
                    countChildren(aPageIndex, aFirstPage);
                    myItems.set(myItemCount, null);
                    secondPage.deallocate();
                }
            } else { // second page is before first page
                final ListPage secondPage = (ListPage) myItems.get(aPageIndex - 1);
                final int secondPageCount = secondPage.myItemCount;

                Assert.that(secondPageCount >= firstPageCount);

                secondPage.modify();

                if (firstPageCount + secondPageCount > maxItems) {
                    // reallocation of nodes between first and second pages
                    final int i = secondPageCount - (firstPageCount + secondPageCount >> 1);

                    aFirstPage.copy(i, aFirstPage, 0, firstPageCount);
                    aFirstPage.copy(0, secondPage, secondPageCount - i, i);
                    secondPage.clear(secondPageCount - i, i);
                    secondPage.myItemCount -= i;
                    aFirstPage.myItemCount += i;
                    myChildCount[aPageIndex - 1] = secondPage.size();
                    countChildren(aPageIndex, aFirstPage);
                } else { // merge second page to first page
                    secondPage.copy(secondPageCount, aFirstPage, 0, firstPageCount);
                    secondPage.myItemCount += firstPageCount;
                    myItemCount -= 1;
                    myChildCount[aPageIndex - 1] = myChildCount[aPageIndex];
                    countChildren(aPageIndex - 1, secondPage);
                    myItems.set(aPageIndex, null);
                    aFirstPage.deallocate();
                }
            }
        }

        @Override
        void copy(final int aDestOffset, final ListPage aSrcPage, final int aSrcOffset, final int aLength) {
            super.copy(aDestOffset, aSrcPage, aSrcOffset, aLength);

            System.arraycopy(((ListIntermediatePage) aSrcPage).myChildCount, aSrcOffset, myChildCount, aDestOffset,
                    aLength);
        }

        @Override
        int getMaxItems() {
            return INTERMEDIATE_PAGE_ITEM_COUNT;
        }

        @Override
        void setItem(final int aIndex, final Object aObject) {
            super.setItem(aIndex, aObject);
            myChildCount[aIndex] = ((ListPage) aObject).size();
        }

        @Override
        int size() {
            if (myChildCount[myItemCount - 1] == Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            } else {
                int size = 0;

                for (int index = 0; index < myItemCount; index++) {
                    size += myChildCount[index];
                }

                return size;
            }
        }

        @Override
        ListPage clonePage() {
            return new ListIntermediatePage(getStorage());
        }

    }

    private class Itr extends TreePosition implements PersistentIterator, Iterator<E> {

        /**
         * Index of element to be returned by subsequent call to next.
         */
        int myCursor = 0;

        /**
         * Index of element returned by most recent call to next or previous. Reset to -1 if this element is deleted
         * by a call to remove.
         */
        int myLastReturned = -1;

        /**
         * The modCount value that the iterator believes that the backing List should have. If this expectation is
         * violated, the iterator has detected concurrent modification.
         */
        int myExpectedModCount = myModCount;

        @Override
        public boolean hasNext() {
            return myCursor != size();
        }

        @Override
        public int nextOID() {
            checkForComodification();

            if (!hasNext()) {
                return 0;
            }

            final int oid = getStorage().getOid(getRawPosition(this, myCursor));

            myLastReturned = myCursor++;

            return oid;
        }

        @Override
        public E next() {
            checkForComodification();

            try {
                final E next = getPosition(this, myCursor);

                myLastReturned = myCursor++;

                return next;
            } catch (final IndexOutOfBoundsException e) {
                checkForComodification();
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            if (myLastReturned == -1) {
                throw new IllegalStateException();
            }

            checkForComodification();

            try {
                PersistentListImpl.this.remove(myLastReturned);

                if (myLastReturned < myCursor) {
                    myCursor--;
                }

                myPage = null;
                myLastReturned = -1;
                myExpectedModCount = myModCount;
            } catch (final IndexOutOfBoundsException e) {
                throw new ConcurrentModificationException();
            }
        }

        final void checkForComodification() {
            if (myModCount != myExpectedModCount) {
                throw new ConcurrentModificationException();
            }
        }
    }

    private class ListItr extends Itr implements ListIterator<E> {

        ListItr(final int aIndex) {
            myCursor = aIndex;
        }

        @Override
        public boolean hasPrevious() {
            return myCursor != 0;
        }

        @Override
        public E previous() {
            checkForComodification();

            try {
                final int index = myCursor - 1;
                final E previous = getPosition(this, index);

                myLastReturned = myCursor = index;

                return previous;
            } catch (final IndexOutOfBoundsException e) {
                checkForComodification();
                throw new NoSuchElementException();
            }
        }

        @Override
        public int nextIndex() {
            return myCursor;
        }

        @Override
        public int previousIndex() {
            return myCursor - 1;
        }

        @Override
        public void set(final E aObject) {
            if (myLastReturned == -1) {
                throw new IllegalStateException();
            }

            checkForComodification();

            try {
                PersistentListImpl.this.set(myLastReturned, aObject);
                myExpectedModCount = myModCount;
            } catch (final IndexOutOfBoundsException e) {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public void add(final E aObject) {
            checkForComodification();

            try {
                PersistentListImpl.this.add(myCursor++, aObject);
                myLastReturned = -1;
                myPage = null;
                myExpectedModCount = myModCount;
            } catch (final IndexOutOfBoundsException details) {
                throw new ConcurrentModificationException();
            }
        }
    }
}

class SubList<E> extends AbstractList<E> {

    private final PersistentListImpl<E> myList;

    private final int myOffset;

    private int mySize;

    private int myExpectedModCount;

    SubList(final PersistentListImpl<E> aList, final int aFromIndex, final int aToIndex) {
        if (aFromIndex < 0) {
            throw new IndexOutOfBoundsException("fromIndex = " + aFromIndex);
        }

        if (aToIndex > aList.size()) {
            throw new IndexOutOfBoundsException("toIndex = " + aToIndex);
        }

        if (aFromIndex > aToIndex) {
            throw new IllegalArgumentException("fromIndex(" + aFromIndex + ") > toIndex(" + aToIndex + ")");
        }

        myList = aList;
        myOffset = aFromIndex;
        mySize = aToIndex - aFromIndex;
        myExpectedModCount = myList.myModCount;
    }

    @Override
    public E set(final int aIndex, final E aElement) {
        rangeCheck(aIndex);
        checkForComodification();

        return myList.set(aIndex + myOffset, aElement);
    }

    @Override
    public E get(final int aIndex) {
        rangeCheck(aIndex);
        checkForComodification();

        return myList.get(aIndex + myOffset);
    }

    @Override
    public int size() {
        checkForComodification();

        return mySize;
    }

    @Override
    public void add(final int aIndex, final E aElement) {
        if (aIndex < 0 || aIndex > mySize) {
            throw new IndexOutOfBoundsException();
        }

        checkForComodification();
        myList.add(aIndex + myOffset, aElement);
        myExpectedModCount = myList.myModCount;
        mySize++;
        modCount++;
    }

    @Override
    public E remove(final int aIndex) {
        final E result;

        rangeCheck(aIndex);
        checkForComodification();
        result = myList.remove(aIndex + myOffset);
        myExpectedModCount = myList.myModCount;
        mySize--;
        modCount++;

        return result;
    }

    @Override
    protected void removeRange(final int aFromIndex, final int aToIndex) {
        checkForComodification();
        myList.removeRange(aFromIndex + myOffset, aToIndex + myOffset);
        myExpectedModCount = myList.myModCount;
        mySize -= aToIndex - aFromIndex;
        modCount++;
    }

    @Override
    public boolean addAll(final Collection<? extends E> aCollection) {
        return addAll(mySize, aCollection);
    }

    @Override
    public boolean addAll(final int aIndex, final Collection<? extends E> aCollection) {
        if (aIndex < 0 || aIndex > mySize) {
            throw new IndexOutOfBoundsException("Index:  " + aIndex + ", Size:  " + mySize);
        }

        final int cSize = aCollection.size();

        if (cSize == 0) {
            return false;
        }

        checkForComodification();
        myList.addAll(myOffset + aIndex, aCollection);
        myExpectedModCount = myList.myModCount;
        mySize += cSize;
        modCount++;

        return true;
    }

    @Override
    public Iterator<E> iterator() {
        return listIterator();
    }

    @Override
    public ListIterator<E> listIterator(final int aIndex) {
        checkForComodification();

        if (aIndex < 0 || aIndex > mySize) {
            throw new IndexOutOfBoundsException("Index : " + aIndex + ", Size : " + mySize);
        }

        return new ListIterator<E>() {

            private final ListIterator<E> myIterator = myList.listIterator(aIndex + myOffset);

            @Override
            public boolean hasNext() {
                return nextIndex() < mySize;
            }

            @Override
            public E next() {
                if (hasNext()) {
                    return myIterator.next();
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public boolean hasPrevious() {
                return previousIndex() >= 0;
            }

            @Override
            public E previous() {
                if (hasPrevious()) {
                    return myIterator.previous();
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public int nextIndex() {
                return myIterator.nextIndex() - myOffset;
            }

            @Override
            public int previousIndex() {
                return myIterator.previousIndex() - myOffset;
            }

            @Override
            public void remove() {
                myIterator.remove();
                myExpectedModCount = myList.myModCount;
                mySize--;
                modCount++;
            }

            @Override
            public void set(final E aObject) {
                myIterator.set(aObject);
            }

            @Override
            public void add(final E aObject) {
                myIterator.add(aObject);
                myExpectedModCount = myList.myModCount;
                mySize++;
                modCount++;
            }
        };
    }

    @Override
    public List<E> subList(final int aFromIndex, final int aToIndex) {
        return new SubList<>(myList, myOffset + aFromIndex, myOffset + aToIndex);
    }

    private void rangeCheck(final int aIndex) {
        if (aIndex < 0 || aIndex >= mySize) {
            throw new IndexOutOfBoundsException("Index: " + aIndex + ", Size: " + mySize);
        }
    }

    private void checkForComodification() {
        if (myList.myModCount != myExpectedModCount) {
            throw new ConcurrentModificationException();
        }
    }
}
