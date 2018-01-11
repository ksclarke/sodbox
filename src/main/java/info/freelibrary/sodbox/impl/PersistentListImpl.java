
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

    private class Itr extends TreePosition implements PersistentIterator, Iterator<E> {

        /**
         * Index of element to be returned by subsequent call to next.
         */
        int cursor = 0;

        /**
         * Index of element returned by most recent call to next or previous. Reset to -1 if this element is deleted
         * by a call to remove.
         */
        int lastRet = -1;

        /**
         * The modCount value that the iterator believes that the backing List should have. If this expectation is
         * violated, the iterator has detected concurrent modification.
         */
        int expectedModCount = modCount;

        final void checkForComodification() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public boolean hasNext() {
            return cursor != size();
        }

        @Override
        public E next() {
            checkForComodification();
            try {
                final E next = getPosition(this, cursor);
                lastRet = cursor++;
                return next;
            } catch (final IndexOutOfBoundsException e) {
                checkForComodification();
                throw new NoSuchElementException();
            }
        }

        @Override
        public int nextOid() {
            checkForComodification();
            if (!hasNext()) {
                return 0;
            }
            final int oid = getStorage().getOid(getRawPosition(this, cursor));
            lastRet = cursor++;
            return oid;
        }

        @Override
        public void remove() {
            if (lastRet == -1) {
                throw new IllegalStateException();
            }
            checkForComodification();

            try {
                PersistentListImpl.this.remove(lastRet);
                if (lastRet < cursor) {
                    cursor--;
                }
                page = null;
                lastRet = -1;
                expectedModCount = modCount;
            } catch (final IndexOutOfBoundsException e) {
                throw new ConcurrentModificationException();
            }
        }
    }

    static class ListIntermediatePage extends ListPage {

        int[] nChildren;

        ListIntermediatePage() {
        }

        ListIntermediatePage(final Storage storage) {
            super(storage);
            nChildren = new int[nIntermediatePageItems];
        }

        @Override
        ListPage add(int i, final Object obj) {
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            final ListPage pg = (ListPage) items.get(j);
            ListPage overflow = pg.add(i, obj);
            if (overflow != null) {
                countChildren(j, pg);
                overflow = super.add(j, overflow);
            } else {
                modify();
                if (nChildren[j] != Integer.MAX_VALUE) {
                    nChildren[j] += 1;
                }
            }
            return overflow;
        }

        @Override
        ListPage clonePage() {
            return new ListIntermediatePage(getStorage());
        }

        @Override
        void copy(final int dstOffs, final ListPage src, final int srcOffs, final int len) {
            super.copy(dstOffs, src, srcOffs, len);
            System.arraycopy(((ListIntermediatePage) src).nChildren, srcOffs, nChildren, dstOffs, len);
        }

        void countChildren(final int i, final ListPage pg) {
            if (nChildren[i] != Integer.MAX_VALUE) {
                nChildren[i] = pg.size();
            }
        }

        @Override
        Object get(int i) {
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            return ((ListPage) items.get(j)).get(i);
        }

        @Override
        int getMaxItems() {
            return nIntermediatePageItems;
        }

        @Override
        Object getPosition(final TreePosition pos, int i) {
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            return ((ListPage) items.get(j)).getPosition(pos, i);
        }

        @Override
        Object getRawPosition(final TreePosition pos, int i) {
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            return ((ListPage) items.get(j)).getRawPosition(pos, i);
        }

        void handlePageUnderflow(final ListPage a, final int r) {
            final int an = a.nItems;
            final int max = a.getMaxItems();
            if (r + 1 < nItems) { // exists greater page
                final ListPage b = (ListPage) items.get(r + 1);
                final int bn = b.nItems;
                Assert.that(bn >= an);
                if (an + bn > max) {
                    // reallocation of nodes between pages a and b
                    final int i = bn - (an + bn >> 1);
                    b.modify();
                    a.copy(an, b, 0, i);
                    b.copy(0, b, i, bn - i);
                    b.clear(bn - i, i);
                    b.nItems -= i;
                    a.nItems += i;
                    nChildren[r] = a.size();
                    countChildren(r + 1, b);
                } else { // merge page b to a
                    a.copy(an, b, 0, bn);
                    a.nItems += bn;
                    nItems -= 1;
                    nChildren[r] = nChildren[r + 1];
                    copy(r + 1, this, r + 2, nItems - r - 1);
                    countChildren(r, a);
                    items.set(nItems, null);
                    b.deallocate();
                }
            } else { // page b is before a
                final ListPage b = (ListPage) items.get(r - 1);
                final int bn = b.nItems;
                Assert.that(bn >= an);
                b.modify();
                if (an + bn > max) {
                    // reallocation of nodes between pages a and b
                    final int i = bn - (an + bn >> 1);
                    a.copy(i, a, 0, an);
                    a.copy(0, b, bn - i, i);
                    b.clear(bn - i, i);
                    b.nItems -= i;
                    a.nItems += i;
                    nChildren[r - 1] = b.size();
                    countChildren(r, a);
                } else { // merge page b to a
                    b.copy(bn, a, 0, an);
                    b.nItems += an;
                    nItems -= 1;
                    nChildren[r - 1] = nChildren[r];
                    countChildren(r - 1, b);
                    items.set(r, null);
                    a.deallocate();
                }
            }
        }

        @Override
        void prune() {
            for (int i = 0; i < nItems; i++) {
                ((ListPage) items.get(i)).prune();
            }
            deallocate();
        }

        @Override
        Object remove(int i) {
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            final ListPage pg = (ListPage) items.get(j);
            final Object obj = pg.remove(i);
            modify();
            if (pg.underflow()) {
                handlePageUnderflow(pg, j);
            } else {
                if (nChildren[j] != Integer.MAX_VALUE) {
                    nChildren[j] -= 1;
                }
            }
            return obj;
        }

        @Override
        Object set(int i, final Object obj) {
            int j;
            for (j = 0; i >= nChildren[j]; j++) {
                i -= nChildren[j];
            }
            return ((ListPage) items.get(j)).set(i, obj);
        }

        @Override
        void setItem(final int i, final Object obj) {
            super.setItem(i, obj);
            nChildren[i] = ((ListPage) obj).size();
        }

        @Override
        int size() {
            if (nChildren[nItems - 1] == Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            } else {
                int n = 0;
                for (int i = 0; i < nItems; i++) {
                    n += nChildren[i];
                }
                return n;
            }
        }
    }

    private class ListItr extends Itr implements ListIterator<E> {

        ListItr(final int index) {
            cursor = index;
        }

        @Override
        public void add(final E o) {
            checkForComodification();

            try {
                PersistentListImpl.this.add(cursor++, o);
                lastRet = -1;
                page = null;
                expectedModCount = modCount;
            } catch (final IndexOutOfBoundsException e) {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public boolean hasPrevious() {
            return cursor != 0;
        }

        @Override
        public int nextIndex() {
            return cursor;
        }

        @Override
        public E previous() {
            checkForComodification();
            try {
                final int i = cursor - 1;
                final E previous = getPosition(this, i);
                lastRet = cursor = i;
                return previous;
            } catch (final IndexOutOfBoundsException e) {
                checkForComodification();
                throw new NoSuchElementException();
            }
        }

        @Override
        public int previousIndex() {
            return cursor - 1;
        }

        @Override
        public void set(final E o) {
            if (lastRet == -1) {
                throw new IllegalStateException();
            }
            checkForComodification();

            try {
                PersistentListImpl.this.set(lastRet, o);
                expectedModCount = modCount;
            } catch (final IndexOutOfBoundsException e) {
                throw new ConcurrentModificationException();
            }
        }
    }

    static class ListPage extends Persistent {

        int nItems;

        Link items;

        ListPage() {
        }

        ListPage(final Storage storage) {
            super(storage);
            final int max = getMaxItems();
            items = storage.createLink(max);
            items.setSize(max);
        }

        ListPage add(final int i, final Object obj) {
            final int max = getMaxItems();
            modify();
            if (nItems < max) {
                copy(i + 1, this, i, nItems - i);
                setItem(i, obj);
                nItems += 1;
                return null;
            } else {
                final ListPage b = clonePage();
                final int m = (max + 1) / 2;
                if (i < m) {
                    b.copy(0, this, 0, i);
                    b.copy(i + 1, this, i, m - i - 1);
                    copy(0, this, m - 1, max - m + 1);
                    b.setItem(i, obj);
                } else {
                    b.copy(0, this, 0, m);
                    copy(0, this, m, i - m);
                    copy(i - m + 1, this, i, max - i);
                    setItem(i - m, obj);
                }
                clear(max - m + 1, m - 1);
                nItems = max - m + 1;
                b.nItems = m;
                return b;
            }
        }

        void clear(int i, int len) {
            while (--len >= 0) {
                items.setObject(i++, null);
            }
        }

        ListPage clonePage() {
            return new ListPage(getStorage());
        }

        void copy(final int dstOffs, final ListPage src, final int srcOffs, final int len) {
            System.arraycopy(src.items.toRawArray(), srcOffs, items.toRawArray(), dstOffs, len);
        }

        Object get(final int i) {
            return items.get(i);
        }

        int getMaxItems() {
            return nLeafPageItems;
        }

        Object getPosition(final TreePosition pos, final int i) {
            pos.page = this;
            pos.index -= i;
            return items.get(i);
        }

        Object getRawPosition(final TreePosition pos, final int i) {
            pos.page = this;
            pos.index -= i;
            return items.getRaw(i);
        }

        void prune() {
            deallocate();
        }

        Object remove(final int i) {
            final Object obj = items.get(i);
            nItems -= 1;
            copy(i, this, i + 1, nItems - i);
            items.setObject(nItems, null);
            modify();
            return obj;
        }

        Object set(final int i, final Object obj) {
            return items.set(i, obj);
        }

        void setItem(final int i, final Object obj) {
            items.setObject(i, obj);
        }

        int size() {
            return nItems;
        }

        boolean underflow() {
            return nItems < getMaxItems() / 3;
        }
    }

    static class TreePosition {

        /**
         * B-Tree page where element is located
         */
        ListPage page;

        /**
         * Index of first element at the page
         */
        int index;
    }

    static final int nLeafPageItems = (Page.pageSize - ObjectHeader.sizeof - 8) / 4;

    static final int nIntermediatePageItems = (Page.pageSize - ObjectHeader.sizeof - 12) / 8;

    int nElems;

    ListPage root;

    transient int modCount;

    PersistentListImpl() {
    }

    PersistentListImpl(final Storage storage) {
        super(storage);
        root = new ListPage(storage);
    }

    @Override
    public boolean add(final E o) {
        add(nElems, o);
        return true;
    }

    @Override
    public void add(final int i, final E obj) {
        if (i < 0 || i > nElems) {
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        final ListPage overflow = root.add(i, obj);
        if (overflow != null) {
            final ListIntermediatePage pg = new ListIntermediatePage(getStorage());
            pg.setItem(0, overflow);
            pg.items.setObject(1, root);
            pg.nChildren[1] = Integer.MAX_VALUE;
            pg.nItems = 2;
            root = pg;
        }
        nElems += 1;
        modCount += 1;
        modify();
    }

    @Override
    public boolean addAll(int index, final Collection<? extends E> c) {
        boolean modified = false;
        final Iterator<? extends E> e = c.iterator();
        while (e.hasNext()) {
            add(index++, e.next());
            modified = true;
        }
        return modified;
    }

    @Override
    public void clear() {
        modCount += 1;
        root.prune();
        root = new ListPage(getStorage());
        nElems = 0;
        modify();
    }

    @Override
    public boolean contains(final Object o) {
        final Iterator e = iterator();
        if (o == null) {
            while (e.hasNext()) {
                if (e.next() == null) {
                    return true;
                }
            }
        } else {
            while (e.hasNext()) {
                if (o.equals(e.next())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public E get(final int i) {
        if (i < 0 || i >= nElems) {
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        return (E) root.get(i);
    }

    E getPosition(final TreePosition pos, final int i) {
        if (i < 0 || i >= nElems) {
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        if (pos.page != null && i >= pos.index && i < pos.index + pos.page.nItems) {
            return (E) pos.page.items.get(i - pos.index);
        }
        pos.index = i;
        return (E) root.getPosition(pos, i);
    }

    Object getRawPosition(final TreePosition pos, final int i) {
        if (i < 0 || i >= nElems) {
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        if (pos.page != null && i >= pos.index && i < pos.index + pos.page.nItems) {
            return pos.page.items.getRaw(i - pos.index);
        }
        pos.index = i;
        return root.getRawPosition(pos, i);
    }

    @Override
    public int indexOf(final Object o) {
        final ListIterator<E> e = listIterator();
        if (o == null) {
            while (e.hasNext()) {
                if (e.next() == null) {
                    return e.previousIndex();
                }
            }
        } else {
            while (e.hasNext()) {
                if (o.equals(e.next())) {
                    return e.previousIndex();
                }
            }
        }
        return -1;
    }

    @Override
    public boolean isEmpty() {
        return nElems == 0;
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    @Override
    public int lastIndexOf(final Object o) {
        final ListIterator<E> e = listIterator(size());
        if (o == null) {
            while (e.hasPrevious()) {
                if (e.previous() == null) {
                    return e.nextIndex();
                }
            }
        } else {
            while (e.hasPrevious()) {
                if (o.equals(e.previous())) {
                    return e.nextIndex();
                }
            }
        }
        return -1;
    }

    @Override
    public ListIterator<E> listIterator() {
        return listIterator(0);
    }

    @Override
    public ListIterator<E> listIterator(final int index) {
        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }

        return new ListItr(index);
    }

    @Override
    public E remove(final int i) {
        if (i < 0 || i >= nElems) {
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        final E obj = (E) root.remove(i);
        if (root.nItems == 1 && root instanceof ListIntermediatePage) {
            final ListPage newRoot = (ListPage) root.items.get(0);
            root.deallocate();
            root = newRoot;
        }
        nElems -= 1;
        modCount += 1;
        modify();
        return obj;
    }

    protected void removeRange(final int fromIndex, int toIndex) {
        while (fromIndex < toIndex) {
            remove(fromIndex);
            toIndex -= 1;
        }
    }

    @Override
    public E set(final int i, final E obj) {
        if (i < 0 || i >= nElems) {
            throw new IndexOutOfBoundsException("index=" + i + ", size=" + nElems);
        }
        return (E) root.set(i, obj);
    }

    @Override
    public int size() {
        return nElems;
    }

    @Override
    public List<E> subList(final int fromIndex, final int toIndex) {
        return new SubList<E>(this, fromIndex, toIndex);
    }

    @Override
    public Object[] toArray() {
        final int n = nElems;
        final Object[] arr = new Object[n];
        final Iterator<E> iterator = listIterator(0);
        for (int i = 0; i < n; i++) {
            arr[i] = iterator.next();
        }
        return arr;
    }

    @Override
    public <T> T[] toArray(T[] arr) {
        final int n = nElems;
        if (arr.length < n) {
            arr = (T[]) Array.newInstance(arr.getClass().getComponentType(), n);
        }
        final Iterator<E> iterator = listIterator(0);
        for (int i = 0; i < n; i++) {
            arr[i] = (T) iterator.next();
        }
        return arr;
    }
}

class SubList<E> extends AbstractList<E> {

    private final PersistentListImpl<E> l;

    private final int offset;

    private int size;

    private int expectedModCount;

    SubList(final PersistentListImpl<E> list, final int fromIndex, final int toIndex) {
        if (fromIndex < 0) {
            throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
        }
        if (toIndex > list.size()) {
            throw new IndexOutOfBoundsException("toIndex = " + toIndex);
        }
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
        }
        l = list;
        offset = fromIndex;
        size = toIndex - fromIndex;
        expectedModCount = l.modCount;
    }

    @Override
    public void add(final int index, final E element) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException();
        }
        checkForComodification();
        l.add(index + offset, element);
        expectedModCount = l.modCount;
        size++;
        modCount++;
    }

    @Override
    public boolean addAll(final Collection<? extends E> c) {
        return addAll(size, c);
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends E> c) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        final int cSize = c.size();
        if (cSize == 0) {
            return false;
        }
        checkForComodification();
        l.addAll(offset + index, c);
        expectedModCount = l.modCount;
        size += cSize;
        modCount++;
        return true;
    }

    private void checkForComodification() {
        if (l.modCount != expectedModCount) {
            throw new ConcurrentModificationException();
        }
    }

    @Override
    public E get(final int index) {
        rangeCheck(index);
        checkForComodification();
        return l.get(index + offset);
    }

    @Override
    public Iterator<E> iterator() {
        return listIterator();
    }

    @Override
    public ListIterator<E> listIterator(final int index) {
        checkForComodification();
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        return new ListIterator<E>() {

            private final ListIterator<E> i = l.listIterator(index + offset);

            @Override
            public void add(final E o) {
                i.add(o);
                expectedModCount = l.modCount;
                size++;
                modCount++;
            }

            @Override
            public boolean hasNext() {
                return nextIndex() < size;
            }

            @Override
            public boolean hasPrevious() {
                return previousIndex() >= 0;
            }

            @Override
            public E next() {
                if (hasNext()) {
                    return i.next();
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public int nextIndex() {
                return i.nextIndex() - offset;
            }

            @Override
            public E previous() {
                if (hasPrevious()) {
                    return i.previous();
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public int previousIndex() {
                return i.previousIndex() - offset;
            }

            @Override
            public void remove() {
                i.remove();
                expectedModCount = l.modCount;
                size--;
                modCount++;
            }

            @Override
            public void set(final E o) {
                i.set(o);
            }
        };
    }

    private void rangeCheck(final int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ",Size: " + size);
        }
    }

    @Override
    public E remove(final int index) {
        rangeCheck(index);
        checkForComodification();
        final E result = l.remove(index + offset);
        expectedModCount = l.modCount;
        size--;
        modCount++;
        return result;
    }

    @Override
    protected void removeRange(final int fromIndex, final int toIndex) {
        checkForComodification();
        l.removeRange(fromIndex + offset, toIndex + offset);
        expectedModCount = l.modCount;
        size -= toIndex - fromIndex;
        modCount++;
    }

    @Override
    public E set(final int index, final E element) {
        rangeCheck(index);
        checkForComodification();
        return l.set(index + offset, element);
    }

    @Override
    public int size() {
        checkForComodification();
        return size;
    }

    @Override
    public List<E> subList(final int fromIndex, final int toIndex) {
        return new SubList<E>(l, offset + fromIndex, offset + toIndex);
    }
}
