
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

    static class Neighbor {

        Object child;

        Neighbor next;

        int level;

        double distance;

        Neighbor(final Object child, final double distance, final int level) {
            this.child = child;
            this.distance = distance;
            this.level = level;
        }
    }

    class NeighborIterator<E> extends IterableIterator<E> implements PersistentIterator {

        Neighbor list;

        int counter;

        int x;

        int y;

        NeighborIterator(final int x, final int y) {
            this.x = x;
            this.y = y;
            counter = updateCounter;
            if (height == 0) {
                return;
            }
            list = new Neighbor(root, root.cover().distance(x, y), height);
        }

        @Override
        public boolean hasNext() {
            if (counter != updateCounter) {
                throw new ConcurrentModificationException();
            }
            while (true) {
                final Neighbor neighbor = list;
                if (neighbor == null) {
                    return false;
                }
                if (neighbor.level == 0) {
                    return true;
                }
                list = neighbor.next;
                final RtreePage pg = (RtreePage) neighbor.child;
                for (int i = 0, n = pg.n; i < n; i++) {
                    insert(new Neighbor(pg.branch.get(i), pg.b[i].distance(x, y), neighbor.level - 1));
                }
            }
        }

        void insert(final Neighbor node) {
            Neighbor prev = null, next = list;
            final double distance = node.distance;
            while (next != null && next.distance < distance) {
                prev = next;
                next = prev.next;
            }
            node.next = next;
            if (prev == null) {
                list = node;
            } else {
                prev.next = node;
            }
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final Neighbor neighbor = list;
            list = neighbor.next;
            Assert.that(neighbor.level == 0);
            return (E) neighbor.child;
        }

        @Override
        public int nextOid() {
            return getStorage().getOid(next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static class RtreeEntry<T> implements Map.Entry<Rectangle, T> {

        RtreePage pg;

        int pos;

        RtreeEntry(final RtreePage pg, final int pos) {
            this.pg = pg;
            this.pos = pos;
        }

        @Override
        public Rectangle getKey() {
            return pg.b[pos];
        }

        @Override
        public T getValue() {
            return (T) pg.branch.get(pos);
        }

        @Override
        public T setValue(final T value) {
            throw new UnsupportedOperationException();
        }
    }

    class RtreeEntryIterator extends RtreeIterator<Map.Entry<Rectangle, T>> {

        RtreeEntryIterator(final Rectangle r) {
            super(r);
        }

        @Override
        protected Object current(final int sp) {
            return new RtreeEntry(pageStack[sp], posStack[sp]);
        }
    }

    class RtreeIterator<E> extends IterableIterator<E> implements PersistentIterator {

        RtreePage[] pageStack;

        int[] posStack;

        int counter;

        Rectangle r;

        RtreeIterator(final Rectangle r) {
            counter = updateCounter;
            if (height == 0) {
                return;
            }
            this.r = r;
            pageStack = new RtreePage[height];
            posStack = new int[height];

            if (!gotoFirstItem(0, root)) {
                pageStack = null;
                posStack = null;
            }
        }

        protected Object current(final int sp) {
            return pageStack[sp].branch.get(posStack[sp]);
        }

        private boolean gotoFirstItem(final int sp, final RtreePage pg) {
            for (int i = 0, n = pg.n; i < n; i++) {
                if (r.intersects(pg.b[i])) {
                    if (sp + 1 == height || gotoFirstItem(sp + 1, (RtreePage) pg.branch.get(i))) {
                        pageStack[sp] = pg;
                        posStack[sp] = i;
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean gotoNextItem(final int sp) {
            final RtreePage pg = pageStack[sp];
            for (int i = posStack[sp], n = pg.n; ++i < n;) {
                if (r.intersects(pg.b[i])) {
                    if (sp + 1 == height || gotoFirstItem(sp + 1, (RtreePage) pg.branch.get(i))) {
                        pageStack[sp] = pg;
                        posStack[sp] = i;
                        return true;
                    }
                }
            }
            pageStack[sp] = null;
            return sp > 0 ? gotoNextItem(sp - 1) : false;
        }

        @Override
        public boolean hasNext() {
            if (counter != updateCounter) {
                throw new ConcurrentModificationException();
            }
            return pageStack != null;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final E curr = (E) current(height - 1);
            if (!gotoNextItem(height - 1)) {
                pageStack = null;
                posStack = null;
            }
            return curr;
        }

        @Override
        public int nextOid() {
            if (!hasNext()) {
                return 0;
            }
            final int oid = getStorage().getOid(pageStack[height - 1].branch.getRaw(posStack[height - 1]));
            if (!gotoNextItem(height - 1)) {
                pageStack = null;
                posStack = null;
            }
            return oid;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private int height;

    private int n;

    private RtreePage root;

    private transient int updateCounter;

    Rtree() {
    }

    @Override
    public void clear() {
        if (root != null) {
            root.purge(height);
            root = null;
        }
        height = 0;
        n = 0;
        modify();
    }

    @Override
    public void deallocate() {
        clear();
        super.deallocate();
    }

    @Override
    public IterableIterator<Map.Entry<Rectangle, T>> entryIterator() {
        return entryIterator(getWrappingRectangle());
    }

    @Override
    public IterableIterator<Map.Entry<Rectangle, T>> entryIterator(final Rectangle r) {
        return new RtreeEntryIterator(r);
    }

    @Override
    public Object[] get(final Rectangle r) {
        return getList(r).toArray();
    }

    @Override
    public ArrayList<T> getList(final Rectangle r) {
        final ArrayList<T> result = new ArrayList<T>();
        if (root != null) {
            root.find(r, result, height);
        }
        return result;
    }

    @Override
    public Rectangle getWrappingRectangle() {
        if (root != null) {
            return root.cover();
        }
        return null;
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(getWrappingRectangle());
    }

    @Override
    public IterableIterator<T> iterator(final Rectangle r) {
        return new RtreeIterator<T>(r);
    }

    @Override
    public IterableIterator<T> neighborIterator(final int x, final int y) {
        return new NeighborIterator(x, y);
    }

    @Override
    public void put(final Rectangle r, final T obj) {
        final Storage db = getStorage();
        if (root == null) {
            root = new RtreePage(db, obj, r);
            height = 1;
        } else {
            final RtreePage p = root.insert(db, r, obj, height);
            if (p != null) {
                root = new RtreePage(db, root, p);
                height += 1;
            }
        }
        updateCounter += 1;
        n += 1;
        modify();
    }

    @Override
    public void remove(final Rectangle r, final T obj) {
        if (root == null) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
        final ArrayList reinsertList = new ArrayList();
        int reinsertLevel = root.remove(r, obj, height, reinsertList);
        if (reinsertLevel < 0) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
        for (int i = reinsertList.size(); --i >= 0;) {
            final RtreePage p = (RtreePage) reinsertList.get(i);
            for (int j = 0, n = p.n; j < n; j++) {
                final RtreePage q = root.insert(getStorage(), p.b[j], p.branch.get(j), height - reinsertLevel);
                if (q != null) {
                    // root splitted
                    root = new RtreePage(getStorage(), root, q);
                    height += 1;
                }
            }
            reinsertLevel -= 1;
            p.deallocate();
        }
        if (root.n == 1 && height > 1) {
            final RtreePage newRoot = (RtreePage) root.branch.get(0);
            root.deallocate();
            root = newRoot;
            height -= 1;
        }
        n -= 1;
        updateCounter += 1;
        modify();
    }

    @Override
    public int size() {
        return n;
    }

    @Override
    public Object[] toArray() {
        return get(getWrappingRectangle());
    }

    @Override
    public <E> E[] toArray(final E[] arr) {
        return getList(getWrappingRectangle()).toArray(arr);
    }
}
