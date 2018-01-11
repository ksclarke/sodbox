
package info.freelibrary.sodbox.impl;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;

import info.freelibrary.sodbox.IPersistentHash;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentResource;
import info.freelibrary.sodbox.Storage;

class PersistentHashImpl<K, V> extends PersistentResource implements IPersistentHash<K, V> {

    static class CollisionItem<K, V> extends Persistent implements Entry<K, V> {

        K key;

        V obj;

        int hashCode;

        CollisionItem<K, V> next;

        CollisionItem() {
        }

        CollisionItem(final K key, final V obj, final int hashCode) {
            this.key = key;
            this.obj = obj;
            this.hashCode = hashCode;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return obj;
        }

        @Override
        public V setValue(final V value) {
            modify();
            final V prevValue = obj;
            obj = value;
            return prevValue;
        }
    }

    class EntryIterator implements Iterator<Entry<K, V>> {

        CollisionItem<K, V> currItem;

        CollisionItem<K, V> nextItem;

        Stack<StackElem> stack = new Stack<StackElem>();

        EntryIterator() {
            HashPage pg = root;

            if (pg != null) {
                int start = 0;
                DepthFirst:
                while (true) {
                    for (int i = start, n = pg.items.size(); i < n; i++) {
                        final Object child = pg.items.get(i);
                        if (child != null) {
                            stack.push(new StackElem(pg, i));
                            if (child instanceof HashPage) {
                                pg = (HashPage) child;
                                start = 0;
                                continue DepthFirst;
                            } else {
                                nextItem = (CollisionItem<K, V>) child;
                                return;
                            }
                        }
                    }
                    if (stack.isEmpty()) {
                        break;
                    } else {
                        final StackElem top = stack.pop();
                        pg = top.page;
                        start = top.pos + 1;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return nextItem != null;
        }

        @Override
        public Entry<K, V> next() {
            if (nextItem == null) {
                throw new NoSuchElementException();
            }
            currItem = nextItem;
            if ((nextItem = nextItem.next) == null) {
                do {
                    final StackElem top = stack.pop();
                    HashPage pg = top.page;
                    int start = top.pos + 1;

                    DepthFirst:
                    while (true) {
                        for (int i = start, n = pg.items.size(); i < n; i++) {
                            final Object child = pg.items.get(i);
                            if (child != null) {
                                stack.push(new StackElem(pg, i));
                                if (child instanceof HashPage) {
                                    pg = (HashPage) child;
                                    start = 0;
                                    continue DepthFirst;
                                } else {
                                    nextItem = (CollisionItem<K, V>) child;
                                    return currItem;
                                }
                            }
                        }
                        break;
                    }
                } while (!stack.isEmpty());
            }
            return currItem;
        }

        @Override
        public void remove() {
            if (currItem == null) {
                throw new NoSuchElementException();
            }
            PersistentHashImpl.this.remove(currItem.key);
        }
    }

    static class HashPage extends Persistent {

        Link items;

        HashPage() {
        }

        HashPage(final Storage db, final int pageSize) {
            super(db);
            items = db.createLink(pageSize);
            items.setSize(pageSize);
        }

        @Override
        public void deallocate() {
            for (final Object child : items) {
                if (child instanceof HashPage) {
                    ((HashPage) child).deallocate();
                } else {
                    CollisionItem next;
                    for (CollisionItem item = (CollisionItem) child; item != null; item = next) {
                        next = item.next;
                        item.deallocate();
                    }
                }
            }
            super.deallocate();
        }
    }

    static class StackElem {

        HashPage page;

        int pos;

        StackElem(final HashPage page, final int pos) {
            this.page = page;
            this.pos = pos;
        }
    }

    private static final long UINT_MASK = 0xFFFFFFFFL;

    HashPage root;

    int nElems;

    int loadFactor;

    int pageSize;

    transient volatile Set<Entry<K, V>> entrySet;

    transient volatile Set<K> keySet;

    transient volatile Collection<V> valuesCol;

    PersistentHashImpl() {
    }

    PersistentHashImpl(final Storage storage, final int pageSize, final int loadFactor) {
        super(storage);
        this.pageSize = pageSize;
        this.loadFactor = loadFactor;
    }

    @Override
    public void clear() {
        if (root != null) {
            root.deallocate();
            root = null;
            nElems = 0;
            modify();
        }
    }

    @Override
    public boolean containsKey(final Object key) {
        return getEntry(key) != null;
    }

    @Override
    public boolean containsValue(final Object value) {
        final Iterator<Entry<K, V>> i = entrySet().iterator();
        if (value == null) {
            while (i.hasNext()) {
                final Entry<K, V> e = i.next();
                if (e.getValue() == null) {
                    return true;
                }
            }
        } else {
            while (i.hasNext()) {
                final Entry<K, V> e = i.next();
                if (value.equals(e.getValue())) {
                    return true;
                }
            }
        }
        return false;
    }

    protected Iterator<Entry<K, V>> entryIterator() {
        return new EntryIterator();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new AbstractSet<Entry<K, V>>() {

                @Override
                public boolean contains(final Object k) {
                    final Entry<K, V> e = (Entry<K, V>) k;
                    if (e.getValue() != null) {
                        return e.getValue().equals(PersistentHashImpl.this.get(e.getKey()));
                    } else {
                        return PersistentHashImpl.this.containsKey(e.getKey()) && PersistentHashImpl.this.get(e
                                .getKey()) == null;
                    }
                }

                @Override
                public Iterator<Entry<K, V>> iterator() {
                    return entryIterator();
                }

                @Override
                public boolean remove(final Object o) {
                    if (!(o instanceof Map.Entry)) {
                        return false;
                    }
                    final Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
                    final K key = entry.getKey();
                    final V value = entry.getValue();
                    if (value != null) {
                        final V v = PersistentHashImpl.this.get(key);
                        if (value.equals(v)) {
                            PersistentHashImpl.this.remove(key);
                            return true;
                        }
                    } else {
                        if (PersistentHashImpl.this.containsKey(key) && PersistentHashImpl.this.get(key) == null) {
                            PersistentHashImpl.this.remove(key);
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public int size() {
                    return PersistentHashImpl.this.size();
                }
            };
        }
        return entrySet;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Map)) {
            return false;
        }
        final Map<K, V> t = (Map<K, V>) o;
        if (t.size() != size()) {
            return false;
        }

        try {
            final Iterator<Entry<K, V>> i = entrySet().iterator();
            while (i.hasNext()) {
                final Entry<K, V> e = i.next();
                final K key = e.getKey();
                final V value = e.getValue();
                if (value == null) {
                    if (!(t.get(key) == null && t.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!value.equals(t.get(key))) {
                        return false;
                    }
                }
            }
        } catch (final ClassCastException unused) {
            return false;
        } catch (final NullPointerException unused) {
            return false;
        }

        return true;
    }

    @Override
    public V get(final Object key) {
        final Entry<K, V> entry = getEntry(key);
        return entry != null ? entry.getValue() : null;
    }

    @Override
    public Entry<K, V> getEntry(final Object key) {
        HashPage pg = root;
        if (pg != null) {
            int divisor = 1;
            final int hashCode = key.hashCode();
            while (true) {
                final int h = (int) ((hashCode & UINT_MASK) / divisor % pageSize);
                final Object child = pg.items.get(h);
                if (child instanceof HashPage) {
                    pg = (HashPage) child;
                    divisor *= pageSize;
                } else {
                    for (CollisionItem<K, V> item = (CollisionItem<K, V>) child; item != null; item = item.next) {
                        if (item.hashCode == hashCode && item.key.equals(key)) {
                            return item;
                        }
                    }
                    break;
                }
            }
        }
        return null;
    }

    @Override
    public int hashCode() {
        int h = 0;
        final Iterator<Entry<K, V>> i = entrySet().iterator();
        while (i.hasNext()) {
            h += i.next().hashCode();
        }
        return h;
    }

    @Override
    public boolean isEmpty() {
        return nElems == 0;
    }

    @Override
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = new AbstractSet<K>() {

                @Override
                public boolean contains(final Object k) {
                    return PersistentHashImpl.this.containsKey(k);
                }

                @Override
                public Iterator<K> iterator() {
                    return new Iterator<K>() {

                        private final Iterator<Entry<K, V>> i = entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return i.hasNext();
                        }

                        @Override
                        public K next() {
                            return i.next().getKey();
                        }

                        @Override
                        public void remove() {
                            i.remove();
                        }
                    };
                }

                @Override
                public int size() {
                    return PersistentHashImpl.this.size();
                }
            };
        }
        return keySet;
    }

    @Override
    public V put(final K key, final V value) {
        final int hashCode = key.hashCode();
        HashPage pg = root;
        if (pg == null) {
            pg = new HashPage(getStorage(), pageSize);
            final int h = (int) ((hashCode & UINT_MASK) % pageSize);
            pg.items.set(h, new CollisionItem<K, V>(key, value, hashCode));
            root = pg;
            nElems = 1;
            modify();
            return null;
        } else {
            int divisor = 1;
            while (true) {
                final int h = (int) ((hashCode & UINT_MASK) / divisor % pageSize);
                final Object child = pg.items.get(h);
                if (child instanceof HashPage) {
                    pg = (HashPage) child;
                    divisor *= pageSize;
                } else {
                    CollisionItem<K, V> prev = null;
                    CollisionItem<K, V> last = null;
                    int collisionChainLength = 0;
                    for (CollisionItem<K, V> item = (CollisionItem<K, V>) child; item != null; item = item.next) {
                        if (item.hashCode == hashCode) {
                            if (item.key.equals(key)) {
                                final V prevValue = item.obj;
                                item.obj = value;
                                item.modify();
                                return prevValue;
                            }
                            if (prev == null || prev.hashCode != hashCode) {
                                collisionChainLength += 1;
                            }
                            prev = item;
                        } else {
                            collisionChainLength += 1;
                        }
                        last = item;
                    }
                    if (prev == null || prev.hashCode != hashCode) {
                        collisionChainLength += 1;
                    }
                    if (collisionChainLength > loadFactor) {
                        final HashPage newPage = new HashPage(getStorage(), pageSize);
                        divisor *= pageSize;
                        CollisionItem<K, V> next;
                        for (CollisionItem<K, V> item = (CollisionItem<K, V>) child; item != null; item = next) {
                            next = item.next;
                            final int hc = (int) ((item.hashCode & UINT_MASK) / divisor % pageSize);
                            item.next = (CollisionItem<K, V>) newPage.items.get(hc);
                            newPage.items.set(hc, item);
                            item.modify();
                        }
                        pg.items.set(h, newPage);
                        pg.modify();
                        pg = newPage;
                    } else {
                        final CollisionItem<K, V> newItem = new CollisionItem<K, V>(key, value, hashCode);
                        if (prev == null) {
                            prev = last;
                        }
                        if (prev != null) {
                            newItem.next = prev.next;
                            prev.next = newItem;
                            prev.modify();
                        } else {
                            pg.items.set(h, newItem);
                            pg.modify();
                        }
                        nElems += 1;
                        modify();
                        return null;
                    }
                }
            }
        }
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> t) {
        final Iterator<? extends Entry<? extends K, ? extends V>> i = t.entrySet().iterator();
        while (i.hasNext()) {
            final Entry<? extends K, ? extends V> e = i.next();
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public V remove(final Object key) {
        HashPage pg = root;
        if (pg != null) {
            int divisor = 1;
            final int hashCode = key.hashCode();
            while (true) {
                final int h = (int) ((hashCode & UINT_MASK) / divisor % pageSize);
                final Object child = pg.items.get(h);
                if (child instanceof HashPage) {
                    pg = (HashPage) child;
                    divisor *= pageSize;
                } else {
                    CollisionItem<K, V> prev = null;
                    for (CollisionItem<K, V> item = (CollisionItem<K, V>) child; item != null; item = item.next) {
                        if (item.hashCode == hashCode && item.key.equals(key)) {
                            final V obj = item.obj;
                            if (prev != null) {
                                prev.next = item.next;
                                prev.modify();
                            } else {
                                pg.items.set(h, item.next);
                                pg.modify();
                            }
                            nElems -= 1;
                            modify();
                            return obj;
                        }
                        prev = item;
                    }
                    break;
                }
            }
        }
        return null;
    }

    @Override
    public int size() {
        return nElems;
    }

    @Override
    public String toString() {
        final StringBuffer buf = new StringBuffer();
        buf.append("{");

        final Iterator<Entry<K, V>> i = entrySet().iterator();
        boolean hasNext = i.hasNext();
        while (hasNext) {
            final Entry<K, V> e = i.next();
            final K key = e.getKey();
            final V value = e.getValue();
            if (key == this) {
                buf.append("(this Hash)");
            } else {
                buf.append(key);
            }
            buf.append("=");
            if (value == this) {
                buf.append("(this Hash)");
            } else {
                buf.append(value);
            }
            hasNext = i.hasNext();
            if (hasNext) {
                buf.append(", ");
            }
        }

        buf.append("}");
        return buf.toString();
    }

    @Override
    public Collection<V> values() {
        if (valuesCol == null) {
            valuesCol = new AbstractCollection<V>() {

                @Override
                public boolean contains(final Object v) {
                    return PersistentHashImpl.this.containsValue(v);
                }

                @Override
                public Iterator<V> iterator() {
                    return new Iterator<V>() {

                        private final Iterator<Entry<K, V>> i = entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return i.hasNext();
                        }

                        @Override
                        public V next() {
                            return i.next().getValue();
                        }

                        @Override
                        public void remove() {
                            i.remove();
                        }
                    };
                }

                @Override
                public int size() {
                    return PersistentHashImpl.this.size();
                }
            };
        }
        return valuesCol;
    }
}
