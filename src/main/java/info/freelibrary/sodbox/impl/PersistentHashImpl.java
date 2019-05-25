
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

    private static final long UINT_MASK = 0xFFFFFFFFL;

    HashPage myRoot;

    int myElementCount;

    int myLoadFactor;

    int myPageSize;

    transient volatile Set<Entry<K, V>> myEntrySet;

    transient volatile Set<K> myKeySet;

    transient volatile Collection<V> myValues;

    PersistentHashImpl(final Storage aStorage, final int aPageSize, final int aLoadFactor) {
        super(aStorage);

        myPageSize = aPageSize;
        myLoadFactor = aLoadFactor;
    }

    PersistentHashImpl() {
    }

    @Override
    public int size() {
        return myElementCount;
    }

    @Override
    public boolean isEmpty() {
        return myElementCount == 0;
    }

    @Override
    public boolean containsValue(final Object aValue) {
        final Iterator<Entry<K, V>> iterator = entrySet().iterator();

        if (aValue == null) {
            while (iterator.hasNext()) {
                final Entry<K, V> entry = iterator.next();

                if (entry.getValue() == null) {
                    return true;
                }
            }
        } else {
            while (iterator.hasNext()) {
                final Entry<K, V> entry = iterator.next();

                if (aValue.equals(entry.getValue())) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public boolean containsKey(final Object aKey) {
        return getEntry(aKey) != null;
    }

    @Override
    public V get(final Object aKey) {
        final Entry<K, V> entry = getEntry(aKey);
        return entry != null ? entry.getValue() : null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Entry<K, V> getEntry(final Object aKey) {
        HashPage page = myRoot;

        if (page != null) {
            final int hashCode = aKey.hashCode();

            int divisor = 1;

            while (true) {
                final int h = (int) ((hashCode & UINT_MASK) / divisor % myPageSize);
                final Object child = page.myItems.get(h);

                if (child instanceof HashPage) {
                    page = (HashPage) child;
                    divisor *= myPageSize;
                } else {
                    for (CollisionItem<K, V> item = (CollisionItem<K, V>) child; item != null; item = item.myNext) {
                        if (item.myHashCode == hashCode && item.myKey.equals(aKey)) {
                            return item;
                        }
                    }

                    break;
                }
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V put(final K aKey, final V aValue) {
        final int hashCode = aKey.hashCode();

        HashPage page = myRoot;

        if (page == null) {
            final int h = (int) ((hashCode & UINT_MASK) % myPageSize);

            page = new HashPage(getStorage(), myPageSize);
            page.myItems.set(h, new CollisionItem<>(aKey, aValue, hashCode));

            myRoot = page;
            myElementCount = 1;
            modify();

            return null;
        } else {
            int divisor = 1;

            while (true) {
                final int h = (int) ((hashCode & UINT_MASK) / divisor % myPageSize);
                final Object child = page.myItems.get(h);

                if (child instanceof HashPage) {
                    page = (HashPage) child;
                    divisor *= myPageSize;
                } else {
                    CollisionItem<K, V> prev = null;
                    CollisionItem<K, V> last = null;
                    int collisionChainLength = 0;

                    for (CollisionItem<K, V> item = (CollisionItem<K, V>) child; item != null; item = item.myNext) {
                        if (item.myHashCode == hashCode) {
                            if (item.myKey.equals(aKey)) {
                                final V prevValue = item.myObject;

                                item.myObject = aValue;
                                item.modify();

                                return prevValue;
                            }

                            if (prev == null || prev.myHashCode != hashCode) {
                                collisionChainLength += 1;
                            }
                            prev = item;
                        } else {
                            collisionChainLength += 1;
                        }

                        last = item;
                    }

                    if (prev == null || prev.myHashCode != hashCode) {
                        collisionChainLength += 1;
                    }

                    if (collisionChainLength > myLoadFactor) {
                        final HashPage newPage = new HashPage(getStorage(), myPageSize);

                        divisor *= myPageSize;
                        CollisionItem<K, V> next;

                        for (CollisionItem<K, V> item = (CollisionItem<K, V>) child; item != null; item = next) {
                            final int hc = (int) ((item.myHashCode & UINT_MASK) / divisor % myPageSize);

                            next = item.myNext;
                            item.myNext = (CollisionItem<K, V>) newPage.myItems.get(hc);
                            newPage.myItems.set(hc, item);
                            item.modify();
                        }

                        page.myItems.set(h, newPage);
                        page.modify();
                        page = newPage;
                    } else {
                        final CollisionItem<K, V> newItem = new CollisionItem<>(aKey, aValue, hashCode);

                        if (prev == null) {
                            prev = last;
                        }

                        if (prev != null) {
                            newItem.myNext = prev.myNext;
                            prev.myNext = newItem;
                            prev.modify();
                        } else {
                            page.myItems.set(h, newItem);
                            page.modify();
                        }

                        myElementCount += 1;
                        modify();

                        return null;
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(final Object aKey) {
        HashPage page = myRoot;

        if (page != null) {
            final int hashCode = aKey.hashCode();

            int divisor = 1;

            while (true) {
                final int h = (int) ((hashCode & UINT_MASK) / divisor % myPageSize);
                final Object child = page.myItems.get(h);

                if (child instanceof HashPage) {
                    page = (HashPage) child;
                    divisor *= myPageSize;
                } else {
                    CollisionItem<K, V> prev = null;

                    for (CollisionItem<K, V> item = (CollisionItem<K, V>) child; item != null; item = item.myNext) {
                        if (item.myHashCode == hashCode && item.myKey.equals(aKey)) {
                            final V obj = item.myObject;

                            if (prev != null) {
                                prev.myNext = item.myNext;
                                prev.modify();
                            } else {
                                page.myItems.set(h, item.myNext);
                                page.modify();
                            }

                            myElementCount -= 1;
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
    public void putAll(final Map<? extends K, ? extends V> aMap) {
        final Iterator<? extends Entry<? extends K, ? extends V>> iterator = aMap.entrySet().iterator();

        while (iterator.hasNext()) {
            final Entry<? extends K, ? extends V> e = iterator.next();

            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        if (myRoot != null) {
            myRoot.deallocate();
            myRoot = null;
            myElementCount = 0;
            modify();
        }
    }

    @Override
    public Set<K> keySet() {
        if (myKeySet == null) {
            myKeySet = new AbstractSet<K>() {

                @Override
                public Iterator<K> iterator() {
                    return new Iterator<K>() {

                        private final Iterator<Entry<K, V>> myKeyIterator = entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return myKeyIterator.hasNext();
                        }

                        @Override
                        public K next() {
                            return myKeyIterator.next().getKey();
                        }

                        @Override
                        public void remove() {
                            myKeyIterator.remove();
                        }
                    };
                }

                @Override
                public int size() {
                    return PersistentHashImpl.this.size();
                }

                @Override
                public boolean contains(final Object aKey) {
                    return PersistentHashImpl.this.containsKey(aKey);
                }
            };
        }

        return myKeySet;
    }

    @Override
    public Collection<V> values() {
        if (myValues == null) {
            myValues = new AbstractCollection<V>() {

                @Override
                public Iterator<V> iterator() {
                    return new Iterator<V>() {

                        private final Iterator<Entry<K, V>> myValuesIterator = entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return myValuesIterator.hasNext();
                        }

                        @Override
                        public V next() {
                            return myValuesIterator.next().getValue();
                        }

                        @Override
                        public void remove() {
                            myValuesIterator.remove();
                        }
                    };
                }

                @Override
                public int size() {
                    return PersistentHashImpl.this.size();
                }

                @Override
                public boolean contains(final Object aValue) {
                    return PersistentHashImpl.this.containsValue(aValue);
                }
            };
        }

        return myValues;
    }

    protected Iterator<Entry<K, V>> entryIterator() {
        return new EntryIterator();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (myEntrySet == null) {
            myEntrySet = new AbstractSet<Entry<K, V>>() {

                @Override
                public Iterator<Entry<K, V>> iterator() {
                    return entryIterator();
                }

                @Override
                public int size() {
                    return PersistentHashImpl.this.size();
                }

                @SuppressWarnings("unchecked")
                @Override
                public boolean remove(final Object aObject) {
                    if (!(aObject instanceof Map.Entry)) {
                        return false;
                    }

                    final Map.Entry<K, V> entry = (Map.Entry<K, V>) aObject;
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

                @SuppressWarnings("unchecked")
                @Override
                public boolean contains(final Object aEntry) {
                    final Entry<K, V> entry = (Entry<K, V>) aEntry;

                    if (entry.getValue() != null) {
                        return entry.getValue().equals(PersistentHashImpl.this.get(entry.getKey()));
                    } else {
                        return PersistentHashImpl.this.containsKey(entry.getKey()) && PersistentHashImpl.this.get(
                                entry.getKey()) == null;
                    }
                }
            };
        }

        return myEntrySet;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(final Object aObject) {
        if (aObject == this) {
            return true;
        }

        if (!(aObject instanceof Map)) {
            return false;
        }

        final Map<K, V> map = (Map<K, V>) aObject;

        if (map.size() != size()) {
            return false;
        }

        try {
            final Iterator<Entry<K, V>> iterator = entrySet().iterator();

            while (iterator.hasNext()) {
                final Entry<K, V> e = iterator.next();
                final K key = e.getKey();
                final V value = e.getValue();

                if (value == null) {
                    if (!(map.get(key) == null && map.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!value.equals(map.get(key))) {
                        return false;
                    }
                }
            }
        } catch (final ClassCastException details) {
            return false;
        } catch (final NullPointerException details) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        final Iterator<Entry<K, V>> iterator = entrySet().iterator();

        int h = 0;

        while (iterator.hasNext()) {
            h += iterator.next().hashCode();
        }

        return h;
    }

    @Override
    public String toString() {
        final StringBuffer buffer = new StringBuffer();
        final Iterator<Entry<K, V>> iterator = entrySet().iterator();

        buffer.append("{");

        boolean hasNext = iterator.hasNext();

        while (hasNext) {
            final Entry<K, V> e = iterator.next();
            final K key = e.getKey();
            final V value = e.getValue();

            if (key == this) {
                buffer.append("( this Hash )");
            } else {
                buffer.append(key);
            }

            buffer.append("=");

            if (value == this) {
                buffer.append("(this Hash)");
            } else {
                buffer.append(value);
            }

            hasNext = iterator.hasNext();

            if (hasNext) {
                buffer.append(", ");
            }
        }

        buffer.append("}");

        return buffer.toString();
    }

    static class HashPage extends Persistent {

        Link myItems;

        HashPage(final Storage aStorage, final int aPageSize) {
            super(aStorage);

            myItems = aStorage.createLink(aPageSize);
            myItems.setSize(aPageSize);
        }

        HashPage() {
        }

        @Override
        public void deallocate() {
            for (final Object child : myItems) {
                if (child instanceof HashPage) {
                    ((HashPage) child).deallocate();
                } else {
                    CollisionItem next;

                    for (CollisionItem item = (CollisionItem) child; item != null; item = next) {
                        next = item.myNext;
                        item.deallocate();
                    }
                }
            }

            super.deallocate();
        }
    }

    static class CollisionItem<K, V> extends Persistent implements Entry<K, V> {

        K myKey;

        V myObject;

        int myHashCode;

        CollisionItem<K, V> myNext;

        CollisionItem(final K aKey, final V aObject, final int aHashCode) {
            myKey = aKey;
            myObject = aObject;
            myHashCode = aHashCode;
        }

        CollisionItem() {
        }

        @Override
        public K getKey() {
            return myKey;
        }

        @Override
        public V getValue() {
            return myObject;
        }

        @Override
        public V setValue(final V aValue) {
            modify();

            final V prevValue = myObject;

            myObject = aValue;

            return prevValue;
        }
    }

    static class StackElem {

        HashPage myPage;

        int myPosition;

        StackElem(final HashPage aPage, final int aPosition) {
            myPage = aPage;
            myPosition = aPosition;
        }
    }

    class EntryIterator implements Iterator<Entry<K, V>> {

        CollisionItem<K, V> myCurrentItem;

        CollisionItem<K, V> myNextItem;

        Stack<StackElem> myStack = new Stack<>();

        @SuppressWarnings("unchecked")
        EntryIterator() {
            HashPage page = myRoot;

            if (page != null) {
                int start = 0;

                DepthFirst:
                while (true) {
                    for (int index = start, n = page.myItems.size(); index < n; index++) {
                        final Object child = page.myItems.get(index);

                        if (child != null) {
                            myStack.push(new StackElem(page, index));

                            if (child instanceof HashPage) {
                                page = (HashPage) child;
                                start = 0;

                                continue DepthFirst;
                            } else {
                                myNextItem = (CollisionItem<K, V>) child;
                                return;
                            }
                        }
                    }

                    if (myStack.isEmpty()) {
                        break;
                    } else {
                        final StackElem top = myStack.pop();

                        page = top.myPage;
                        start = top.myPosition + 1;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return myNextItem != null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Entry<K, V> next() {
            if (myNextItem == null) {
                throw new NoSuchElementException();
            }

            myCurrentItem = myNextItem;

            if ((myNextItem = myNextItem.myNext) == null) {
                do {
                    final StackElem top = myStack.pop();

                    HashPage page = top.myPage;
                    int start = top.myPosition + 1;

                    DepthFirst:
                    while (true) {
                        for (int index = start, n = page.myItems.size(); index < n; index++) {
                            final Object child = page.myItems.get(index);

                            if (child != null) {
                                myStack.push(new StackElem(page, index));

                                if (child instanceof HashPage) {
                                    page = (HashPage) child;
                                    start = 0;

                                    continue DepthFirst;
                                } else {
                                    myNextItem = (CollisionItem<K, V>) child;

                                    return myCurrentItem;
                                }
                            }
                        }

                        break;
                    }
                } while (!myStack.isEmpty());
            }

            return myCurrentItem;
        }

        @Override
        public void remove() {
            if (myCurrentItem == null) {
                throw new NoSuchElementException();
            }

            PersistentHashImpl.this.remove(myCurrentItem.myKey);
        }
    }
}
