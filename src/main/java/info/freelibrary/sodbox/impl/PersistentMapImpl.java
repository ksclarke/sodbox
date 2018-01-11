
package info.freelibrary.sodbox.impl;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;

import info.freelibrary.sodbox.GenericIndex;
import info.freelibrary.sodbox.IPersistent;
import info.freelibrary.sodbox.IPersistentMap;
import info.freelibrary.sodbox.IValue;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentComparator;
import info.freelibrary.sodbox.PersistentResource;
import info.freelibrary.sodbox.SortedCollection;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

class PersistentMapImpl<K extends Comparable, V> extends PersistentResource implements IPersistentMap<K, V> {

    static class PersistentMapComparator<K extends Comparable, V> extends
            PersistentComparator<PersistentMapEntry<K, V>> {

        @Override
        public int compareMembers(final PersistentMapEntry<K, V> m1, final PersistentMapEntry<K, V> m2) {
            return m1.key.compareTo(m2.key);
        }

        @Override
        public int compareMemberWithKey(final PersistentMapEntry<K, V> mbr, final Object key) {
            return mbr.key.compareTo(key);
        }

    }

    static class PersistentMapEntry<K extends Comparable, V> extends Persistent implements Entry<K, V> {

        K key;

        V value;

        PersistentMapEntry() {
        }

        PersistentMapEntry(final K key, final V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(final V value) {
            modify();
            final V prevValue = this.value;
            this.value = value;
            return prevValue;
        }

    }

    private class SubMap extends AbstractMap<K, V> implements SortedMap<K, V> {

        private final Key fromKey;

        private final Key toKey;

        private final K from;

        private final K to;

        volatile Set<Entry<K, V>> entrySet;

        SubMap(final K from, final K to) {
            this.from = from;
            this.to = to;
            this.fromKey = from != null ? generateKey(from, true) : null;
            this.toKey = to != null ? generateKey(to, false) : null;
        }

        @Override
        public Comparator<? super K> comparator() {
            return null;
        }

        @Override
        public boolean containsKey(final Object key) {
            return inRange((K) key) && PersistentMapImpl.this.containsKey(key);
        }

        protected Iterator<Entry<K, V>> entryIterator(final int order) {
            if (index != null) {
                if (type == ClassDescriptor.tpValue) {
                    if (order == GenericIndex.ASCENT_ORDER) {
                        return new Iterator<Entry<K, V>>() {

                            private final Iterator<PersistentMapEntry<K, V>> i =
                                    ((SortedCollection<PersistentMapEntry<K, V>>) index).iterator(fromKey, toKey);

                            @Override
                            public boolean hasNext() {
                                return i.hasNext();
                            }

                            @Override
                            public Entry<K, V> next() {
                                return i.next();
                            }

                            @Override
                            public void remove() {
                                i.remove();
                            }
                        };
                    } else {
                        return new Iterator<Entry<K, V>>() {

                            private final ArrayList<PersistentMapEntry<K, V>> entries =
                                    ((SortedCollection<PersistentMapEntry<K, V>>) index).getList(fromKey, toKey);

                            private int i = entries.size();

                            @Override
                            public boolean hasNext() {
                                return i > 0;
                            }

                            @Override
                            public Entry<K, V> next() {
                                if (!hasNext()) {
                                    throw new NoSuchElementException();
                                }
                                return entries.get(--i);
                            }

                            @Override
                            public void remove() {
                                if (i < entries.size() || entries.get(i) == null) {
                                    throw new IllegalStateException();
                                }
                                ((SortedCollection<PersistentMapEntry<K, V>>) index).remove(entries.get(i));
                                entries.set(i, null);
                            }
                        };
                    }
                } else {
                    return new Iterator<Entry<K, V>>() {

                        private final Iterator<Entry<Object, V>> i = ((Index<V>) index).entryIterator(fromKey, toKey,
                                order);

                        @Override
                        public boolean hasNext() {
                            return i.hasNext();
                        }

                        @Override
                        public Entry<K, V> next() {
                            final Entry<Object, V> e = i.next();
                            return new Entry<K, V>() {

                                @Override
                                public K getKey() {
                                    return (K) e.getKey();
                                }

                                @Override
                                public V getValue() {
                                    return e.getValue();
                                }

                                @Override
                                public V setValue(final V value) {
                                    throw new UnsupportedOperationException("Entry.Map.setValue");
                                }
                            };
                        }

                        @Override
                        public void remove() {
                            i.remove();
                        }
                    };
                }
            } else {
                if (order == GenericIndex.ASCENT_ORDER) {
                    final int beg = (from != null ? binarySearch(from) : 0) - 1;
                    final int end = values.size();

                    return new Iterator<Entry<K, V>>() {

                        private int i = beg;

                        @Override
                        public boolean hasNext() {
                            return i + 1 < end && (to == null || ((Comparable[]) keys)[i + 1].compareTo(to) < 0);
                        }

                        @Override
                        public Entry<K, V> next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }
                            i += 1;
                            return new Entry<K, V>() {

                                @Override
                                public K getKey() {
                                    return (K) ((Comparable[]) keys)[i];
                                }

                                @Override
                                public V getValue() {
                                    return values.get(i);
                                }

                                @Override
                                public V setValue(final V value) {
                                    throw new UnsupportedOperationException("Entry.Map.setValue");
                                }
                            };
                        }

                        @Override
                        public void remove() {
                            if (i < 0) {
                                throw new IllegalStateException();
                            }
                            final int size = values.size();
                            System.arraycopy(keys, i + 1, keys, i, size - i - 1);
                            ((Comparable[]) keys)[size - 1] = null;
                            values.removeObject(i);
                            i -= 1;
                        }
                    };
                } else {
                    final int beg = (to != null ? binarySearch(to) : 0) - 1;

                    return new Iterator<Entry<K, V>>() {

                        private int i = beg;

                        @Override
                        public boolean hasNext() {
                            return i > 0 && (from == null || ((Comparable[]) keys)[i - 1].compareTo(from) >= 0);
                        }

                        @Override
                        public Entry<K, V> next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }
                            i -= 1;
                            return new Entry<K, V>() {

                                @Override
                                public K getKey() {
                                    return (K) ((Comparable[]) keys)[i];
                                }

                                @Override
                                public V getValue() {
                                    return values.get(i);
                                }

                                @Override
                                public V setValue(final V value) {
                                    throw new UnsupportedOperationException("Entry.Map.setValue");
                                }
                            };
                        }

                        @Override
                        public void remove() {
                            if (i < 0) {
                                throw new IllegalStateException();
                            }
                            final int size = values.size();
                            System.arraycopy(keys, i + 1, keys, i, size - i - 1);
                            ((Comparable[]) keys)[size - 1] = null;
                            values.removeObject(i);
                        }
                    };
                }
            }
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            if (entrySet == null) {
                entrySet = new AbstractSet<Entry<K, V>>() {

                    @Override
                    public boolean contains(final Object k) {
                        final Entry<K, V> e = (Entry<K, V>) k;
                        if (!inRange(e.getKey())) {
                            return false;
                        }
                        if (e.getValue() != null) {
                            return e.getValue().equals(PersistentMapImpl.this.get(e.getKey()));
                        } else {
                            return PersistentMapImpl.this.containsKey(e.getKey()) && PersistentMapImpl.this.get(e
                                    .getKey()) == null;
                        }
                    }

                    @Override
                    public boolean isEmpty() {
                        return !iterator().hasNext();
                    }

                    @Override
                    public Iterator<Entry<K, V>> iterator() {
                        return entryIterator(GenericIndex.ASCENT_ORDER);
                    }

                    @Override
                    public boolean remove(final Object o) {
                        if (!(o instanceof Map.Entry)) {
                            return false;
                        }
                        final Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
                        final K key = entry.getKey();
                        if (!inRange(key)) {
                            return false;
                        }
                        final V value = entry.getValue();
                        if (value != null) {
                            final V v = PersistentMapImpl.this.get(key);
                            if (value.equals(v)) {
                                PersistentMapImpl.this.remove(key);
                                return true;
                            }
                        } else {
                            if (PersistentMapImpl.this.containsKey(key) && PersistentMapImpl.this.get(key) == null) {
                                PersistentMapImpl.this.remove(key);
                                return true;
                            }
                        }
                        return false;
                    }

                    @Override
                    public int size() {
                        final Iterator<Entry<K, V>> i = iterator();
                        int n;
                        for (n = 0; i.hasNext(); i.next()) {
                            n += 1;
                        }
                        return n;
                    }
                };
            }
            return entrySet;
        }

        @Override
        public K firstKey() {
            return entryIterator(GenericIndex.ASCENT_ORDER).next().getKey();
        }

        @Override
        public V get(final Object key) {
            if (!inRange((K) key)) {
                return null;
            }

            return PersistentMapImpl.this.get(key);
        }

        @Override
        public SortedMap<K, V> headMap(final K to) {
            if (!inRange2(to)) {
                throw new IllegalArgumentException("'to' out of range");
            }

            return new SubMap(this.from, to);
        }

        private boolean inRange(final K key) {
            return (from == null || key.compareTo(from) >= 0) && (to == null || key.compareTo(to) < 0);
        }

        // This form allows the high endpoint (as well as all legit keys)
        private boolean inRange2(final K key) {
            return (from == null || key.compareTo(from) >= 0) && (to == null || key.compareTo(to) <= 0);
        }

        @Override
        public boolean isEmpty() {
            return entrySet().isEmpty();
        }

        @Override
        public K lastKey() {
            return entryIterator(GenericIndex.DESCENT_ORDER).next().getKey();
        }

        @Override
        public V put(final K key, final V value) {
            if (!inRange(key)) {
                throw new IllegalArgumentException("key out of range");
            }

            return PersistentMapImpl.this.put(key, value);
        }

        @Override
        public SortedMap<K, V> subMap(final K from, final K to) {
            if (!inRange2(from)) {
                throw new IllegalArgumentException("'from' out of range");
            }
            if (!inRange2(to)) {
                throw new IllegalArgumentException("'to' out of range");
            }

            return new SubMap(from, to);
        }

        @Override
        public SortedMap<K, V> tailMap(final K from) {
            if (!inRange2(from)) {
                throw new IllegalArgumentException("'from' out of range");
            }

            return new SubMap(from, this.to);
        }
    }

    static final int BTREE_TRESHOLD = 128;

    IPersistent index;

    Object keys;

    Link<V> values;

    int type;

    transient volatile Set<Entry<K, V>> entrySet;

    transient volatile Set<K> keySet;

    transient volatile Collection<V> valuesCol;

    PersistentMapImpl() {
    }

    PersistentMapImpl(final Storage storage, final Class keyType, final int initialSize) {
        super(storage);
        type = getTypeCode(keyType);
        keys = new Comparable[initialSize];
        values = storage.<V>createLink(initialSize);
    }

    private int binarySearch(final Object key) {
        final Comparable[] keys = (Comparable[]) this.keys;
        int l = 0, r = values.size();

        while (l < r) {
            final int i = l + r >> 1;

            if (keys[i].compareTo(key) < 0) {
                l = i + 1;
            } else {
                r = i;
            }
        }

        return r;
    }

    @Override
    public void clear() {
        if (index != null) {
            ((Collection) index).clear();
        } else {
            values.clear();
            keys = new Comparable[((Comparable[]) keys).length];
        }
    }

    @Override
    public Comparator<? super K> comparator() {
        return null;
    }

    @Override
    public boolean containsKey(final Object key) {
        if (index != null) {
            if (type == ClassDescriptor.tpValue) {
                return ((SortedCollection) index).containsKey(key);
            } else {
                final Key k = generateKey(key);
                return ((Index) index).entryIterator(k, k, GenericIndex.ASCENT_ORDER).hasNext();
            }
        } else {
            final int i = binarySearch(key);
            return i < values.size() && ((Comparable[]) keys)[i].equals(key);
        }
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
        if (index != null) {
            if (type == ClassDescriptor.tpValue) {
                return new Iterator<Entry<K, V>>() {

                    private final Iterator<PersistentMapEntry<K, V>> i =
                            ((SortedCollection<PersistentMapEntry<K, V>>) index).iterator();

                    @Override
                    public boolean hasNext() {
                        return i.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        return i.next();
                    }

                    @Override
                    public void remove() {
                        i.remove();
                    }
                };
            } else {
                return new Iterator<Entry<K, V>>() {

                    private final Iterator<Entry<Object, V>> i = ((Index<V>) index).entryIterator();

                    @Override
                    public boolean hasNext() {
                        return i.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        final Entry<Object, V> e = i.next();
                        return new Entry<K, V>() {

                            @Override
                            public K getKey() {
                                return (K) e.getKey();
                            }

                            @Override
                            public V getValue() {
                                return e.getValue();
                            }

                            @Override
                            public V setValue(final V value) {
                                throw new UnsupportedOperationException("Entry.Map.setValue");
                            }
                        };
                    }

                    @Override
                    public void remove() {
                        i.remove();
                    }
                };
            }
        } else {
            return new Iterator<Entry<K, V>>() {

                private int i = -1;

                @Override
                public boolean hasNext() {
                    return i + 1 < values.size();
                }

                @Override
                public Entry<K, V> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }

                    i += 1;

                    return new Entry<K, V>() {

                        @Override
                        public K getKey() {
                            return (K) ((Comparable[]) keys)[i];
                        }

                        @Override
                        public V getValue() {
                            return values.get(i);
                        }

                        @Override
                        public V setValue(final V value) {
                            throw new UnsupportedOperationException("Entry.Map.setValue");
                        }
                    };
                }

                @Override
                public void remove() {
                    if (i < 0) {
                        throw new IllegalStateException();
                    }

                    final int size = values.size();

                    System.arraycopy(keys, i + 1, keys, i, size - i - 1);
                    ((Comparable[]) keys)[size - 1] = null;
                    values.removeObject(i);
                    i -= 1;
                }
            };
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new AbstractSet<Entry<K, V>>() {

                @Override
                public boolean contains(final Object k) {
                    final Entry<K, V> e = (Entry<K, V>) k;

                    if (e.getValue() != null) {
                        return e.getValue().equals(PersistentMapImpl.this.get(e.getKey()));
                    } else {
                        return PersistentMapImpl.this.containsKey(e.getKey()) && PersistentMapImpl.this.get(e
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
                        final V v = PersistentMapImpl.this.get(key);

                        if (value.equals(v)) {
                            PersistentMapImpl.this.remove(key);
                            return true;
                        }
                    } else {
                        if (PersistentMapImpl.this.containsKey(key) && PersistentMapImpl.this.get(key) == null) {
                            PersistentMapImpl.this.remove(key);
                            return true;
                        }
                    }

                    return false;
                }

                @Override
                public int size() {
                    return PersistentMapImpl.this.size();
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
    public K firstKey() {
        if (index != null) {
            if (type == ClassDescriptor.tpValue) {
                return ((SortedCollection<PersistentMapEntry<K, V>>) index).iterator().next().key;
            } else {
                return (K) ((Index<V>) index).entryIterator().next().getKey();
            }
        } else {
            final Comparable[] keys = (Comparable[]) this.keys;

            if (values.size() == 0) {
                throw new NoSuchElementException();
            }

            return (K) keys[0];
        }
    }

    final Key generateKey(final Object key) {
        return generateKey(key, true);
    }

    final Key generateKey(final Object key, final boolean inclusive) {
        if (key instanceof Integer) {
            return new Key(((Integer) key).intValue(), inclusive);
        } else if (key instanceof Byte) {
            return new Key(((Byte) key).byteValue(), inclusive);
        } else if (key instanceof Character) {
            return new Key(((Character) key).charValue(), inclusive);
        } else if (key instanceof Short) {
            return new Key(((Short) key).shortValue(), inclusive);
        } else if (key instanceof Long) {
            return new Key(((Long) key).longValue(), inclusive);
        } else if (key instanceof Float) {
            return new Key(((Float) key).floatValue(), inclusive);
        } else if (key instanceof Double) {
            return new Key(((Double) key).doubleValue(), inclusive);
        } else if (key instanceof String) {
            return new Key((String) key, inclusive);
        } else if (key instanceof Enum) {
            return new Key((Enum) key, inclusive);
        } else if (key instanceof java.util.Date) {
            return new Key((java.util.Date) key, inclusive);
        } else if (key instanceof IValue) {
            return new Key((IValue) key, inclusive);
        } else {
            return new Key(key, inclusive);
        }
    }

    @Override
    public V get(final Object key) {
        if (index != null) {
            if (type == ClassDescriptor.tpValue) {
                final PersistentMapEntry<K, V> entry = ((SortedCollection<PersistentMapEntry<K, V>>) index).get(key);
                return entry != null ? entry.value : null;
            } else {
                return ((Index<V>) index).get(generateKey(key));
            }
        } else {
            final int i = binarySearch(key);

            if (i < values.size() && ((Comparable[]) keys)[i].equals(key)) {
                return values.get(i);
            }

            return null;
        }
    }

    @Override
    public Entry<K, V> getEntry(final Object key) {
        if (index != null) {
            if (type == ClassDescriptor.tpValue) {
                return ((SortedCollection<PersistentMapEntry<K, V>>) index).get(key);
            } else {
                final V value = ((Index<V>) index).get(generateKey(key));
                return value != null ? new PersistentMapEntry((K) key, value) : null;
            }
        } else {
            final int i = binarySearch(key);

            if (i < values.size() && ((Comparable[]) keys)[i].equals(key)) {
                final V value = values.get(i);
                return value != null ? new PersistentMapEntry((K) key, value) : null;
            }

            return null;
        }
    }

    protected int getTypeCode(final Class c) {
        if (c.equals(byte.class) || c.equals(Byte.class)) {
            return ClassDescriptor.tpByte;
        } else if (c.equals(short.class) || c.equals(Short.class)) {
            return ClassDescriptor.tpShort;
        } else if (c.equals(char.class) || c.equals(Character.class)) {
            return ClassDescriptor.tpChar;
        } else if (c.equals(int.class) || c.equals(Integer.class)) {
            return ClassDescriptor.tpInt;
        } else if (c.equals(long.class) || c.equals(Long.class)) {
            return ClassDescriptor.tpLong;
        } else if (c.equals(float.class) || c.equals(Float.class)) {
            return ClassDescriptor.tpFloat;
        } else if (c.equals(double.class) || c.equals(Double.class)) {
            return ClassDescriptor.tpDouble;
        } else if (c.equals(String.class)) {
            return ClassDescriptor.tpString;
        } else if (c.equals(boolean.class) || c.equals(Boolean.class)) {
            return ClassDescriptor.tpBoolean;
        } else if (c.isEnum()) {
            return ClassDescriptor.tpEnum;
        } else if (c.equals(java.util.Date.class)) {
            return ClassDescriptor.tpDate;
        } else if (IValue.class.isAssignableFrom(c)) {
            return ClassDescriptor.tpValue;
        } else {
            return ClassDescriptor.tpObject;
        }
    }

    @Override
    public int hashCode() {
        final Iterator<Entry<K, V>> i = entrySet().iterator();
        int h = 0;

        while (i.hasNext()) {
            h += i.next().hashCode();
        }

        return h;
    }

    @Override
    public SortedMap<K, V> headMap(final K to) {
        return new SubMap(null, to);
    }

    private V insertInSortedCollection(final K key, final V value) {
        final SortedCollection<PersistentMapEntry<K, V>> col = (SortedCollection<PersistentMapEntry<K, V>>) index;
        final PersistentMapEntry<K, V> entry = col.get(key);
        V prev = null;

        getStorage().makePersistent(value);

        if (entry == null) {
            col.add(new PersistentMapEntry(key, value));
        } else {
            prev = entry.setValue(value);
        }

        return prev;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = new AbstractSet<K>() {

                @Override
                public boolean contains(final Object k) {
                    return PersistentMapImpl.this.containsKey(k);
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
                    return PersistentMapImpl.this.size();
                }
            };
        }

        return keySet;
    }

    @Override
    public K lastKey() {
        if (index != null) {
            if (type == ClassDescriptor.tpValue) {
                final ArrayList<PersistentMapEntry<K, V>> entries =
                        ((SortedCollection<PersistentMapEntry<K, V>>) index).getList(null, null);
                return entries.get(entries.size() - 1).key;
            } else {
                return (K) ((Index<V>) index).entryIterator(null, null, GenericIndex.DESCENT_ORDER).next().getKey();
            }
        } else {
            final int size = values.size();

            if (size == 0) {
                throw new NoSuchElementException();
            }

            return (K) ((Comparable[]) keys)[size - 1];
        }
    }

    @Override
    public V put(final K key, final V value) {
        V prev = null;

        if (index == null) {
            final int size = values.size();
            int i = binarySearch(key);

            if (i < size && key.equals(((Comparable[]) keys)[i])) {
                prev = values.set(i, value);
            } else {
                if (size == BTREE_TRESHOLD) {
                    final Comparable[] keys = (Comparable[]) this.keys;

                    if (type == ClassDescriptor.tpValue) {
                        final SortedCollection<PersistentMapEntry<K, V>> col = getStorage()
                                .<PersistentMapEntry<K, V>>createSortedCollection(new PersistentMapComparator<K, V>(),
                                        true);
                        index = col;

                        for (i = 0; i < size; i++) {
                            col.add(new PersistentMapEntry(keys[i], values.get(i)));
                        }

                        prev = insertInSortedCollection(key, value);
                    } else {
                        final Index<V> idx = getStorage().<V>createIndex(Btree.mapKeyType(type), true);
                        index = idx;

                        for (i = 0; i < size; i++) {
                            idx.set(generateKey(keys[i]), values.get(i));
                        }

                        prev = idx.set(generateKey(key), value);
                    }

                    this.keys = null;
                    this.values = null;
                    modify();
                } else {
                    final Object[] oldKeys = (Object[]) keys;

                    if (size >= oldKeys.length) {
                        final Comparable[] newKeys = new Comparable[size + 1 > oldKeys.length * 2 ? size + 1
                                : oldKeys.length * 2];

                        System.arraycopy(oldKeys, 0, newKeys, 0, i);
                        System.arraycopy(oldKeys, i, newKeys, i + 1, size - i);

                        keys = newKeys;
                        newKeys[i] = key;
                    } else {
                        System.arraycopy(oldKeys, i, oldKeys, i + 1, size - i);
                        oldKeys[i] = key;
                    }

                    values.insert(i, value);
                }
            }
        } else {
            if (type == ClassDescriptor.tpValue) {
                prev = insertInSortedCollection(key, value);
            } else {
                prev = ((Index<V>) index).set(generateKey(key), value);
            }
        }

        return prev;
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
        if (index == null) {
            final int size = values.size();
            final int i = binarySearch(key);

            if (i < size && ((Comparable[]) keys)[i].equals(key)) {
                System.arraycopy(keys, i + 1, keys, i, size - i - 1);
                ((Comparable[]) keys)[size - 1] = null;
                return values.remove(i);
            }

            return null;
        } else {
            if (type == ClassDescriptor.tpValue) {
                final SortedCollection<PersistentMapEntry<K, V>> col =
                        (SortedCollection<PersistentMapEntry<K, V>>) index;
                final PersistentMapEntry<K, V> entry = col.get(key);

                if (entry == null) {
                    return null;
                }

                col.remove(entry);
                return entry.value;
            } else {
                try {
                    return ((Index<V>) index).remove(generateKey(key));
                } catch (final StorageError x) {
                    if (x.getErrorCode() == StorageError.KEY_NOT_FOUND) {
                        return null;
                    }

                    throw x;
                }
            }
        }
    }

    @Override
    public int size() {
        return index != null ? ((Collection) index).size() : values.size();
    }

    @Override
    public SortedMap<K, V> subMap(final K from, final K to) {
        if (from.compareTo(to) > 0) {
            throw new IllegalArgumentException("from > to");
        }

        return new SubMap(from, to);
    }

    @Override
    public SortedMap<K, V> tailMap(final K from) {
        return new SubMap(from, null);
    }

    @Override
    public String toString() {
        final Iterator<Entry<K, V>> i = entrySet().iterator();
        final StringBuffer buf = new StringBuffer();
        boolean hasNext = i.hasNext();

        buf.append("{");

        while (hasNext) {
            final Entry<K, V> e = i.next();
            final K key = e.getKey();
            final V value = e.getValue();

            if (key == this) {
                buf.append("(this Map)");
            } else {
                buf.append(key);
            }

            buf.append("=");

            if (value == this) {
                buf.append("(this Map)");
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
                    return PersistentMapImpl.this.containsValue(v);
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
                    return PersistentMapImpl.this.size();
                }
            };
        }

        return valuesCol;
    }
}
