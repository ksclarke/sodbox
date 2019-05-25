
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

    static final int BTREE_TRESHOLD = 128;

    IPersistent myIndex;

    Object myKeys;

    Link<V> myValues;

    int myType;

    transient volatile Set<Entry<K, V>> myEntrySet;

    transient volatile Set<K> myKeySet;

    transient volatile Collection<V> myValuesCollection;

    PersistentMapImpl() {
    }

    PersistentMapImpl(final Storage aStorage, final Class aKeyType, final int aInitialSize) {
        super(aStorage);

        myType = getTypeCode(aKeyType);
        myKeys = new Comparable[aInitialSize];
        myValues = aStorage.<V>createLink(aInitialSize);
    }

    protected int getTypeCode(final Class aClass) {
        if (aClass.equals(byte.class) || aClass.equals(Byte.class)) {
            return ClassDescriptor.TP_BYTE;
        } else if (aClass.equals(short.class) || aClass.equals(Short.class)) {
            return ClassDescriptor.TP_SHORT;
        } else if (aClass.equals(char.class) || aClass.equals(Character.class)) {
            return ClassDescriptor.TP_CHAR;
        } else if (aClass.equals(int.class) || aClass.equals(Integer.class)) {
            return ClassDescriptor.TP_INT;
        } else if (aClass.equals(long.class) || aClass.equals(Long.class)) {
            return ClassDescriptor.TP_LONG;
        } else if (aClass.equals(float.class) || aClass.equals(Float.class)) {
            return ClassDescriptor.TP_FLOAT;
        } else if (aClass.equals(double.class) || aClass.equals(Double.class)) {
            return ClassDescriptor.TP_DOUBLE;
        } else if (aClass.equals(String.class)) {
            return ClassDescriptor.TP_STRING;
        } else if (aClass.equals(boolean.class) || aClass.equals(Boolean.class)) {
            return ClassDescriptor.TP_BOOLEAN;
        } else if (aClass.isEnum()) {
            return ClassDescriptor.TP_ENUM;
        } else if (aClass.equals(java.util.Date.class)) {
            return ClassDescriptor.TP_DATE;
        } else if (IValue.class.isAssignableFrom(aClass)) {
            return ClassDescriptor.TP_VALUE;
        } else {
            return ClassDescriptor.TP_OBJECT;
        }
    }

    @Override
    public int size() {
        return myIndex != null ? ((Collection) myIndex).size() : myValues.size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsValue(final Object aValue) {
        final Iterator<Entry<K, V>> iterator = entrySet().iterator();

        if (aValue == null) {
            while (iterator.hasNext()) {
                final Entry<K, V> e = iterator.next();

                if (e.getValue() == null) {
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

    @SuppressWarnings("unchecked")
    private int binarySearch(final Object aKey) {
        final Comparable[] keys = (Comparable[]) myKeys;

        int right = myValues.size();
        int left = 0;

        while (left < right) {
            final int index = left + right >> 1;

            if (keys[index].compareTo(aKey) < 0) {
                left = index + 1;
            } else {
                right = index;
            }
        }

        return right;
    }

    @Override
    public boolean containsKey(final Object aKey) {
        if (myIndex != null) {
            if (myType == ClassDescriptor.TP_VALUE) {
                return ((SortedCollection) myIndex).containsKey(aKey);
            } else {
                final Key key = generateKey(aKey);
                return ((Index) myIndex).entryIterator(key, key, Index.ASCENT_ORDER).hasNext();
            }
        } else {
            final int index = binarySearch(aKey);
            return index < myValues.size() && ((Comparable[]) myKeys)[index].equals(aKey);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(final Object aKey) {
        if (myIndex != null) {
            if (myType == ClassDescriptor.TP_VALUE) {
                final PersistentMapEntry<K, V> entry;

                entry = ((SortedCollection<PersistentMapEntry<K, V>>) myIndex).get(aKey);

                return entry != null ? entry.myValue : null;
            } else {
                return ((Index<V>) myIndex).get(generateKey(aKey));
            }
        } else {
            final int index = binarySearch(aKey);

            if (index < myValues.size() && ((Comparable[]) myKeys)[index].equals(aKey)) {
                return myValues.get(index);
            }

            return null;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Entry<K, V> getEntry(final Object aKey) {
        if (myIndex != null) {
            if (myType == ClassDescriptor.TP_VALUE) {
                return ((SortedCollection<PersistentMapEntry<K, V>>) myIndex).get(aKey);
            } else {
                final V value = ((Index<V>) myIndex).get(generateKey(aKey));
                return value != null ? new PersistentMapEntry((K) aKey, value) : null;
            }
        } else {
            final int index = binarySearch(aKey);

            if (index < myValues.size() && ((Comparable[]) myKeys)[index].equals(aKey)) {
                final V value = myValues.get(index);
                return value != null ? new PersistentMapEntry((K) aKey, value) : null;
            }

            return null;
        }
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public V put(final K aKey, final V aValue) {
        V previous = null;

        if (myIndex == null) {
            final int size = myValues.size();

            int index = binarySearch(aKey);

            if (index < size && aKey.equals(((Comparable[]) myKeys)[index])) {
                previous = myValues.set(index, aValue);
            } else {
                if (size == BTREE_TRESHOLD) {
                    final Comparable[] keys = (Comparable[]) this.myKeys;

                    if (myType == ClassDescriptor.TP_VALUE) {
                        final SortedCollection<PersistentMapEntry<K, V>> collection;

                        collection = getStorage().<PersistentMapEntry<K, V>>createSortedCollection(
                                new PersistentMapComparator<K, V>(), true);
                        myIndex = collection;

                        for (index = 0; index < size; index++) {
                            collection.add(new PersistentMapEntry(keys[index], myValues.get(index)));
                        }

                        previous = insertInSortedCollection(aKey, aValue);
                    } else {
                        final Index<V> newIndex = getStorage().<V>createIndex(Btree.mapKeyType(myType), true);

                        myIndex = newIndex;

                        for (index = 0; index < size; index++) {
                            newIndex.set(generateKey(keys[index]), myValues.get(index));
                        }

                        previous = newIndex.set(generateKey(aKey), aValue);
                    }

                    myKeys = null;
                    myValues = null;

                    modify();
                } else {
                    final Object[] oldKeys = (Object[]) myKeys;

                    if (size >= oldKeys.length) {
                        final Comparable[] newKeys = new Comparable[size + 1 > oldKeys.length * 2 ? size + 1
                                : oldKeys.length * 2];

                        System.arraycopy(oldKeys, 0, newKeys, 0, index);
                        System.arraycopy(oldKeys, index, newKeys, index + 1, size - index);

                        myKeys = newKeys;
                        newKeys[index] = aKey;
                    } else {
                        System.arraycopy(oldKeys, index, oldKeys, index + 1, size - index);

                        oldKeys[index] = aKey;
                    }

                    myValues.insert(index, aValue);
                }
            }
        } else {
            if (myType == ClassDescriptor.TP_VALUE) {
                previous = insertInSortedCollection(aKey, aValue);
            } else {
                previous = ((Index<V>) myIndex).set(generateKey(aKey), aValue);
            }
        }

        return previous;
    }

    @SuppressWarnings("unchecked")
    private V insertInSortedCollection(final K aKey, final V aValue) {
        final SortedCollection<PersistentMapEntry<K, V>> collection =
                (SortedCollection<PersistentMapEntry<K, V>>) myIndex;
        final PersistentMapEntry<K, V> entry = collection.get(aKey);

        V previous = null;

        getStorage().makePersistent(aValue);

        if (entry == null) {
            collection.add(new PersistentMapEntry(aKey, aValue));
        } else {
            previous = entry.setValue(aValue);
        }

        return previous;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(final Object aKey) {
        if (myIndex == null) {
            final int size = myValues.size();
            final int index = binarySearch(aKey);

            if (index < size && ((Comparable[]) myKeys)[index].equals(aKey)) {
                System.arraycopy(myKeys, index + 1, myKeys, index, size - index - 1);

                ((Comparable[]) myKeys)[size - 1] = null;

                return myValues.remove(index);
            }

            return null;
        } else {
            if (myType == ClassDescriptor.TP_VALUE) {
                final SortedCollection<PersistentMapEntry<K, V>> collection =
                        (SortedCollection<PersistentMapEntry<K, V>>) myIndex;
                final PersistentMapEntry<K, V> entry = collection.get(aKey);

                if (entry == null) {
                    return null;
                }

                collection.remove(entry);
                return entry.myValue;
            } else {
                try {
                    return ((Index<V>) myIndex).remove(generateKey(aKey));
                } catch (final StorageError details) {
                    if (details.getErrorCode() == StorageError.KEY_NOT_FOUND) {
                        return null;
                    }

                    throw details;
                }
            }
        }
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> aMap) {
        final Iterator<? extends Entry<? extends K, ? extends V>> iterator = aMap.entrySet().iterator();

        while (iterator.hasNext()) {
            final Entry<? extends K, ? extends V> entry = iterator.next();

            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        if (myIndex != null) {
            ((Collection) myIndex).clear();
        } else {
            myValues.clear();
            myKeys = new Comparable[((Comparable[]) myKeys).length];
        }
    }

    @Override
    public Set<K> keySet() {
        if (myKeySet == null) {
            myKeySet = new AbstractSet<K>() {

                @Override
                public Iterator<K> iterator() {
                    return new Iterator<K>() {

                        private final Iterator<Entry<K, V>> myIterator = entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return myIterator.hasNext();
                        }

                        @Override
                        public K next() {
                            return myIterator.next().getKey();
                        }

                        @Override
                        public void remove() {
                            myIterator.remove();
                        }
                    };
                }

                @Override
                public int size() {
                    return PersistentMapImpl.this.size();
                }

                @Override
                public boolean contains(final Object aKey) {
                    return PersistentMapImpl.this.containsKey(aKey);
                }
            };
        }

        return myKeySet;
    }

    @Override
    public Collection<V> values() {
        if (myValuesCollection == null) {
            myValuesCollection = new AbstractCollection<V>() {

                @Override
                public Iterator<V> iterator() {
                    return new Iterator<V>() {

                        private final Iterator<Entry<K, V>> myIterator = entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return myIterator.hasNext();
                        }

                        @Override
                        public V next() {
                            return myIterator.next().getValue();
                        }

                        @Override
                        public void remove() {
                            myIterator.remove();
                        }
                    };
                }

                @Override
                public int size() {
                    return PersistentMapImpl.this.size();
                }

                @Override
                public boolean contains(final Object aValue) {
                    return PersistentMapImpl.this.containsValue(aValue);
                }
            };
        }

        return myValuesCollection;
    }

    protected Iterator<Entry<K, V>> entryIterator() {
        if (myIndex != null) {
            if (myType == ClassDescriptor.TP_VALUE) {
                return new Iterator<Entry<K, V>>() {

                    @SuppressWarnings("unchecked")
                    private final Iterator<PersistentMapEntry<K, V>> myIterator =
                            ((SortedCollection<PersistentMapEntry<K, V>>) myIndex).iterator();

                    @Override
                    public boolean hasNext() {
                        return myIterator.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        return myIterator.next();
                    }

                    @Override
                    public void remove() {
                        myIterator.remove();
                    }
                };
            } else {
                return new Iterator<Entry<K, V>>() {

                    @SuppressWarnings("unchecked")
                    private final Iterator<Entry<Object, V>> myIterator = ((Index<V>) myIndex).entryIterator();

                    @Override
                    public boolean hasNext() {
                        return myIterator.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        final Entry<Object, V> entry = myIterator.next();
                        return new Entry<K, V>() {

                            @SuppressWarnings("unchecked")
                            @Override
                            public K getKey() {
                                return (K) entry.getKey();
                            }

                            @Override
                            public V getValue() {
                                return entry.getValue();
                            }

                            @Override
                            public V setValue(final V aValue) {
                                throw new UnsupportedOperationException("  Entry.Map.setValue ");
                            }
                        };
                    }

                    @Override
                    public void remove() {
                        myIterator.remove();
                    }
                };
            }
        } else {
            return new Iterator<Entry<K, V>>() {

                private int myIndex = -1;

                @Override
                public boolean hasNext() {
                    return myIndex + 1 < myValues.size();
                }

                @Override
                public Entry<K, V> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }

                    myIndex += 1;

                    return new Entry<K, V>() {

                        @SuppressWarnings("unchecked")
                        @Override
                        public K getKey() {
                            return (K) ((Comparable[]) myKeys)[myIndex];
                        }

                        @Override
                        public V getValue() {
                            return myValues.get(myIndex);
                        }

                        @Override
                        public V setValue(final V aValue) {
                            throw new UnsupportedOperationException(" Entry.Map.setValue");
                        }
                    };
                }

                @Override
                public void remove() {
                    if (myIndex < 0) {
                        throw new IllegalStateException();
                    }

                    final int size = myValues.size();

                    System.arraycopy(myKeys, myIndex + 1, myKeys, myIndex, size - myIndex - 1);

                    ((Comparable[]) myKeys)[size - 1] = null;

                    myValues.removeObject(myIndex);
                    myIndex -= 1;
                }
            };
        }
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
                    return PersistentMapImpl.this.size();
                }

                @SuppressWarnings({ "unchecked" })
                @Override
                public boolean remove(final Object aObject) {
                    if (!(aObject instanceof Map.Entry)) {
                        return false;
                    }

                    final Map.Entry<K, V> entry = (Map.Entry<K, V>) aObject;
                    final K key = entry.getKey();
                    final V value = entry.getValue();

                    if (value != null) {
                        final V mapValue = PersistentMapImpl.this.get(key);

                        if (value.equals(mapValue)) {
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

                @SuppressWarnings("unchecked")
                @Override
                public boolean contains(final Object aKey) {
                    final Entry<K, V> entry = (Entry<K, V>) aKey;

                    if (entry.getValue() != null) {
                        return entry.getValue().equals(PersistentMapImpl.this.get(entry.getKey()));
                    } else {
                        return PersistentMapImpl.this.containsKey(entry.getKey()) && PersistentMapImpl.this.get(entry
                                .getKey()) == null;
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
                final Entry<K, V> entry = iterator.next();
                final K key = entry.getKey();
                final V value = entry.getValue();

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
        } catch (final ClassCastException unused) {
            return false;
        } catch (final NullPointerException unused) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        final Iterator<Entry<K, V>> iterator = entrySet().iterator();

        int hashCode = 0;

        while (iterator.hasNext()) {
            hashCode += iterator.next().hashCode();
        }

        return hashCode;
    }

    @Override
    public String toString() {
        final Iterator<Entry<K, V>> iterator = entrySet().iterator();
        final StringBuffer buffer = new StringBuffer();

        boolean hasNext = iterator.hasNext();

        buffer.append("{");

        while (hasNext) {
            final Entry<K, V> entry = iterator.next();
            final K key = entry.getKey();
            final V value = entry.getValue();

            if (key == this) {
                buffer.append(" (this Map) ");
            } else {
                buffer.append(key);
            }

            buffer.append("=");

            if (value == this) {
                buffer.append("( this Map )");
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

    final Key generateKey(final Object aKey) {
        return generateKey(aKey, true);
    }

    final Key generateKey(final Object aKey, final boolean aInclusive) {
        if (aKey instanceof Integer) {
            return new Key(((Integer) aKey).intValue(), aInclusive);
        } else if (aKey instanceof Byte) {
            return new Key(((Byte) aKey).byteValue(), aInclusive);
        } else if (aKey instanceof Character) {
            return new Key(((Character) aKey).charValue(), aInclusive);
        } else if (aKey instanceof Short) {
            return new Key(((Short) aKey).shortValue(), aInclusive);
        } else if (aKey instanceof Long) {
            return new Key(((Long) aKey).longValue(), aInclusive);
        } else if (aKey instanceof Float) {
            return new Key(((Float) aKey).floatValue(), aInclusive);
        } else if (aKey instanceof Double) {
            return new Key(((Double) aKey).doubleValue(), aInclusive);
        } else if (aKey instanceof String) {
            return new Key((String) aKey, aInclusive);
        } else if (aKey instanceof Enum) {
            return new Key((Enum) aKey, aInclusive);
        } else if (aKey instanceof java.util.Date) {
            return new Key((java.util.Date) aKey, aInclusive);
        } else if (aKey instanceof IValue) {
            return new Key((IValue) aKey, aInclusive);
        } else {
            return new Key(aKey, aInclusive);
        }
    }

    @Override
    public Comparator<? super K> comparator() {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SortedMap<K, V> subMap(final K aFrom, final K aTo) {
        if (aFrom.compareTo(aTo) > 0) {
            throw new IllegalArgumentException("from > to");
        }

        return new SubMap(aFrom, aTo);
    }

    @Override
    public SortedMap<K, V> headMap(final K aTo) {
        return new SubMap(null, aTo);
    }

    @Override
    public SortedMap<K, V> tailMap(final K aFrom) {
        return new SubMap(aFrom, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public K lastKey() {
        if (myIndex != null) {
            if (myType == ClassDescriptor.TP_VALUE) {
                final ArrayList<PersistentMapEntry<K, V>> entries =
                        ((SortedCollection<PersistentMapEntry<K, V>>) myIndex).getList(null, null);
                return entries.get(entries.size() - 1).myKey;
            } else {
                return (K) ((Index<V>) myIndex).entryIterator(null, null, Index.DESCENT_ORDER).next().getKey();
            }
        } else {
            final int size = myValues.size();

            if (size == 0) {
                throw new NoSuchElementException();
            }

            return (K) ((Comparable[]) myKeys)[size - 1];
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public K firstKey() {
        if (myIndex != null) {
            if (myType == ClassDescriptor.TP_VALUE) {
                return ((SortedCollection<PersistentMapEntry<K, V>>) myIndex).iterator().next().myKey;
            } else {
                return (K) ((Index<V>) myIndex).entryIterator().next().getKey();
            }
        } else {
            final Comparable[] keys = (Comparable[]) this.myKeys;

            if (myValues.size() == 0) {
                throw new NoSuchElementException();
            }

            return (K) keys[0];
        }
    }

    private class SubMap extends AbstractMap<K, V> implements SortedMap<K, V> {

        volatile Set<Entry<K, V>> mySubMapEntrySet;

        private final Key myFromKey;

        private final Key myToKey;

        private final K myFrom;

        private final K myTo;

        SubMap(final K aFrom, final K aTo) {
            myFrom = aFrom;
            myTo = aTo;
            myFromKey = aFrom != null ? generateKey(aFrom, true) : null;
            myToKey = aTo != null ? generateKey(aTo, false) : null;
        }

        @Override
        public boolean isEmpty() {
            return entrySet().isEmpty();
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean containsKey(final Object aKey) {
            return inRange((K) aKey) && PersistentMapImpl.this.containsKey(aKey);
        }

        @SuppressWarnings("unchecked")
        @Override
        public V get(final Object aKey) {
            if (!inRange((K) aKey)) {
                return null;
            }

            return PersistentMapImpl.this.get(aKey);
        }

        @Override
        public V put(final K aKey, final V aValue) {
            if (!inRange(aKey)) {
                throw new IllegalArgumentException("key out of range");
            }

            return PersistentMapImpl.this.put(aKey, aValue);
        }

        @Override
        public Comparator<? super K> comparator() {
            return null;
        }

        @Override
        public K firstKey() {
            return entryIterator(Index.ASCENT_ORDER).next().getKey();
        }

        @Override
        public K lastKey() {
            return entryIterator(Index.DESCENT_ORDER).next().getKey();
        }

        protected Iterator<Entry<K, V>> entryIterator(final int aOrder) {
            if (myIndex != null) {
                if (myType == ClassDescriptor.TP_VALUE) {
                    if (aOrder == Index.ASCENT_ORDER) {
                        return new Iterator<Entry<K, V>>() {

                            @SuppressWarnings("unchecked")
                            private final Iterator<PersistentMapEntry<K, V>> myIterator =
                                    ((SortedCollection<PersistentMapEntry<K, V>>) myIndex).iterator(myFromKey,
                                            myToKey);

                            @Override
                            public boolean hasNext() {
                                return myIterator.hasNext();
                            }

                            @Override
                            public Entry<K, V> next() {
                                return myIterator.next();
                            }

                            @Override
                            public void remove() {
                                myIterator.remove();
                            }
                        };
                    } else {
                        return new Iterator<Entry<K, V>>() {

                            @SuppressWarnings("unchecked")
                            private final ArrayList<PersistentMapEntry<K, V>> myEntries =
                                    ((SortedCollection<PersistentMapEntry<K, V>>) myIndex).getList(myFromKey,
                                            myToKey);

                            private int myIterIndex = myEntries.size();

                            @Override
                            public boolean hasNext() {
                                return myIterIndex > 0;
                            }

                            @Override
                            public Entry<K, V> next() {
                                if (!hasNext()) {
                                    throw new NoSuchElementException();
                                }

                                return myEntries.get(--myIterIndex);
                            }

                            @SuppressWarnings("unchecked")
                            @Override
                            public void remove() {
                                if (myIterIndex < myEntries.size() || myEntries.get(myIterIndex) == null) {
                                    throw new IllegalStateException();
                                }

                                ((SortedCollection<PersistentMapEntry<K, V>>) myIndex).remove(myEntries.get(
                                        myIterIndex));

                                myEntries.set(myIterIndex, null);
                            }
                        };
                    }
                } else {
                    return new Iterator<Entry<K, V>>() {

                        @SuppressWarnings("unchecked")
                        private final Iterator<Entry<Object, V>> myIterator = ((Index<V>) myIndex).entryIterator(
                                myFromKey, myToKey, aOrder);

                        @Override
                        public boolean hasNext() {
                            return myIterator.hasNext();
                        }

                        @Override
                        public Entry<K, V> next() {
                            final Entry<Object, V> entry = myIterator.next();
                            return new Entry<K, V>() {

                                @SuppressWarnings("unchecked")
                                @Override
                                public K getKey() {
                                    return (K) entry.getKey();
                                }

                                @Override
                                public V getValue() {
                                    return entry.getValue();
                                }

                                @Override
                                public V setValue(final V aValue) {
                                    throw new UnsupportedOperationException(" Entry.Map.setValue ");
                                }
                            };
                        }

                        @Override
                        public void remove() {
                            myIterator.remove();
                        }
                    };
                }
            } else {
                if (aOrder == Index.ASCENT_ORDER) {
                    final int beg = (myFrom != null ? binarySearch(myFrom) : 0) - 1;
                    final int end = myValues.size();

                    return new Iterator<Entry<K, V>>() {

                        private int myIndex = beg;

                        @SuppressWarnings("unchecked")
                        @Override
                        public boolean hasNext() {
                            return myIndex + 1 < end && (myTo == null || ((Comparable[]) myKeys)[myIndex + 1]
                                    .compareTo(myTo) < 0);
                        }

                        @Override
                        public Entry<K, V> next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }

                            myIndex += 1;

                            return new Entry<K, V>() {

                                @SuppressWarnings("unchecked")
                                @Override
                                public K getKey() {
                                    return (K) ((Comparable[]) myKeys)[myIndex];
                                }

                                @Override
                                public V getValue() {
                                    return myValues.get(myIndex);
                                }

                                @Override
                                public V setValue(final V aValue) {
                                    throw new UnsupportedOperationException("Entry.Map.setValue ");
                                }
                            };
                        }

                        @Override
                        public void remove() {
                            if (myIndex < 0) {
                                throw new IllegalStateException();
                            }

                            final int size = myValues.size();

                            System.arraycopy(myKeys, myIndex + 1, myKeys, myIndex, size - myIndex - 1);

                            ((Comparable[]) myKeys)[size - 1] = null;

                            myValues.removeObject(myIndex);
                            myIndex -= 1;
                        }
                    };
                } else {
                    final int beg = (myTo != null ? binarySearch(myTo) : 0) - 1;

                    return new Iterator<Entry<K, V>>() {

                        private int myIndex = beg;

                        @SuppressWarnings("unchecked")
                        @Override
                        public boolean hasNext() {
                            return myIndex > 0 && (myFrom == null || ((Comparable[]) myKeys)[myIndex - 1].compareTo(
                                    myFrom) >= 0);
                        }

                        @Override
                        public Entry<K, V> next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }

                            myIndex -= 1;

                            return new Entry<K, V>() {

                                @SuppressWarnings({ "unchecked" })
                                @Override
                                public K getKey() {
                                    return (K) ((Comparable[]) myKeys)[myIndex];
                                }

                                @Override
                                public V getValue() {
                                    return myValues.get(myIndex);
                                }

                                @Override
                                public V setValue(final V aValue) {
                                    throw new UnsupportedOperationException("Entry.Map.setValue");
                                }
                            };
                        }

                        @Override
                        public void remove() {
                            if (myIndex < 0) {
                                throw new IllegalStateException();
                            }

                            final int size = myValues.size();

                            System.arraycopy(myKeys, myIndex + 1, myKeys, myIndex, size - myIndex - 1);

                            ((Comparable[]) myKeys)[size - 1] = null;

                            myValues.removeObject(myIndex);
                        }
                    };
                }
            }
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            if (mySubMapEntrySet == null) {
                mySubMapEntrySet = new AbstractSet<Entry<K, V>>() {

                    @Override
                    public Iterator<Entry<K, V>> iterator() {
                        return entryIterator(Index.ASCENT_ORDER);
                    }

                    @Override
                    public int size() {
                        final Iterator<Entry<K, V>> iterator = iterator();

                        int size;

                        for (size = 0; iterator.hasNext(); iterator.next()) {
                            size += 1;
                        }

                        return size;
                    }

                    @Override
                    public boolean isEmpty() {
                        return !iterator().hasNext();
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public boolean remove(final Object aObject) {
                        if (!(aObject instanceof Map.Entry)) {
                            return false;
                        }

                        final Map.Entry<K, V> entry = (Map.Entry<K, V>) aObject;
                        final K key = entry.getKey();

                        if (!inRange(key)) {
                            return false;
                        }

                        final V value = entry.getValue();

                        if (value != null) {
                            final V mapValue = PersistentMapImpl.this.get(key);

                            if (value.equals(mapValue)) {
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

                    @SuppressWarnings("unchecked")
                    @Override
                    public boolean contains(final Object aEntry) {
                        final Entry<K, V> entry = (Entry<K, V>) aEntry;

                        if (!inRange(entry.getKey())) {
                            return false;
                        }

                        if (entry.getValue() != null) {
                            return entry.getValue().equals(PersistentMapImpl.this.get(entry.getKey()));
                        } else {
                            return PersistentMapImpl.this.containsKey(entry.getKey()) && PersistentMapImpl.this.get(
                                    entry.getKey()) == null;
                        }
                    }
                };
            }

            return mySubMapEntrySet;
        }

        @Override
        public SortedMap<K, V> subMap(final K aFrom, final K aTo) {
            if (!inRange2(aFrom)) {
                throw new IllegalArgumentException("'from' out of range ");
            }

            if (!inRange2(aTo)) {
                throw new IllegalArgumentException("'to' out of range ");
            }

            return new SubMap(aFrom, aTo);
        }

        @Override
        public SortedMap<K, V> headMap(final K aTo) {
            if (!inRange2(aTo)) {
                throw new IllegalArgumentException("'to' out of range");
            }

            return new SubMap(this.myFrom, aTo);
        }

        @Override
        public SortedMap<K, V> tailMap(final K aFrom) {
            if (!inRange2(aFrom)) {
                throw new IllegalArgumentException("'from' out of range");
            }

            return new SubMap(aFrom, this.myTo);
        }

        @SuppressWarnings("unchecked")
        private boolean inRange(final K aKey) {
            return (myFrom == null || aKey.compareTo(myFrom) >= 0) && (myTo == null || aKey.compareTo(myTo) < 0);
        }

        // This form allows the high endpoint (as well as all legit keys)
        @SuppressWarnings("unchecked")
        private boolean inRange2(final K aKey) {
            return (myFrom == null || aKey.compareTo(myFrom) >= 0) && (myTo == null || aKey.compareTo(myTo) <= 0);
        }
    }

    static class PersistentMapEntry<K extends Comparable, V> extends Persistent implements Entry<K, V> {

        K myKey;

        V myValue;

        PersistentMapEntry() {
        }

        PersistentMapEntry(final K aKey, final V aValue) {
            myKey = aKey;
            myValue = aValue;
        }

        @Override
        public K getKey() {
            return myKey;
        }

        @Override
        public V getValue() {
            return myValue;
        }

        @Override
        public V setValue(final V aValue) {
            final V previousValue;

            modify();

            previousValue = myValue;
            myValue = aValue;

            return previousValue;
        }

    }

    static class PersistentMapComparator<K extends Comparable, V> extends
            PersistentComparator<PersistentMapEntry<K, V>> {

        @SuppressWarnings("unchecked")
        @Override
        public int compareMembers(final PersistentMapEntry<K, V> aFirstMember,
                final PersistentMapEntry<K, V> aSecondMember) {
            return aFirstMember.myKey.compareTo(aSecondMember.myKey);
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compareMemberWithKey(final PersistentMapEntry<K, V> aMember, final Object aKey) {
            return aMember.myKey.compareTo(aKey);
        }
    }
}
