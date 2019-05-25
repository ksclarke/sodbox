
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.IPersistent;
import info.freelibrary.sodbox.IPersistentSet;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.Relation;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

class ThickIndex<T> extends PersistentCollection<T> implements Index<T> {

    static final int BTREE_THRESHOLD = 128;

    Index<Object> myIndex;

    int myIndexSize;

    ThickIndex(final StorageImpl aStorage, final Class aKeyType) {
        super(aStorage);

        myIndex = aStorage.<Object>createIndex(aKeyType, true);
    }

    ThickIndex() {
    }

    private T getFromRelation(final Object aObj) {
        if (aObj == null) {
            return null;
        }

        if (aObj instanceof Relation) {
            final Relation relation = (Relation) aObj;

            if (relation.size() == 1) {
                return (T) relation.get(0);
            }
        }

        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    @Override
    public T get(final Key aKey) {
        return getFromRelation(myIndex.get(aKey));
    }

    @Override
    public T get(final Object aKey) {
        return getFromRelation(myIndex.get(aKey));
    }

    @Override
    public ArrayList<T> getList(final Key aFrom, final Key aTo) {
        return extendList(myIndex.getList(aFrom, aTo));
    }

    @Override
    public ArrayList<T> getList(final Object aFrom, final Object aTo) {
        return extendList(myIndex.getList(aFrom, aTo));
    }

    @Override
    public Object[] get(final Key aFrom, final Key aTo) {
        return extend(myIndex.get(aFrom, aTo));
    }

    @Override
    public Object[] get(final Object aFrom, final Object aTo) {
        return extend(myIndex.get(aFrom, aTo));
    }

    private ArrayList<T> extendList(final ArrayList aList) {
        final ArrayList<T> list = new ArrayList<>();

        for (int index = 0, count = aList.size(); index < count; index++) {
            list.addAll((Collection<T>) aList.get(index));
        }

        return list;
    }

    protected Object[] extend(final Object[] aObjArray) {
        final ArrayList list = new ArrayList();

        for (int index = 0; index < aObjArray.length; index++) {
            list.addAll((Collection) aObjArray[index]);
        }

        return list.toArray();
    }

    public T get(final String aKey) {
        return get(new Key(aKey));
    }

    @Override
    public Object[] getPrefix(final String aPrefix) {
        return extend(myIndex.getPrefix(aPrefix));
    }

    @Override
    public ArrayList<T> getPrefixList(final String aPrefix) {
        return extendList(myIndex.getPrefixList(aPrefix));
    }

    @Override
    public Object[] prefixSearch(final String aWord) {
        return extend(myIndex.prefixSearch(aWord));
    }

    @Override
    public ArrayList<T> prefixSearchList(final String aWord) {
        return extendList(myIndex.prefixSearchList(aWord));
    }

    @Override
    public int size() {
        return myIndexSize;
    }

    @Override
    public void clear() {
        for (final Object obj : myIndex) {
            ((IPersistent) obj).deallocate();
        }

        myIndex.clear();
        myIndexSize = 0;

        modify();
    }

    @Override
    public Object[] toArray() {
        return extend(myIndex.toArray());
    }

    @Override
    public <E> E[] toArray(final E[] aArray) {
        final ArrayList<E> list = new ArrayList<>();

        for (final Object obj : myIndex) {
            list.addAll((Collection<E>) obj);
        }

        return list.toArray(aArray);
    }

    @Override
    public Iterator<T> iterator() {
        return new ExtendIterator<>(myIndex.iterator());
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator() {
        return new ExtendEntryIterator<>(myIndex.entryIterator());
    }

    @Override
    public IterableIterator<T> iterator(final Key aFrom, final Key aTo, final int aOrder) {
        return new ExtendIterator<>(myIndex.iterator(aFrom, aTo, aOrder));
    }

    @Override
    public IterableIterator<T> iterator(final Object aFrom, final Object aTo, final int aOrder) {
        return new ExtendIterator<>(myIndex.iterator(aFrom, aTo, aOrder));
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key aFrom, final Key aTo, final int aOrder) {
        return new ExtendEntryIterator<>(myIndex.entryIterator(aFrom, aTo, aOrder));
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Object aFrom, final Object aTo,
            final int aOrder) {
        return new ExtendEntryIterator<>(myIndex.entryIterator(aFrom, aTo, aOrder));
    }

    @Override
    public IterableIterator<T> prefixIterator(final String aPrefix) {
        return prefixIterator(aPrefix, ASCENT_ORDER);
    }

    @Override
    public IterableIterator<T> prefixIterator(final String aPrefix, final int aOrder) {
        return new ExtendIterator<>(myIndex.prefixIterator(aPrefix, aOrder));
    }

    @Override
    public Class getKeyType() {
        return myIndex.getKeyType();
    }

    @Override
    public Class[] getKeyTypes() {
        return new Class[] { getKeyType() };
    }

    @Override
    public boolean put(final Key aKey, final T aObj) {
        final Object obj = myIndex.get(aKey);
        final Storage storage = getStorage();

        int oid = storage.getOid(aObj);

        if (oid == 0) {
            oid = storage.makePersistent(aObj);
        }

        if (obj == null) {
            final Relation<T, ThickIndex> relation = storage.<T, ThickIndex>createRelation(null);

            relation.add(aObj);
            myIndex.put(aKey, relation);
        } else if (obj instanceof Relation) {
            final Relation relation = (Relation) obj;

            if (relation.size() == BTREE_THRESHOLD) {
                final IPersistentSet<T> persistentSet = storage.<T>createBag();

                for (int index = 0; index < BTREE_THRESHOLD; index++) {
                    persistentSet.add((T) relation.get(index));
                }

                Assert.that(persistentSet.add(aObj));

                myIndex.set(aKey, persistentSet);
                relation.deallocate();
            } else {
                final int count = relation.size();

                int left = 0;
                int right = count;

                while (left < right) {
                    final int m = left + right >>> 1;

                    if (storage.getOid(relation.getRaw(m)) <= oid) {
                        left = m + 1;
                    } else {
                        right = m;
                    }
                }

                relation.insert(right, aObj);
            }
        } else {
            Assert.that(((IPersistentSet<T>) obj).add(aObj));
        }

        myIndexSize += 1;
        modify();
        return true;
    }

    @Override
    public T set(final Key aKey, final T aObj) {
        final Object obj = myIndex.get(aKey);
        final Storage storage = getStorage();

        int oid = storage.getOid(aObj);

        if (oid == 0) {
            oid = storage.makePersistent(aObj);
        }

        if (obj == null) {
            final Relation<T, ThickIndex> relation = storage.<T, ThickIndex>createRelation(null);

            relation.add(aObj);
            myIndex.put(aKey, relation);
            myIndexSize += 1;

            modify();

            return null;
        } else if (obj instanceof Relation) {
            final Relation relation = (Relation) obj;

            if (relation.size() == 1) {
                final Object prev = relation.get(0);

                relation.set(0, aObj);

                return (T) prev;
            }
        }

        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    @Override
    public boolean unlink(final Key aKey, final T aObj) {
        return removeIfExists(aKey, aObj);
    }

    boolean removeIfExists(final Key aKey, final T aObj) {
        final Object obj = myIndex.get(aKey);

        if (obj instanceof Relation) {
            final Relation relation = (Relation) obj;
            final Storage storage = getStorage();
            final int oid = storage.getOid(aObj);
            final int relationCount = relation.size();

            int left = 0;
            int right = relationCount;

            while (left < right) {
                final int m = left + right >>> 1;

                if (storage.getOid(relation.getRaw(m)) < oid) {
                    left = m + 1;
                } else {
                    right = m;
                }
            }

            if (right < relationCount && storage.getOid(relation.getRaw(right)) == oid) {
                relation.remove(right);

                if (relation.size() == 0) {
                    myIndex.remove(aKey, relation);
                    relation.deallocate();
                }

                myIndexSize -= 1;

                modify();

                return true;
            }
        } else if (obj instanceof IPersistentSet) {
            final IPersistentSet persistentSet = (IPersistentSet) obj;

            if (persistentSet.remove(aObj)) {
                if (persistentSet.size() == 0) {
                    myIndex.remove(aKey, persistentSet);
                    persistentSet.deallocate();
                }

                myIndexSize -= 1;

                modify();

                return true;
            }
        }

        return false;
    }

    @Override
    public void remove(final Key aKey, final T aObj) {
        if (!removeIfExists(aKey, aObj)) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
    }

    @Override
    public T remove(final Key aKey) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    @Override
    public boolean put(final Object aKey, final T aObj) {
        return put(Btree.getKeyFromObject(aKey), aObj);
    }

    @Override
    public T set(final Object aKey, final T aObj) {
        return set(Btree.getKeyFromObject(aKey), aObj);
    }

    @Override
    public void remove(final Object aKey, final T aObj) {
        remove(Btree.getKeyFromObject(aKey), aObj);
    }

    @Override
    public T remove(final String aKey) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    @Override
    public T removeKey(final Object aKey) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    @Override
    public void deallocate() {
        clear();

        myIndex.deallocate();

        super.deallocate();
    }

    @Override
    public int indexOf(final Key aKey) {
        final PersistentIterator iterator = (PersistentIterator) iterator(null, aKey, DESCENT_ORDER);

        int index;

        for (index = -1; iterator.nextOID() != 0; index++) {

        }

        return index;
    }

    @Override
    public T getAt(final int aIndex) {
        final IterableIterator<Map.Entry<Object, T>> iterator;

        int index = aIndex;

        if (index < 0 || index >= myIndexSize) {
            throw new IndexOutOfBoundsException("Position " + index + ", index size " + myIndexSize);
        }

        if (index <= myIndexSize / 2) {
            iterator = entryIterator(null, null, ASCENT_ORDER);

            while (--index >= 0) {
                iterator.next();
            }
        } else {
            iterator = entryIterator(null, null, DESCENT_ORDER);
            index -= myIndexSize;

            while (++index < 0) {
                iterator.next();
            }
        }

        return iterator.next().getValue();
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final int aStart, final int aOrder) {
        return new ExtendEntryStartFromIterator(aStart, aOrder);
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    static class ExtendIterator<E> extends IterableIterator<E> implements PersistentIterator {

        private Iterator myOuter;

        private Iterator<E> myInner;

        ExtendIterator(final IterableIterator<?> aIterable) {
            this(aIterable.iterator());
        }

        ExtendIterator(final Iterator<?> aIterator) {
            myOuter = aIterator;

            if (aIterator.hasNext()) {
                myInner = ((Iterable<E>) aIterator.next()).iterator();
            }
        }

        @Override
        public boolean hasNext() {
            return myInner != null;
        }

        @Override
        public E next() {
            if (myInner == null) {
                throw new NoSuchElementException();
            }

            final E obj = myInner.next();

            if (!myInner.hasNext()) {
                if (myOuter.hasNext()) {
                    myInner = ((Iterable<E>) myOuter.next()).iterator();
                } else {
                    myInner = null;
                }
            }

            return obj;
        }

        @Override
        public int nextOID() {
            if (myInner == null) {
                return 0;
            }

            final int oid = ((PersistentIterator) myInner).nextOID();

            if (!myInner.hasNext()) {
                if (myOuter.hasNext()) {
                    myInner = ((Iterable<E>) myOuter.next()).iterator();
                } else {
                    myInner = null;
                }
            }

            return oid;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    static class ExtendEntry<E> implements Map.Entry<Object, E> {

        private final Object myKey;

        private final E myValue;

        ExtendEntry(final Object aKey, final E aValue) {
            myKey = aKey;
            myValue = aValue;
        }

        @Override
        public Object getKey() {
            return myKey;
        }

        @Override
        public E getValue() {
            return myValue;
        }

        @Override
        public E setValue(final E aValue) {
            throw new UnsupportedOperationException();
        }

    }

    static class ExtendEntryIterator<E> extends IterableIterator<Map.Entry<Object, E>> {

        private final Iterator myOuter;

        private Iterator<E> myInner;

        private Object myKey;

        ExtendEntryIterator(final IterableIterator<?> aIterator) {
            myOuter = aIterator;

            if (aIterator.hasNext()) {
                final Map.Entry entry = (Map.Entry) aIterator.next();

                myKey = entry.getKey();
                myInner = ((Iterable<E>) entry.getValue()).iterator();
            }
        }

        @Override
        public boolean hasNext() {
            return myInner != null;
        }

        @Override
        public Map.Entry<Object, E> next() {
            final ExtendEntry<E> current = new ExtendEntry<>(myKey, myInner.next());

            if (!myInner.hasNext()) {
                if (myOuter.hasNext()) {
                    final Map.Entry entry = (Map.Entry) myOuter.next();

                    myKey = entry.getKey();
                    myInner = ((Iterable<E>) entry.getValue()).iterator();
                } else {
                    myInner = null;
                }
            }

            return current;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    class ExtendEntryStartFromIterator extends ExtendEntryIterator<T> {

        ExtendEntryStartFromIterator(final int aStart, final int aOrder) {
            super(entryIterator(null, null, aOrder));

            int skip = aOrder == ASCENT_ORDER ? aStart : myIndexSize - aStart - 1;

            while (--skip >= 0 && hasNext()) {
                next();
            }
        }
    }

}
