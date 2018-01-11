
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

    static class ExtendEntry<E> implements Map.Entry<Object, E> {

        private final Object key;

        private final E value;

        ExtendEntry(final Object key, final E value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public E getValue() {
            return value;
        }

        @Override
        public E setValue(final E value) {
            throw new UnsupportedOperationException();
        }
    }

    static class ExtendEntryIterator<E> extends IterableIterator<Map.Entry<Object, E>> {

        private final Iterator outer;

        private Iterator<E> inner;

        private Object key;

        ExtendEntryIterator(final IterableIterator<?> iterator) {
            outer = iterator;
            if (iterator.hasNext()) {
                final Map.Entry entry = (Map.Entry) iterator.next();
                key = entry.getKey();
                inner = ((Iterable<E>) entry.getValue()).iterator();
            }
        }

        @Override
        public boolean hasNext() {
            return inner != null;
        }

        @Override
        public Map.Entry<Object, E> next() {
            final ExtendEntry<E> curr = new ExtendEntry<E>(key, inner.next());
            if (!inner.hasNext()) {
                if (outer.hasNext()) {
                    final Map.Entry entry = (Map.Entry) outer.next();
                    key = entry.getKey();
                    inner = ((Iterable<E>) entry.getValue()).iterator();
                } else {
                    inner = null;
                }
            }
            return curr;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    class ExtendEntryStartFromIterator extends ExtendEntryIterator<T> {

        ExtendEntryStartFromIterator(final int start, final int order) {
            super(entryIterator(null, null, order));
            int skip = order == ASCENT_ORDER ? start : nElems - start - 1;
            while (--skip >= 0 && hasNext()) {
                next();
            }
        }
    }

    static class ExtendIterator<E> extends IterableIterator<E> implements PersistentIterator {

        private Iterator outer;

        private Iterator<E> inner;

        ExtendIterator(final IterableIterator<?> iterable) {
            this(iterable.iterator());
        }

        ExtendIterator(final Iterator<?> iterator) {
            outer = iterator;
            if (iterator.hasNext()) {
                inner = ((Iterable<E>) iterator.next()).iterator();
            }
        }

        @Override
        public boolean hasNext() {
            return inner != null;
        }

        @Override
        public E next() {
            if (inner == null) {
                throw new NoSuchElementException();
            }
            final E obj = inner.next();
            if (!inner.hasNext()) {
                if (outer.hasNext()) {
                    inner = ((Iterable<E>) outer.next()).iterator();
                } else {
                    inner = null;
                }
            }
            return obj;
        }

        @Override
        public int nextOid() {
            if (inner == null) {
                return 0;
            }
            final int oid = ((PersistentIterator) inner).nextOid();
            if (!inner.hasNext()) {
                if (outer.hasNext()) {
                    outer.next();
                    inner = ((Iterable<E>) outer.next()).iterator();
                } else {
                    inner = null;
                }
            }
            return oid;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    static final int BTREE_THRESHOLD = 128;

    Index<Object> index;

    int nElems;

    ThickIndex() {
    }

    ThickIndex(final StorageImpl db, final Class keyType) {
        super(db);
        index = db.<Object>createIndex(keyType, true);
    }

    @Override
    public void clear() {
        for (final Object p : index) {
            ((IPersistent) p).deallocate();
        }
        index.clear();
        nElems = 0;
        modify();
    }

    @Override
    public void deallocate() {
        clear();
        index.deallocate();
        super.deallocate();
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator() {
        return new ExtendEntryIterator<T>(index.entryIterator());
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final int start, final int order) {
        return new ExtendEntryStartFromIterator(start, order);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key from, final Key till, final int order) {
        return new ExtendEntryIterator<T>(index.entryIterator(from, till, order));
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Object from, final Object till,
            final int order) {
        return new ExtendEntryIterator<T>(index.entryIterator(from, till, order));
    }

    protected Object[] extend(final Object[] s) {
        final ArrayList list = new ArrayList();
        for (final Object element : s) {
            list.addAll((Collection) element);
        }
        return list.toArray();
    }

    private ArrayList<T> extendList(final ArrayList s) {
        final ArrayList<T> list = new ArrayList<T>();
        for (int i = 0, n = s.size(); i < n; i++) {
            list.addAll((Collection<T>) s.get(i));
        }
        return list;
    }

    @Override
    public T get(final Key key) {
        return getFromRelation(index.get(key));
    }

    @Override
    public Object[] get(final Key from, final Key till) {
        return extend(index.get(from, till));
    }

    @Override
    public T get(final Object key) {
        return getFromRelation(index.get(key));
    }

    @Override
    public Object[] get(final Object from, final Object till) {
        return extend(index.get(from, till));
    }

    public T get(final String key) {
        return get(new Key(key));
    }

    @Override
    public T getAt(int i) {
        IterableIterator<Map.Entry<Object, T>> iterator;
        if (i < 0 || i >= nElems) {
            throw new IndexOutOfBoundsException("Position " + i + ", index size " + nElems);
        }
        if (i <= nElems / 2) {
            iterator = entryIterator(null, null, ASCENT_ORDER);
            while (--i >= 0) {
                iterator.next();
            }
        } else {
            iterator = entryIterator(null, null, DESCENT_ORDER);
            i -= nElems;
            while (++i < 0) {
                iterator.next();
            }
        }
        return iterator.next().getValue();
    }

    private final T getFromRelation(final Object s) {
        if (s == null) {
            return null;
        }
        if (s instanceof Relation) {
            final Relation r = (Relation) s;
            if (r.size() == 1) {
                return (T) r.get(0);
            }
        }
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    @Override
    public Class getKeyType() {
        return index.getKeyType();
    }

    @Override
    public Class[] getKeyTypes() {
        return new Class[] { getKeyType() };
    }

    @Override
    public ArrayList<T> getList(final Key from, final Key till) {
        return extendList(index.getList(from, till));
    }

    @Override
    public ArrayList<T> getList(final Object from, final Object till) {
        return extendList(index.getList(from, till));
    }

    @Override
    public Object[] getPrefix(final String prefix) {
        return extend(index.getPrefix(prefix));
    }

    @Override
    public ArrayList<T> getPrefixList(final String prefix) {
        return extendList(index.getPrefixList(prefix));
    }

    @Override
    public int indexOf(final Key key) {
        final PersistentIterator iterator = (PersistentIterator) iterator(null, key, DESCENT_ORDER);
        int i;
        for (i = -1; iterator.nextOid() != 0; i++) {
            ;
        }
        return i;
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public Iterator<T> iterator() {
        return new ExtendIterator<T>(index.iterator());
    }

    @Override
    public IterableIterator<T> iterator(final Key from, final Key till, final int order) {
        return new ExtendIterator<T>(index.iterator(from, till, order));
    }

    @Override
    public IterableIterator<T> iterator(final Object from, final Object till, final int order) {
        return new ExtendIterator<T>(index.iterator(from, till, order));
    }

    @Override
    public IterableIterator<T> prefixIterator(final String prefix) {
        return prefixIterator(prefix, ASCENT_ORDER);
    }

    @Override
    public IterableIterator<T> prefixIterator(final String prefix, final int order) {
        return new ExtendIterator<T>(index.prefixIterator(prefix, order));
    }

    @Override
    public Object[] prefixSearch(final String word) {
        return extend(index.prefixSearch(word));
    }

    @Override
    public ArrayList<T> prefixSearchList(final String word) {
        return extendList(index.prefixSearchList(word));
    }

    @Override
    public boolean put(final Key key, final T obj) {
        final Object s = index.get(key);
        final Storage storage = getStorage();
        int oid = storage.getOid(obj);
        if (oid == 0) {
            oid = storage.makePersistent(obj);
        }
        if (s == null) {
            final Relation<T, ThickIndex> r = storage.<T, ThickIndex>createRelation(null);
            r.add(obj);
            index.put(key, r);
        } else if (s instanceof Relation) {
            final Relation rel = (Relation) s;
            if (rel.size() == BTREE_THRESHOLD) {
                final IPersistentSet<T> ps = storage.<T>createBag();
                for (int i = 0; i < BTREE_THRESHOLD; i++) {
                    ps.add((T) rel.get(i));
                }
                Assert.that(ps.add(obj));
                index.set(key, ps);
                rel.deallocate();
            } else {
                int l = 0;
                final int n = rel.size();
                int r = n;
                while (l < r) {
                    final int m = l + r >>> 1;
                    if (storage.getOid(rel.getRaw(m)) <= oid) {
                        l = m + 1;
                    } else {
                        r = m;
                    }
                }
                rel.insert(r, obj);
            }
        } else {
            Assert.that(((IPersistentSet<T>) s).add(obj));
        }
        nElems += 1;
        modify();
        return true;
    }

    @Override
    public boolean put(final Object key, final T obj) {
        return put(Btree.getKeyFromObject(key), obj);
    }

    @Override
    public T remove(final Key key) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    @Override
    public void remove(final Key key, final T obj) {
        if (!removeIfExists(key, obj)) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
    }

    @Override
    public void remove(final Object key, final T obj) {
        remove(Btree.getKeyFromObject(key), obj);
    }

    @Override
    public T remove(final String key) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    boolean removeIfExists(final Key key, final T obj) {
        final Object s = index.get(key);
        if (s instanceof Relation) {
            final Relation rel = (Relation) s;
            final Storage storage = getStorage();
            final int oid = storage.getOid(obj);
            int l = 0;
            final int n = rel.size();
            int r = n;
            while (l < r) {
                final int m = l + r >>> 1;
                if (storage.getOid(rel.getRaw(m)) < oid) {
                    l = m + 1;
                } else {
                    r = m;
                }
            }
            if (r < n && storage.getOid(rel.getRaw(r)) == oid) {
                rel.remove(r);
                if (rel.size() == 0) {
                    index.remove(key, rel);
                    rel.deallocate();
                }
                nElems -= 1;
                modify();
                return true;
            }
        } else if (s instanceof IPersistentSet) {
            final IPersistentSet ps = (IPersistentSet) s;
            if (ps.remove(obj)) {
                if (ps.size() == 0) {
                    index.remove(key, ps);
                    ps.deallocate();
                }
                nElems -= 1;
                modify();
                return true;
            }
        }
        return false;
    }

    @Override
    public T removeKey(final Object key) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    @Override
    public T set(final Key key, final T obj) {
        final Object s = index.get(key);
        final Storage storage = getStorage();
        int oid = storage.getOid(obj);
        if (oid == 0) {
            oid = storage.makePersistent(obj);
        }
        if (s == null) {
            final Relation<T, ThickIndex> r = storage.<T, ThickIndex>createRelation(null);
            r.add(obj);
            index.put(key, r);
            nElems += 1;
            modify();
            return null;
        } else if (s instanceof Relation) {
            final Relation r = (Relation) s;
            if (r.size() == 1) {
                final Object prev = r.get(0);
                r.set(0, obj);
                return (T) prev;
            }
        }
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    @Override
    public T set(final Object key, final T obj) {
        return set(Btree.getKeyFromObject(key), obj);
    }

    @Override
    public int size() {
        return nElems;
    }

    @Override
    public Object[] toArray() {
        return extend(index.toArray());
    }

    @Override
    public <E> E[] toArray(final E[] arr) {
        final ArrayList<E> list = new ArrayList<E>();
        for (final Object c : index) {
            list.addAll((Collection<E>) c);
        }
        return list.toArray(arr);
    }

    @Override
    public boolean unlink(final Key key, final T obj) {
        return removeIfExists(key, obj);
    }
}