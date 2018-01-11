
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import info.freelibrary.sodbox.FieldIndex;
import info.freelibrary.sodbox.IValue;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.StorageError;

class AltBtreeCaseInsensitiveMultiFieldIndex<T> extends AltBtreeMultiFieldIndex<T> {

    AltBtreeCaseInsensitiveMultiFieldIndex() {
    }

    AltBtreeCaseInsensitiveMultiFieldIndex(final Class cls, final String[] fieldNames, final boolean unique) {
        super(cls, fieldNames, unique);
    }

    @Override
    Key checkKey(final Key key) {
        if (key != null) {
            final CompoundKey ck = (CompoundKey) key.oval;
            for (int i = 0; i < ck.keys.length; i++) {
                if (ck.keys[i] instanceof String) {
                    ck.keys[i] = ((String) ck.keys[i]).toLowerCase();
                }
            }
        }
        return super.checkKey(key);
    }

    @Override
    public boolean isCaseInsensitive() {
        return true;
    }
}

class AltBtreeMultiFieldIndex<T> extends AltBtree<T> implements FieldIndex<T> {

    static class CompoundKey implements Comparable, IValue {

        Object[] keys;

        CompoundKey(final Object[] keys) {
            this.keys = keys;
        }

        @Override
        public int compareTo(final Object o) {
            final CompoundKey c = (CompoundKey) o;
            final int n = keys.length < c.keys.length ? keys.length : c.keys.length;
            for (int i = 0; i < n; i++) {
                if (keys[i] != c.keys[i]) {
                    if (keys[i] == null) {
                        return -1;
                    } else if (c.keys[i] == null) {
                        return 1;
                    } else {
                        final int diff = ((Comparable) keys[i]).compareTo(c.keys[i]);
                        if (diff != 0) {
                            return diff;
                        }
                    }
                }
            }
            return 0; // allow to compare part of the compound key
        }
    }

    String className;

    String[] fieldName;

    transient Class cls;

    transient Field[] fld;

    AltBtreeMultiFieldIndex() {
    }

    AltBtreeMultiFieldIndex(final Class cls, final String[] fieldName, final boolean unique) {
        this.cls = cls;
        this.unique = unique;
        this.fieldName = fieldName;
        this.className = ClassDescriptor.getClassName(cls);
        locateFields();
        type = ClassDescriptor.tpValue;
    }

    @Override
    public boolean addAll(final Collection<? extends T> c) {
        final MultiFieldValue[] arr = new MultiFieldValue[c.size()];
        final Iterator<? extends T> e = c.iterator();
        try {
            for (int i = 0; e.hasNext(); i++) {
                final T obj = e.next();
                final Comparable[] values = new Comparable[fld.length];
                for (int j = 0; j < values.length; j++) {
                    values[j] = (Comparable) fld[j].get(obj);
                }
                arr[i] = new MultiFieldValue(obj, values);
            }
        } catch (final Exception x) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        Arrays.sort(arr);
        for (final MultiFieldValue element : arr) {
            add((T) element.obj);
        }
        return arr.length > 0;
    }

    @Override
    public void append(final T obj) {
        throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
    }

    @Override
    public boolean contains(final Object obj) {
        final Key key = extractKey(obj);
        if (unique) {
            return super.get(key) != null;
        } else {
            final Object[] mbrs = get(key, key);
            for (final Object mbr : mbrs) {
                if (mbr.equals(obj)) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public boolean containsObject(final T obj) {
        final Key key = extractKey(obj);
        if (unique) {
            return super.get(key) != null;
        } else {
            final Object[] mbrs = get(key, key);
            for (final Object mbr : mbrs) {
                if (mbr == obj) {
                    return true;
                }
            }
            return false;
        }
    }

    private Key convertKey(final Key key) {
        if (key == null) {
            return null;
        }
        if (key.type != ClassDescriptor.tpArrayOfObject) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        return new Key(new CompoundKey((Object[]) key.oval), key.inclusion != 0);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key from, final Key till, final int order) {
        return super.entryIterator(convertKey(from), convertKey(till), order);
    }

    private Key extractKey(final Object obj) {
        final Object[] keys = new Object[fld.length];
        try {
            for (int i = 0; i < keys.length; i++) {
                final Object val = fld[i].get(obj);
                keys[i] = val;
                if (!ClassDescriptor.isEmbedded(val)) {
                    getStorage().makePersistent(val);
                }
            }
        } catch (final Exception x) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        return new Key(new CompoundKey(keys));
    }

    @Override
    public T get(final Key key) {
        return super.get(convertKey(key));
    }

    @Override
    public T[] get(final Key from, final Key till) {
        final ArrayList list = new ArrayList();
        if (root != null) {
            root.find(convertKey(from), convertKey(till), height, list);
        }
        return (T[]) list.toArray((T[]) Array.newInstance(cls, list.size()));
    }

    @Override
    public Class getIndexedClass() {
        return cls;
    }

    @Override
    public Field[] getKeyFields() {
        return fld;
    }

    @Override
    public T[] getPrefix(final String prefix) {
        throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
    }

    @Override
    public boolean isCaseInsensitive() {
        return false;
    }

    @Override
    public IterableIterator<T> iterator(final Key from, final Key till, final int order) {
        return super.iterator(convertKey(from), convertKey(till), order);
    }

    private final void locateFields() {
        fld = new Field[fieldName.length];
        for (int i = 0; i < fieldName.length; i++) {
            fld[i] = ClassDescriptor.locateField(cls, fieldName[i]);
            if (fld[i] == null) {
                throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName[i]);
            }
        }
    }

    @Override
    public void onLoad() {
        cls = ClassDescriptor.loadClass(getStorage(), className);
        locateFields();
    }

    @Override
    public T[] prefixSearch(final String key) {
        throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
    }

    @Override
    public boolean put(final T obj) {
        return super.put(extractKey(obj), obj);
    }

    public IterableIterator<T> queryByExample(final T obj) {
        final Key key = extractKey(obj);
        return iterator(key, key, ASCENT_ORDER);
    }

    @Override
    public T remove(final Key key) {
        return super.remove(convertKey(key));
    }

    @Override
    public boolean remove(final Object obj) {
        return super.removeIfExists(extractKey(obj), obj);
    }

    @Override
    public T set(final T obj) {
        return super.set(extractKey(obj), obj);
    }

    @Override
    public T[] toArray() {
        final T[] arr = (T[]) Array.newInstance(cls, nElems);
        if (root != null) {
            root.traverseForward(height, arr, 0);
        }
        return arr;
    }
}
