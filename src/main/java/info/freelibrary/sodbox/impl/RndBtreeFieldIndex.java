
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.FieldIndex;
import info.freelibrary.sodbox.IValue;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.StorageError;

class RndBtreeCaseInsensitiveFieldIndex<T> extends RndBtreeFieldIndex<T> {

    RndBtreeCaseInsensitiveFieldIndex() {
    }

    RndBtreeCaseInsensitiveFieldIndex(final Class cls, final String fieldName, final boolean unique) {
        super(cls, fieldName, unique);
    }

    @Override
    Key checkKey(Key key) {
        if (key != null && key.oval instanceof String) {
            key = new Key(((String) key.oval).toLowerCase(), key.inclusion != 0);
        }
        return super.checkKey(key);
    }

    @Override
    public boolean isCaseInsensitive() {
        return true;
    }
}

class RndBtreeFieldIndex<T> extends RndBtree<T> implements FieldIndex<T> {

    String className;

    String fieldName;

    long autoincCount;

    transient Class cls;

    transient Field fld;

    RndBtreeFieldIndex() {
    }

    RndBtreeFieldIndex(final Class cls, final String fieldName, final boolean unique) {
        this.cls = cls;
        this.unique = unique;
        this.fieldName = fieldName;
        this.className = ClassDescriptor.getClassName(cls);
        locateField();
        type = checkType(fld.getType());
    }

    @Override
    public boolean addAll(final Collection<? extends T> c) {
        final FieldValue[] arr = new FieldValue[c.size()];
        final Iterator<? extends T> e = c.iterator();
        try {
            for (int i = 0; e.hasNext(); i++) {
                final T obj = e.next();
                arr[i] = new FieldValue(obj, fld.get(obj));
            }
        } catch (final Exception x) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        Arrays.sort(arr);
        for (final FieldValue element : arr) {
            add((T) element.obj);
        }
        return arr.length > 0;
    }

    @Override
    public synchronized void append(final T obj) {
        Key key;
        try {
            switch (type) {
                case ClassDescriptor.tpInt:
                    key = new Key((int) autoincCount);
                    fld.setInt(obj, (int) autoincCount);
                    break;
                case ClassDescriptor.tpLong:
                    key = new Key(autoincCount);
                    fld.setLong(obj, autoincCount);
                    break;
                default:
                    throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, fld.getType());
            }
        } catch (final Exception x) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        autoincCount += 1;
        getStorage().modify(obj);
        super.insert(key, obj, false);
    }

    @Override
    public boolean contains(final Object obj) {
        final Key key = extractKey(obj);
        if (key == null) {
            return false;
        }
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
        if (key == null) {
            return false;
        }
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

    private Key extractKey(final Object obj) {
        try {
            final Field f = fld;
            Key key = null;
            switch (type) {
                case ClassDescriptor.tpBoolean:
                    key = new Key(f.getBoolean(obj));
                    break;
                case ClassDescriptor.tpByte:
                    key = new Key(f.getByte(obj));
                    break;
                case ClassDescriptor.tpShort:
                    key = new Key(f.getShort(obj));
                    break;
                case ClassDescriptor.tpChar:
                    key = new Key(f.getChar(obj));
                    break;
                case ClassDescriptor.tpInt:
                    key = new Key(f.getInt(obj));
                    break;
                case ClassDescriptor.tpObject: {
                    final Object val = f.get(obj);
                    key = new Key(val, getStorage().makePersistent(val), true);
                    break;
                }
                case ClassDescriptor.tpLong:
                    key = new Key(f.getLong(obj));
                    break;
                case ClassDescriptor.tpDate:
                    key = new Key((Date) f.get(obj));
                    break;
                case ClassDescriptor.tpFloat:
                    key = new Key(f.getFloat(obj));
                    break;
                case ClassDescriptor.tpDouble:
                    key = new Key(f.getDouble(obj));
                    break;
                case ClassDescriptor.tpEnum:
                    key = new Key((Enum) f.get(obj));
                    break;
                case ClassDescriptor.tpString: {
                    final Object val = f.get(obj);
                    if (val != null) {
                        key = new Key((String) val);
                    }
                }
                    break;
                case ClassDescriptor.tpValue:
                    key = new Key((IValue) f.get(obj));
                    break;
                default:
                    Assert.failed("Invalid type");
            }
            return key;
        } catch (final Exception x) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
    }

    @Override
    public T[] get(final Key from, final Key till) {
        final ArrayList<T> list = new ArrayList();
        if (root != null) {
            root.find(checkKey(from), checkKey(till), height, list);
        }
        return list.toArray((T[]) Array.newInstance(cls, list.size()));
    }

    @Override
    public Class getIndexedClass() {
        return cls;
    }

    @Override
    public Field[] getKeyFields() {
        return new Field[] { fld };
    }

    @Override
    public T[] getPrefix(final String prefix) {
        final ArrayList<T> list = getList(new Key(prefix, true), new Key(prefix + Character.MAX_VALUE, false));
        return list.toArray((T[]) Array.newInstance(cls, list.size()));
    }

    @Override
    public boolean isCaseInsensitive() {
        return false;
    }

    private final void locateField() {
        fld = ClassDescriptor.locateField(cls, fieldName);
        if (fld == null) {
            throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName);
        }
    }

    @Override
    public void onLoad() {
        cls = ClassDescriptor.loadClass(getStorage(), className);
        locateField();
    }

    @Override
    public T[] prefixSearch(final String key) {
        final ArrayList<T> list = prefixSearchList(key);
        return list.toArray((T[]) Array.newInstance(cls, list.size()));
    }

    @Override
    public boolean put(final T obj) {
        final Key key = extractKey(obj);
        return key != null && super.insert(key, obj, false) == null;
    }

    public IterableIterator<T> queryByExample(final T obj) {
        final Key key = extractKey(obj);
        return iterator(key, key, ASCENT_ORDER);
    }

    @Override
    public boolean remove(final Object obj) {
        final Key key = extractKey(obj);
        return key != null && super.removeIfExists(key, obj);
    }

    @Override
    public T set(final T obj) {
        final Key key = extractKey(obj);
        if (key == null) {
            throw new StorageError(StorageError.KEY_IS_NULL);
        }
        return super.set(key, obj);
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
