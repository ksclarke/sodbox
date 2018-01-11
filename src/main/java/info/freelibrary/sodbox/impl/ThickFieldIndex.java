
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.FieldIndex;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.StorageError;

class ThickCaseInsensitiveFieldIndex<T> extends ThickFieldIndex<T> {

    ThickCaseInsensitiveFieldIndex() {
    }

    ThickCaseInsensitiveFieldIndex(final StorageImpl db, final Class cls, final String fieldName) {
        super(db, cls, fieldName);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key from, final Key till, final int order) {
        return super.entryIterator(transformKey(from), transformKey(till), order);
    }

    @Override
    public T get(final Key key) {
        return super.get(transformKey(key));
    }

    @Override
    public ArrayList<T> getList(final Key from, final Key till) {
        return super.getList(transformKey(from), transformKey(till));
    }

    @Override
    public ArrayList<T> getPrefixList(final String prefix) {
        return super.getPrefixList(transformStringKey(prefix));
    }

    @Override
    public int indexOf(final Key key) {
        return super.indexOf(transformKey(key));
    }

    @Override
    public boolean isCaseInsensitive() {
        return true;
    }

    @Override
    public IterableIterator<T> iterator(final Key from, final Key till, final int order) {
        return super.iterator(transformKey(from), transformKey(till), order);
    }

    @Override
    public IterableIterator<T> prefixIterator(final String prefix, final int order) {
        return super.prefixIterator(transformStringKey(prefix), order);
    }

    @Override
    public ArrayList<T> prefixSearchList(final String word) {
        return super.prefixSearchList(transformStringKey(word));
    }

    @Override
    protected Key transformKey(Key key) {
        if (key != null && key.oval instanceof String) {
            key = new Key(((String) key.oval).toLowerCase(), key.inclusion != 0);
        }
        return key;
    }

    @Override
    protected String transformStringKey(final String key) {
        return key.toLowerCase();
    }
}

class ThickFieldIndex<T> extends ThickIndex<T> implements FieldIndex<T> {

    static Field locateField(final Class cls, final String fieldName) {
        final Field fld = ClassDescriptor.locateField(cls, fieldName);
        if (fld == null) {
            throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, cls.getName() + "." + fieldName);
        }
        return fld;
    }

    String fieldName;

    int type;

    Class cls;

    transient Field fld;

    ThickFieldIndex() {
    }

    ThickFieldIndex(final StorageImpl db, final Class cls, final String fieldName) {
        this(db, cls, fieldName, locateField(cls, fieldName));
    }

    ThickFieldIndex(final StorageImpl db, final Class cls, final String fieldName, final Field fld) {
        super(db, fld.getType());
        type = Btree.checkType(fld.getType());
        this.cls = cls;
        this.fld = fld;
        this.fieldName = fieldName;
    }

    @Override
    public boolean add(final T obj) {
        return put(obj);
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
    public void append(final T obj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(final Object obj) {
        final Key key = extractKey(obj);
        if (key == null) {
            return false;
        }
        final Object[] mbrs = get(key, key);
        for (final Object mbr : mbrs) {
            if (mbr.equals(obj)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsObject(final T obj) {
        final Key key = extractKey(obj);
        if (key == null) {
            return false;
        }
        final Object[] mbrs = get(key, key);
        for (final Object mbr : mbrs) {
            if (mbr == obj) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected Object[] extend(final Object[] s) {
        final ArrayList list = new ArrayList();
        for (final Object element : s) {
            list.addAll((Collection) element);
        }
        return list.toArray((T[]) Array.newInstance(cls, list.size()));
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
                        key = new Key(transformStringKey((String) val));
                    }
                }
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
        return (T[]) super.get(transformKey(from), transformKey(till));
    }

    @Override
    public T[] get(final Object from, final Object till) {
        return (T[]) super.get(from, till);
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
        return (T[]) super.getPrefix(transformStringKey(prefix));
    }

    @Override
    public boolean isCaseInsensitive() {
        return false;
    }

    private final void locateField() {
        fld = locateField(cls, fieldName);
    }

    @Override
    public void onLoad() {
        locateField();
    }

    @Override
    public T[] prefixSearch(final String key) {
        return (T[]) super.prefixSearch(transformStringKey(key));
    }

    @Override
    public boolean put(final T obj) {
        final Key key = extractKey(obj);
        return key != null && super.put(key, obj);
    }

    public IterableIterator<T> queryByExample(final T obj) {
        final Key key = extractKey(obj);
        return iterator(key, key, ASCENT_ORDER);
    }

    @Override
    public boolean remove(final Object obj) {
        final Key key = extractKey(obj);
        return key != null && super.removeIfExists(key, (T) obj);
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
        return (T[]) super.toArray();
    }

    protected Key transformKey(final Key key) {
        return key;
    }

    protected String transformStringKey(final String key) {
        return key;
    }
}
