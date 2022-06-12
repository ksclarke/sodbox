
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
import info.freelibrary.sodbox.Constants;
import info.freelibrary.sodbox.FieldIndex;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.MessageCodes;
import info.freelibrary.sodbox.StorageError;
import info.freelibrary.util.Logger;
import info.freelibrary.util.LoggerFactory;

class ThickFieldIndex<T> extends ThickIndex<T> implements FieldIndex<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThickFieldIndex.class, Constants.MESSAGES);

    String myFieldName;

    int myType;

    Class myClass;

    transient Field myField;

    ThickFieldIndex() {
    }

    ThickFieldIndex(final StorageImpl aStorageImpl, final Class aClass, final String aFieldName) {
        this(aStorageImpl, aClass, aFieldName, locateField(aClass, aFieldName));
    }

    ThickFieldIndex(final StorageImpl aStorageImpl, final Class aClass, final String aFieldName, final Field aField) {
        super(aStorageImpl, aField.getType());

        myType = Btree.checkType(aField.getType());
        myClass = aClass;
        myField = aField;
        myFieldName = aFieldName;
    }

    static Field locateField(final Class aClass, final String aFieldName) {
        final Field field = ClassDescriptor.locateField(aClass, aFieldName);

        if (field == null) {
            throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, aClass.getName() + "." + aFieldName);
        }

        return field;
    }

    private void locateField() {
        myField = locateField(myClass, myFieldName);
    }

    @Override
    public Class getIndexedClass() {
        return myClass;
    }

    @Override
    public Field[] getKeyFields() {
        return new Field[] { myField };
    }

    @Override
    public void onLoad() {
        locateField();
    }

    protected String transformStringKey(final String aKey) {
        return aKey;
    }

    protected Key transformKey(final Key aKey) {
        return aKey;
    }

    private Key extractKey(final Object aObj) {
        try {
            final Field field = myField;

            Key key = null;

            switch (myType) {
                case ClassDescriptor.TP_BOOLEAN:
                    key = new Key(field.getBoolean(aObj));
                    break;
                case ClassDescriptor.TP_BYTE:
                    key = new Key(field.getByte(aObj));
                    break;
                case ClassDescriptor.TP_SHORT:
                    key = new Key(field.getShort(aObj));
                    break;
                case ClassDescriptor.TP_CHAR:
                    key = new Key(field.getChar(aObj));
                    break;
                case ClassDescriptor.TP_INT:
                    key = new Key(field.getInt(aObj));
                    break;
                case ClassDescriptor.TP_OBJECT: {
                    final Object value = field.get(aObj);

                    key = new Key(value, getStorage().makePersistent(value), true);
                    break;
                }
                case ClassDescriptor.TP_LONG:
                    key = new Key(field.getLong(aObj));
                    break;
                case ClassDescriptor.TP_DATE:
                    key = new Key((Date) field.get(aObj));
                    break;
                case ClassDescriptor.TP_FLOAT:
                    key = new Key(field.getFloat(aObj));
                    break;
                case ClassDescriptor.TP_DOUBLE:
                    key = new Key(field.getDouble(aObj));
                    break;
                case ClassDescriptor.TP_ENUM:
                    key = new Key((Enum) field.get(aObj));
                    break;
                case ClassDescriptor.TP_STRING: {
                    final Object value = field.get(aObj);

                    if (value != null) {
                        key = new Key(transformStringKey((String) value));
                    }
                }
                    break;
                default:
                    Assert.failed(LOGGER.getMessage(MessageCodes.SB_027));
            }
            return key;
        } catch (final Exception x) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
    }

    @Override
    public boolean add(final T aObj) {
        return put(aObj);
    }

    @Override
    public boolean put(final T aObj) {
        final Key key = extractKey(aObj);
        return key != null && super.put(key, aObj);
    }

    @Override
    public T set(final T aObj) {
        final Key key = extractKey(aObj);

        if (key == null) {
            throw new StorageError(StorageError.KEY_IS_NULL);
        }

        return super.set(key, aObj);
    }

    @Override
    public boolean addAll(final Collection<? extends T> aCollection) {
        final FieldValue[] array = new FieldValue[aCollection.size()];
        final Iterator<? extends T> iterator = aCollection.iterator();

        try {
            for (int index = 0; iterator.hasNext(); index++) {
                final T obj = iterator.next();

                array[index] = new FieldValue(obj, myField.get(obj));
            }
        } catch (final Exception details) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, details);
        }

        Arrays.sort(array);

        for (int index = 0; index < array.length; index++) {
            add((T) array[index].myObject);
        }

        return array.length > 0;
    }

    @Override
    public boolean remove(final Object aObj) {
        final Key key = extractKey(aObj);
        return key != null && super.removeIfExists(key, (T) aObj);
    }

    @Override
    public boolean containsObject(final T aObj) {
        final Key key = extractKey(aObj);

        if (key == null) {
            return false;
        }

        final Object[] members = get(key, key);

        for (int index = 0; index < members.length; index++) {
            if (members[index] == aObj) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean contains(final Object aObj) {
        final Key key = extractKey(aObj);

        if (key == null) {
            return false;
        }

        final Object[] members = get(key, key);

        for (int index = 0; index < members.length; index++) {
            if (members[index].equals(aObj)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void append(final T aObj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T[] getPrefix(final String aPrefix) {
        return (T[]) super.getPrefix(transformStringKey(aPrefix));
    }

    @Override
    protected Object[] extend(final Object[] aArray) {
        final ArrayList list = new ArrayList();

        for (int index = 0; index < aArray.length; index++) {
            list.addAll((Collection) aArray[index]);
        }

        return list.toArray((T[]) Array.newInstance(myClass, list.size()));
    }

    @Override
    public T[] prefixSearch(final String aKey) {
        return (T[]) super.prefixSearch(transformStringKey(aKey));
    }

    @Override
    public T[] get(final Key aFrom, final Key aTo) {
        return (T[]) super.get(transformKey(aFrom), transformKey(aTo));
    }

    @Override
    public T[] get(final Object aFrom, final Object aTo) {
        return (T[]) super.get(aFrom, aTo);
    }

    @Override
    public T[] toArray() {
        return (T[]) super.toArray();
    }

    public IterableIterator<T> queryByExample(final T aObj) {
        final Key key = extractKey(aObj);
        return iterator(key, key, ASCENT_ORDER);
    }

    @Override
    public boolean isCaseInsensitive() {
        return false;
    }
}

class ThickCaseInsensitiveFieldIndex<T> extends ThickFieldIndex<T> {

    ThickCaseInsensitiveFieldIndex() {
    }

    ThickCaseInsensitiveFieldIndex(final StorageImpl aStorageImpl, final Class aClass, final String aFieldName) {
        super(aStorageImpl, aClass, aFieldName);
    }

    @Override
    protected String transformStringKey(final String aKey) {
        return aKey.toLowerCase();
    }

    @Override
    protected Key transformKey(final Key aKey) {
        Key key = aKey;

        if (key != null && key.myObjectValue instanceof String) {
            key = new Key(((String) key.myObjectValue).toLowerCase(), key.myInclusion != 0);
        }

        return key;
    }

    @Override
    public T get(final Key aKey) {
        return super.get(transformKey(aKey));
    }

    @Override
    public ArrayList<T> getList(final Key aFrom, final Key aTo) {
        return super.getList(transformKey(aFrom), transformKey(aTo));
    }

    @Override
    public ArrayList<T> getPrefixList(final String aPrefix) {
        return super.getPrefixList(transformStringKey(aPrefix));
    }

    @Override
    public ArrayList<T> prefixSearchList(final String aWord) {
        return super.prefixSearchList(transformStringKey(aWord));
    }

    @Override
    public IterableIterator<T> iterator(final Key aFrom, final Key aTo, final int aOrder) {
        return super.iterator(transformKey(aFrom), transformKey(aTo), aOrder);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key aFrom, final Key aTo, final int aOrder) {
        return super.entryIterator(transformKey(aFrom), transformKey(aTo), aOrder);
    }

    @Override
    public IterableIterator<T> prefixIterator(final String aPrefix, final int aOrder) {
        return super.prefixIterator(transformStringKey(aPrefix), aOrder);
    }

    @Override
    public int indexOf(final Key aKey) {
        return super.indexOf(transformKey(aKey));
    }

    @Override
    public boolean isCaseInsensitive() {
        return true;
    }

}
