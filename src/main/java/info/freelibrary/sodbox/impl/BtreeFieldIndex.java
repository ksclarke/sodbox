
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
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.StorageError;

class FieldValue implements Comparable<FieldValue> {

    Comparable myValue;

    Object myObject;

    FieldValue(final Object aObject, final Object aValue) {
        myObject = aObject;
        myValue = (Comparable) aValue;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(final FieldValue aFieldValue) {
        return myValue.compareTo(aFieldValue.myValue);
    }

}

class BtreeFieldIndex<T> extends Btree<T> implements FieldIndex<T> {

    String myClassName;

    String myFieldName;

    long myAutoIncrementCount;

    transient Class myClass;

    transient Field myField;

    BtreeFieldIndex() {
    }

    BtreeFieldIndex(final Class aClass, final String aFieldName, final boolean aUniqueRestriction) {
        this(aClass, aFieldName, aUniqueRestriction, 0);
    }

    BtreeFieldIndex(final Class aClass, final String aFieldName, final boolean aUniqueRestriction,
            final long aAutoIncrementCount) {
        myClass = aClass;
        isUniqueKeyIndex = aUniqueRestriction;
        myFieldName = aFieldName;
        myClassName = ClassDescriptor.getClassName(aClass);
        myAutoIncrementCount = aAutoIncrementCount;
        locateField();
        myType = checkType(myField.getType());
    }

    private void locateField() {
        myField = ClassDescriptor.locateField(myClass, myFieldName);

        if (myField == null) {
            throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, myClassName + "." + myFieldName);
        }
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
        myClass = ClassDescriptor.loadClass(getStorage(), myClassName);
        locateField();
    }

    @Override
    protected Object unpackEnum(final int aValue) {
        return myField.getType().getEnumConstants()[aValue];
    }

    private Key extractKey(final Object aObject) {
        try {
            final Field f = myField;

            Key key = null;

            switch (myType) {
                case ClassDescriptor.TP_BOOLEAN:
                    key = new Key(f.getBoolean(aObject));
                    break;
                case ClassDescriptor.TP_BYTE:
                    key = new Key(f.getByte(aObject));
                    break;
                case ClassDescriptor.TP_SHORT:
                    key = new Key(f.getShort(aObject));
                    break;
                case ClassDescriptor.TP_CHAR:
                    key = new Key(f.getChar(aObject));
                    break;
                case ClassDescriptor.TP_INT:
                    key = new Key(f.getInt(aObject));
                    break;
                case ClassDescriptor.TP_OBJECT: {
                    final Object value = f.get(aObject);

                    key = new Key(value, getStorage().makePersistent(value), true);
                    break;
                }
                case ClassDescriptor.TP_LONG:
                    key = new Key(f.getLong(aObject));
                    break;
                case ClassDescriptor.TP_DATE:
                    key = new Key((Date) f.get(aObject));
                    break;
                case ClassDescriptor.TP_FLOAT:
                    key = new Key(f.getFloat(aObject));
                    break;
                case ClassDescriptor.TP_DOUBLE:
                    key = new Key(f.getDouble(aObject));
                    break;
                case ClassDescriptor.TP_ENUM:
                    key = new Key((Enum) f.get(aObject));
                    break;
                case ClassDescriptor.TP_STRING: {
                    final Object value = f.get(aObject);

                    if (value != null) {
                        key = new Key((String) value);
                    }

                    break;
                }
                default:
                    Assert.failed("Invalid type");
            }

            return key;
        } catch (final Exception details) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, details);
        }
    }

    @Override
    public boolean add(final T aObject) {
        return put(aObject);
    }

    @Override
    public boolean put(final T aObject) {
        final Key key = extractKey(aObject);
        return key != null && super.insert(key, aObject, false) >= 0;
    }

    @Override
    public T set(final T aObject) {
        final Key key = extractKey(aObject);

        if (key == null) {
            throw new StorageError(StorageError.KEY_IS_NULL);
        }

        return super.set(key, aObject);
    }

    @SuppressWarnings("unchecked")
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

        for (int i = 0; i < array.length; i++) {
            add((T) array[i].myObject);
        }

        return array.length > 0;
    }

    @Override
    public boolean remove(final Object aObject) {
        final Key key = extractKey(aObject);
        return key != null && super.removeIfExists(key, aObject);
    }

    @Override
    public boolean containsObject(final T aObject) {
        final Key key = extractKey(aObject);

        if (key == null) {
            return false;
        }

        if (isUniqueKeyIndex) {
            return super.get(key) != null;
        } else {
            final Object[] members = get(key, key);

            for (int index = 0; index < members.length; index++) {
                if (members[index] == aObject) {
                    return true;
                }
            }

            return false;
        }
    }

    @Override
    public boolean contains(final Object aObject) {
        final Key key = extractKey(aObject);

        if (key == null) {
            return false;
        }

        if (isUniqueKeyIndex) {
            return super.get(key) != null;
        } else {
            final Object[] members = get(key, key);

            for (int index = 0; index < members.length; index++) {
                if (members[index].equals(aObject)) {
                    return true;
                }
            }

            return false;
        }
    }

    @Override
    public synchronized void append(final T aObject) {
        final Key key;

        try {
            switch (myType) {
                case ClassDescriptor.TP_INT:
                    key = new Key((int) myAutoIncrementCount);
                    myField.setInt(aObject, (int) myAutoIncrementCount);
                    break;
                case ClassDescriptor.TP_LONG:
                    key = new Key(myAutoIncrementCount);
                    myField.setLong(aObject, myAutoIncrementCount);
                    break;
                default:
                    throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, myField.getType());
            }
        } catch (final Exception details) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, details);
        }

        myAutoIncrementCount += 1;
        getStorage().modify(aObject);
        super.insert(key, aObject, false);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T[] getPrefix(final String aPrefix) {
        final ArrayList<T> list = getList(new Key(aPrefix, true), new Key(aPrefix + Character.MAX_VALUE, false));
        return list.toArray((T[]) Array.newInstance(myClass, list.size()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T[] prefixSearch(final String aKey) {
        final ArrayList<T> list = prefixSearchList(aKey);
        return list.toArray((T[]) Array.newInstance(myClass, list.size()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T[] get(final Key aFrom, final Key aTo) {
        final ArrayList list = new ArrayList();

        if (myRoot != 0) {
            BtreePage.find((StorageImpl) getStorage(), myRoot, checkKey(aFrom), checkKey(aTo), this, myHeight, list);
        }

        return (T[]) list.toArray((T[]) Array.newInstance(myClass, list.size()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T[] toArray() {
        final T[] array = (T[]) Array.newInstance(myClass, myNumOfElems);

        if (myRoot != 0) {
            BtreePage.traverseForward((StorageImpl) getStorage(), myRoot, myType, myHeight, array, 0);
        }

        return array;
    }

    public IterableIterator<T> queryByExample(final T aObject) {
        final Key key = extractKey(aObject);
        return iterator(key, key, ASCENT_ORDER);
    }

    @Override
    public boolean isCaseInsensitive() {
        return false;
    }

}

class BtreeCaseInsensitiveFieldIndex<T> extends BtreeFieldIndex<T> {

    BtreeCaseInsensitiveFieldIndex() {
    }

    BtreeCaseInsensitiveFieldIndex(final Class aClass, final String aFieldName, final boolean aUniqueRestriction) {
        super(aClass, aFieldName, aUniqueRestriction);
    }

    BtreeCaseInsensitiveFieldIndex(final Class aClass, final String aFieldName, final boolean aUniqueRestriction,
            final long aAutoIncrementCount) {
        super(aClass, aFieldName, aUniqueRestriction, aAutoIncrementCount);
    }

    @Override
    Key checkKey(final Key aKey) {
        Key key = aKey;

        if (key != null && key.myObjectValue instanceof String) {
            key = new Key(((String) key.myObjectValue).toLowerCase(), key.myInclusion != 0);
        }

        return super.checkKey(key);
    }

    @Override
    public boolean isCaseInsensitive() {
        return true;
    }

}
