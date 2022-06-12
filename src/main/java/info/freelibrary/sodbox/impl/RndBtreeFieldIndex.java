
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

class RndBtreeFieldIndex<T> extends RndBtree<T> implements FieldIndex<T> {

    String myClassName;

    String myFieldName;

    long myAutoIncCount;

    transient Class myClass;

    transient Field myField;

    RndBtreeFieldIndex() {
    }

    RndBtreeFieldIndex(final Class aClass, final String aFieldName, final boolean aUniqueKeyIndex) {
        myClass = aClass;
        isUniqueKeyIndex = aUniqueKeyIndex;
        myFieldName = aFieldName;
        myClassName = ClassDescriptor.getClassName(aClass);

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
                    final Object val = field.get(aObj);
                    if (val != null) {
                        key = new Key((String) val);
                    }
                }
                    break;
                case ClassDescriptor.TP_VALUE:
                    key = new Key((IValue) field.get(aObj));
                    break;
                default:
                    Assert.failed("Invalid type");
            }

            return key;
        } catch (final Exception details) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, details);
        }
    }

    @Override
    public boolean put(final T aObj) {
        final Key key = extractKey(aObj);
        return key != null && super.insert(key, aObj, false) == null;
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
        final FieldValue[] fieldValues = new FieldValue[aCollection.size()];
        final Iterator<? extends T> iterator = aCollection.iterator();

        try {
            for (int index = 0; iterator.hasNext(); index++) {
                final T obj = iterator.next();

                fieldValues[index] = new FieldValue(obj, myField.get(obj));
            }
        } catch (final Exception details) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, details);
        }

        Arrays.sort(fieldValues);

        for (int index = 0; index < fieldValues.length; index++) {
            add((T) fieldValues[index].myObject);
        }

        return fieldValues.length > 0;
    }

    @Override
    public boolean remove(final Object aObj) {
        final Key key = extractKey(aObj);
        return key != null && super.removeIfExists(key, aObj);
    }

    @Override
    public boolean containsObject(final T aObj) {
        final Key key = extractKey(aObj);

        if (key == null) {
            return false;
        }

        if (isUniqueKeyIndex) {
            return super.get(key) != null;
        } else {
            final Object[] mbrs = get(key, key);

            for (int index = 0; index < mbrs.length; index++) {
                if (mbrs[index] == aObj) {
                    return true;
                }
            }

            return false;
        }
    }

    @Override
    public boolean contains(final Object aObj) {
        final Key key = extractKey(aObj);

        if (key == null) {
            return false;
        }

        if (isUniqueKeyIndex) {
            return super.get(key) != null;
        } else {
            final Object[] mbrs = get(key, key);

            for (int i = 0; i < mbrs.length; i++) {
                if (mbrs[i].equals(aObj)) {
                    return true;
                }
            }

            return false;
        }
    }

    @Override
    public synchronized void append(final T aObj) {
        final Key key;

        try {
            switch (myType) {
                case ClassDescriptor.TP_INT:
                    key = new Key((int) myAutoIncCount);
                    myField.setInt(aObj, (int) myAutoIncCount);
                    break;
                case ClassDescriptor.TP_LONG:
                    key = new Key(myAutoIncCount);
                    myField.setLong(aObj, myAutoIncCount);
                    break;
                default:
                    throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, myField.getType());
            }
        } catch (final Exception details) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, details);
        }

        myAutoIncCount += 1;
        getStorage().modify(aObj);

        super.insert(key, aObj, false);
    }

    @Override
    public T[] getPrefix(final String aPrefix) {
        final ArrayList<T> list = getList(new Key(aPrefix, true), new Key(aPrefix + Character.MAX_VALUE, false));
        return list.toArray((T[]) Array.newInstance(myClass, list.size()));
    }

    @Override
    public T[] prefixSearch(final String aKey) {
        final ArrayList<T> list = prefixSearchList(aKey);
        return list.toArray((T[]) Array.newInstance(myClass, list.size()));
    }

    @Override
    public T[] get(final Key aFrom, final Key aTo) {
        final ArrayList<T> list = new ArrayList();

        if (myRoot != null) {
            myRoot.find(checkKey(aFrom), checkKey(aTo), myHeight, list);
        }

        return list.toArray((T[]) Array.newInstance(myClass, list.size()));
    }

    @Override
    public T[] toArray() {
        final T[] array = (T[]) Array.newInstance(myClass, myElementCount);

        if (myRoot != null) {
            myRoot.traverseForward(myHeight, array, 0);
        }

        return array;
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

class RndBtreeCaseInsensitiveFieldIndex<T> extends RndBtreeFieldIndex<T> {

    RndBtreeCaseInsensitiveFieldIndex() {
    }

    RndBtreeCaseInsensitiveFieldIndex(final Class aClass, final String aFieldName, final boolean aUniqueKeyIndex) {
        super(aClass, aFieldName, aUniqueKeyIndex);
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
