
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

class RndBtreeMultiFieldIndex<T> extends RndBtree<T> implements FieldIndex<T> {

    String myClassName;

    String[] myFieldNames;

    transient Class myClass;

    transient Field[] myFields;

    RndBtreeMultiFieldIndex() {
    }

    RndBtreeMultiFieldIndex(final Class aClass, final String[] aFieldName, final boolean aUniqueIndex) {
        this.myClass = aClass;
        isUniqueKeyIndex = aUniqueIndex;
        this.myFieldNames = aFieldName;
        this.myClassName = ClassDescriptor.getClassName(aClass);

        locateFields();

        myType = ClassDescriptor.TP_VALUE;
    }

    private void locateFields() {
        myFields = new Field[myFieldNames.length];

        for (int index = 0; index < myFieldNames.length; index++) {
            myFields[index] = ClassDescriptor.locateField(myClass, myFieldNames[index]);

            if (myFields[index] == null) {
                throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, myClassName + "." + myFieldNames[index]);
            }
        }
    }

    @Override
    public Class getIndexedClass() {
        return myClass;
    }

    @Override
    public Field[] getKeyFields() {
        return myFields;
    }

    @Override
    public void onLoad() {
        myClass = ClassDescriptor.loadClass(getStorage(), myClassName);
        locateFields();
    }

    private Key convertKey(final Key aKey) {
        if (aKey == null) {
            return null;
        }

        if (aKey.myType != ClassDescriptor.TP_ARRAY_OF_OBJECTS) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }

        return new Key(new CompoundKey((Object[]) aKey.myObjectValue), aKey.myInclusion != 0);
    }

    private Key extractKey(final Object aObj) {
        final Object[] keys = new Object[myFields.length];

        try {
            for (int index = 0; index < keys.length; index++) {
                final Object value = myFields[index].get(aObj);

                keys[index] = value;

                if (!ClassDescriptor.isEmbedded(value)) {
                    getStorage().makePersistent(value);
                }
            }
        } catch (final Exception details) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, details);
        }

        return new Key(new CompoundKey(keys));
    }

    @Override
    public boolean put(final T aObj) {
        return super.put(extractKey(aObj), aObj);
    }

    @Override
    public T set(final T aObj) {
        return super.set(extractKey(aObj), aObj);
    }

    @Override
    public boolean addAll(final Collection<? extends T> aCollection) {
        final MultiFieldValue[] array = new MultiFieldValue[aCollection.size()];
        final Iterator<? extends T> iterator = aCollection.iterator();

        try {
            for (int index = 0; iterator.hasNext(); index++) {
                final T obj = iterator.next();
                final Comparable[] values = new Comparable[myFields.length];

                for (int jndex = 0; jndex < values.length; jndex++) {
                    values[jndex] = (Comparable) myFields[jndex].get(obj);
                }

                array[index] = new MultiFieldValue(obj, values);
            }
        } catch (final Exception x) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }

        Arrays.sort(array);

        for (int index = 0; index < array.length; index++) {
            add((T) array[index].myObject);
        }

        return array.length > 0;
    }

    @Override
    public boolean remove(final Object aObj) {
        return super.removeIfExists(extractKey(aObj), aObj);
    }

    @Override
    public T remove(final Key aKey) {
        return super.remove(convertKey(aKey));
    }

    @Override
    public boolean containsObject(final T aObj) {
        final Key key = extractKey(aObj);

        if (isUniqueKeyIndex) {
            return super.get(key) != null;
        } else {
            final Object[] members = get(key, key);

            for (int index = 0; index < members.length; index++) {
                if (members[index] == aObj) {
                    return true;
                }
            }

            return false;
        }
    }

    @Override
    public boolean contains(final Object aObj) {
        final Key key = extractKey(aObj);

        if (isUniqueKeyIndex) {
            return super.get(key) != null;
        } else {
            final Object[] members = get(key, key);

            for (int index = 0; index < members.length; index++) {
                if (members[index].equals(aObj)) {
                    return true;
                }
            }

            return false;
        }
    }

    @Override
    public void append(final T aObj) {
        throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
    }

    @Override
    public T[] get(final Key aFrom, final Key aTo) {
        final ArrayList list = new ArrayList();

        if (myRoot != null) {
            myRoot.find(convertKey(aFrom), convertKey(aTo), myHeight, list);
        }

        return (T[]) list.toArray((T[]) Array.newInstance(myClass, list.size()));
    }

    @Override
    public T[] getPrefix(final String aPrefix) {
        throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
    }

    @Override
    public T[] prefixSearch(final String aKey) {
        throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
    }

    @Override
    public T[] toArray() {
        final T[] array = (T[]) Array.newInstance(myClass, myElementCount);

        if (myRoot != null) {
            myRoot.traverseForward(myHeight, array, 0);
        }

        return array;
    }

    @Override
    public T get(final Key aKey) {
        return super.get(convertKey(aKey));
    }

    @Override
    public int indexOf(final Key aKey) {
        return super.indexOf(convertKey(aKey));
    }

    @Override
    public IterableIterator<T> iterator(final Key aFrom, final Key aTo, final int aOrder) {
        return super.iterator(convertKey(aFrom), convertKey(aTo), aOrder);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key aFrom, final Key aTo, final int aOrder) {
        return super.entryIterator(convertKey(aFrom), convertKey(aTo), aOrder);
    }

    public IterableIterator<T> queryByExample(final T aObj) {
        final Key key = extractKey(aObj);
        return iterator(key, key, ASCENT_ORDER);
    }

    @Override
    public boolean isCaseInsensitive() {
        return false;
    }

    static class CompoundKey implements Comparable, IValue {

        Object[] myKeys;

        CompoundKey(final Object[] aKeysArray) {
            myKeys = aKeysArray;
        }

        @Override
        public int compareTo(final Object aObj) {
            final CompoundKey compoundKey = (CompoundKey) aObj;
            final int count = myKeys.length < compoundKey.myKeys.length ? myKeys.length : compoundKey.myKeys.length;

            for (int index = 0; index < count; index++) {
                if (myKeys[index] != compoundKey.myKeys[index]) {
                    if (myKeys[index] == null) {
                        return -1;
                    } else if (compoundKey.myKeys[index] == null) {
                        return 1;
                    } else {
                        final int diff = ((Comparable) myKeys[index]).compareTo(compoundKey.myKeys[index]);

                        if (diff != 0) {
                            return diff;
                        }
                    }
                }
            }

            return 0; // allow to compare part of the compound key
        }
    }
}

class RndBtreeCaseInsensitiveMultiFieldIndex<T> extends RndBtreeMultiFieldIndex<T> {

    RndBtreeCaseInsensitiveMultiFieldIndex() {
    }

    RndBtreeCaseInsensitiveMultiFieldIndex(final Class aClass, final String[] aFieldNames,
            final boolean aUniqueIndex) {
        super(aClass, aFieldNames, aUniqueIndex);
    }

    @Override
    Key checkKey(final Key aKey) {
        if (aKey != null) {
            final CompoundKey compoundKey = (CompoundKey) aKey.myObjectValue;

            for (int index = 0; index < compoundKey.myKeys.length; index++) {
                if (compoundKey.myKeys[index] instanceof String) {
                    compoundKey.myKeys[index] = ((String) compoundKey.myKeys[index]).toLowerCase();
                }
            }
        }

        return super.checkKey(aKey);
    }

    @Override
    public boolean isCaseInsensitive() {
        return true;
    }
}
