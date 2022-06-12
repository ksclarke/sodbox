
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

class AltBtreeMultiFieldIndex<T> extends AltBtree<T> implements FieldIndex<T> {

    String myClassName;

    String[] myFieldName;

    transient Class myClass;

    transient Field[] myField;

    AltBtreeMultiFieldIndex() {
    }

    AltBtreeMultiFieldIndex(final Class aClass, final String[] aFieldName, final boolean aUnique) {
        myClass = aClass;
        myUnique = aUnique;
        myFieldName = aFieldName;
        myClassName = ClassDescriptor.getClassName(aClass);
        locateFields();
        myType = ClassDescriptor.TP_VALUE;
    }

    private void locateFields() {
        myField = new Field[myFieldName.length];

        for (int i = 0; i < myFieldName.length; i++) {
            myField[i] = ClassDescriptor.locateField(myClass, myFieldName[i]);

            if (myField[i] == null) {
                throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, myClassName + "." + myFieldName[i]);
            }
        }
    }

    @Override
    public Class getIndexedClass() {
        return myClass;
    }

    @Override
    public Field[] getKeyFields() {
        return myField;
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

    private Key extractKey(final Object aObject) {
        final Object[] keys = new Object[myField.length];

        try {
            for (int i = 0; i < keys.length; i++) {
                final Object val = myField[i].get(aObject);

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
    public boolean put(final T aObject) {
        return super.put(extractKey(aObject), aObject);
    }

    @Override
    public T set(final T aObject) {
        return super.set(extractKey(aObject), aObject);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean addAll(final Collection<? extends T> aCollection) {
        final MultiFieldValue[] multiFieldValues = new MultiFieldValue[aCollection.size()];
        final Iterator<? extends T> e = aCollection.iterator();

        try {
            for (int i = 0; e.hasNext(); i++) {
                final T obj = e.next();
                final Comparable[] values = new Comparable[myField.length];

                for (int j = 0; j < values.length; j++) {
                    values[j] = (Comparable) myField[j].get(obj);
                }

                multiFieldValues[i] = new MultiFieldValue(obj, values);
            }
        } catch (final Exception x) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }

        Arrays.sort(multiFieldValues);

        for (int i = 0; i < multiFieldValues.length; i++) {
            add((T) multiFieldValues[i].myObject);
        }

        return multiFieldValues.length > 0;
    }

    @Override
    public boolean remove(final Object aObject) {
        return super.removeIfExists(extractKey(aObject), aObject);
    }

    @Override
    public T remove(final Key aKey) {
        return super.remove(convertKey(aKey));
    }

    @Override
    public boolean containsObject(final T aObject) {
        final Key key = extractKey(aObject);

        if (myUnique) {
            return super.get(key) != null;
        } else {
            final Object[] mbrs = get(key, key);

            for (int i = 0; i < mbrs.length; i++) {
                if (mbrs[i] == aObject) {
                    return true;
                }
            }

            return false;
        }
    }

    @Override
    public boolean contains(final Object aObject) {
        final Key key = extractKey(aObject);

        if (myUnique) {
            return super.get(key) != null;
        } else {
            final Object[] mbrs = get(key, key);

            for (int index = 0; index < mbrs.length; index++) {
                if (mbrs[index].equals(aObject)) {
                    return true;
                }
            }

            return false;
        }
    }

    @Override
    public void append(final T aObject) {
        throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
    }

    @SuppressWarnings("unchecked")
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

    @SuppressWarnings("unchecked")
    @Override
    public T[] toArray() {
        final T[] array = (T[]) Array.newInstance(myClass, myNumOfElems);

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
    public IterableIterator<T> iterator(final Key aFrom, final Key aTo, final int aOrder) {
        return super.iterator(convertKey(aFrom), convertKey(aTo), aOrder);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key aFrom, final Key aTo, final int aOrder) {
        return super.entryIterator(convertKey(aFrom), convertKey(aTo), aOrder);
    }

    public IterableIterator<T> queryByExample(final T aObject) {
        final Key key = extractKey(aObject);
        return iterator(key, key, ASCENT_ORDER);
    }

    @Override
    public boolean isCaseInsensitive() {
        return false;
    }

    static class CompoundKey implements Comparable, IValue {

        Object[] myKeys;

        CompoundKey(final Object[] aKeys) {
            this.myKeys = aKeys;
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compareTo(final Object aObject) {
            final CompoundKey c = (CompoundKey) aObject;
            final int n = myKeys.length < c.myKeys.length ? myKeys.length : c.myKeys.length;

            for (int index = 0; index < n; index++) {
                if (myKeys[index] != c.myKeys[index]) {
                    if (myKeys[index] == null) {
                        return -1;
                    } else if (c.myKeys[index] == null) {
                        return 1;
                    } else {
                        final int diff = ((Comparable) myKeys[index]).compareTo(c.myKeys[index]);

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

class AltBtreeCaseInsensitiveMultiFieldIndex<T> extends AltBtreeMultiFieldIndex<T> {

    AltBtreeCaseInsensitiveMultiFieldIndex() {
    }

    AltBtreeCaseInsensitiveMultiFieldIndex(final Class aClass, final String[] aFieldNames, final boolean aUnique) {
        super(aClass, aFieldNames, aUnique);
    }

    @Override
    Key checkKey(final Key aKey) {
        if (aKey != null) {
            final CompoundKey ck = (CompoundKey) aKey.myObjectValue;

            for (int i = 0; i < ck.myKeys.length; i++) {
                if (ck.myKeys[i] instanceof String) {
                    ck.myKeys[i] = ((String) ck.myKeys[i]).toLowerCase();
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
