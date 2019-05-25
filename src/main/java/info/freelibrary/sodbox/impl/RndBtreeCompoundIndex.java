
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import info.freelibrary.sodbox.IValue;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.StorageError;

class RndBtreeCompoundIndex<T> extends RndBtree<T> implements Index<T> {

    int[] myTypes;

    RndBtreeCompoundIndex() {
    }

    RndBtreeCompoundIndex(final Class[] aKeyTypes, final boolean aUnique) {
        isUniqueKeyIndex = aUnique;
        myType = ClassDescriptor.TP_VALUE;
        myTypes = new int[aKeyTypes.length];

        for (int index = 0; index < aKeyTypes.length; index++) {
            myTypes[index] = getCompoundKeyComponentType(aKeyTypes[index]);
        }
    }

    static int getCompoundKeyComponentType(final Class aClass) {
        if (aClass.equals(Boolean.class)) {
            return ClassDescriptor.TP_BOOLEAN;
        } else if (aClass.equals(Byte.class)) {
            return ClassDescriptor.TP_BYTE;
        } else if (aClass.equals(Character.class)) {
            return ClassDescriptor.TP_CHAR;
        } else if (aClass.equals(Short.class)) {
            return ClassDescriptor.TP_SHORT;
        } else if (aClass.equals(Integer.class)) {
            return ClassDescriptor.TP_INT;
        } else if (aClass.equals(Long.class)) {
            return ClassDescriptor.TP_LONG;
        } else if (aClass.equals(Float.class)) {
            return ClassDescriptor.TP_FLOAT;
        } else if (aClass.equals(Double.class)) {
            return ClassDescriptor.TP_DOUBLE;
        } else if (aClass.equals(String.class)) {
            return ClassDescriptor.TP_STRING;
        } else if (aClass.equals(Date.class)) {
            return ClassDescriptor.TP_DATE;
        } else if (IValue.class.isAssignableFrom(aClass)) {
            return ClassDescriptor.TP_VALUE;
        } else {
            return ClassDescriptor.TP_OBJECT;
        }
    }

    @Override
    public Class[] getKeyTypes() {
        final Class[] keyTypes = new Class[myTypes.length];

        for (int index = 0; index < keyTypes.length; index++) {
            keyTypes[index] = mapKeyType(myTypes[index]);
        }

        return keyTypes;
    }

    private Key convertKey(final Key aKey) {
        return convertKey(aKey, true);
    }

    private Key convertKey(final Key aKey, final boolean aPrefix) {
        if (aKey == null) {
            return null;
        }

        if (aKey.myType != ClassDescriptor.TP_ARRAY_OF_OBJECTS) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }

        final Object[] keyComponents = (Object[]) aKey.myObjectValue;

        if ((!aPrefix && keyComponents.length != myTypes.length) || keyComponents.length > myTypes.length) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }

        return new Key(new CompoundKey(keyComponents), aKey.myInclusion != 0);
    }

    @Override
    public ArrayList<T> getList(final Key aFrom, final Key aTo) {
        return super.getList(convertKey(aFrom), convertKey(aTo));
    }

    @Override
    public T get(final Key aKey) {
        return super.get(convertKey(aKey));
    }

    @Override
    public T remove(final Key aKey) {
        return super.remove(convertKey(aKey, false));
    }

    @Override
    public void remove(final Key aKey, final T aObj) {
        super.remove(convertKey(aKey, false), aObj);
    }

    @Override
    public T set(final Key aKey, final T aObj) {
        return super.set(convertKey(aKey, false), aObj);
    }

    @Override
    public boolean put(final Key aKey, final T aObj) {
        return super.put(convertKey(aKey, false), aObj);
    }

    @Override
    public IterableIterator<T> iterator(final Key aFrom, final Key aTo, final int aOrder) {
        return super.iterator(convertKey(aFrom), convertKey(aTo), aOrder);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key aFrom, final Key aTo, final int aOrder) {
        return super.entryIterator(convertKey(aFrom), convertKey(aTo), aOrder);
    }

    @Override
    public int indexOf(final Key aKey) {
        return super.indexOf(convertKey(aKey));
    }

    static class CompoundKey implements Comparable, IValue {

        Object[] myKeys;

        CompoundKey(final Object[] aKeys) {
            myKeys = aKeys;
        }

        @Override
        public int compareTo(final Object aObject) {
            final CompoundKey compoundKey = (CompoundKey) aObject;
            final int total = myKeys.length < compoundKey.myKeys.length ? myKeys.length : compoundKey.myKeys.length;

            for (int index = 0; index < total; index++) {
                final int diff = ((Comparable) myKeys[index]).compareTo(compoundKey.myKeys[index]);

                if (diff != 0) {
                    return diff;
                }
            }

            return 0; // allow to compare part of the compound key
        }

    }

}
