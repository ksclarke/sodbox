
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import info.freelibrary.sodbox.IValue;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.StorageError;

class AltBtreeCompoundIndex<T> extends AltBtree<T> implements Index<T> {

    int[] myTypes;

    AltBtreeCompoundIndex() {
    }

    AltBtreeCompoundIndex(final Class[] aKeyTypesArray, final boolean aKeyIsUnique) {
        myUnique = aKeyIsUnique;
        myType = ClassDescriptor.TP_VALUE;
        myTypes = new int[aKeyTypesArray.length];

        for (int index = 0; index < aKeyTypesArray.length; index++) {
            myTypes[index] = getCompoundKeyComponentType(aKeyTypesArray[index]);
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

        Object[] keyComponents = (Object[]) aKey.myObjectValue;

        if (!aPrefix && keyComponents.length != myTypes.length || keyComponents.length > myTypes.length) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }

        boolean isCopy = false;

        for (int index = 0; index < keyComponents.length; index++) {
            final int type = myTypes[index];

            if (type == ClassDescriptor.TP_OBJECT || type == ClassDescriptor.TP_BOOLEAN) {
                if (!isCopy) {
                    final Object[] newKeyComponents = new Object[keyComponents.length];

                    System.arraycopy(keyComponents, 0, newKeyComponents, 0, keyComponents.length);

                    keyComponents = newKeyComponents;
                    isCopy = true;
                }

                keyComponents[index] = type == ClassDescriptor.TP_OBJECT ? (Object) new Integer(
                        keyComponents[index] == null ? 0 : getStorage().getOid(keyComponents[index]))
                        : (Object) new Byte((byte) (((Boolean) keyComponents[index]).booleanValue() ? 1 : 0));

            }
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
    public void remove(final Key aKey, final T aObject) {
        super.remove(convertKey(aKey, false), aObject);
    }

    @Override
    public T set(final Key aKey, final T aObject) {
        return super.set(convertKey(aKey, false), aObject);
    }

    @Override
    public boolean put(final Key aKey, final T aObject) {
        return super.put(convertKey(aKey, false), aObject);
    }

    @Override
    public IterableIterator<T> iterator(final Key aFrom, final Key aTo, final int aOrder) {
        return super.iterator(convertKey(aFrom), convertKey(aTo), aOrder);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key aFrom, final Key aTo, final int aOrder) {
        return super.entryIterator(convertKey(aFrom), convertKey(aTo), aOrder);
    }

    static class CompoundKey implements Comparable, IValue {

        Object[] myKeys;

        CompoundKey(final Object[] aKeysArray) {
            myKeys = aKeysArray;
        }

        @Override
        public int compareTo(final Object aObject) {
            final CompoundKey compoundKey = (CompoundKey) aObject;
            final int n = myKeys.length < compoundKey.myKeys.length ? myKeys.length : compoundKey.myKeys.length;

            for (int index = 0; index < n; index++) {
                @SuppressWarnings("unchecked")
                final int diff = ((Comparable) myKeys[index]).compareTo(compoundKey.myKeys[index]);

                if (diff != 0) {
                    return diff;
                }
            }

            return 0; // allow to compare part of the compound key
        }
    }
}
