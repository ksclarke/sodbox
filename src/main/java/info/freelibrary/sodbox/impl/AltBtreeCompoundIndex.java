
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
                final int diff = ((Comparable) keys[i]).compareTo(c.keys[i]);
                if (diff != 0) {
                    return diff;
                }
            }
            return 0; // allow to compare part of the compound key
        }
    }

    static int getCompoundKeyComponentType(final Class c) {
        if (c.equals(Boolean.class)) {
            return ClassDescriptor.tpBoolean;
        } else if (c.equals(Byte.class)) {
            return ClassDescriptor.tpByte;
        } else if (c.equals(Character.class)) {
            return ClassDescriptor.tpChar;
        } else if (c.equals(Short.class)) {
            return ClassDescriptor.tpShort;
        } else if (c.equals(Integer.class)) {
            return ClassDescriptor.tpInt;
        } else if (c.equals(Long.class)) {
            return ClassDescriptor.tpLong;
        } else if (c.equals(Float.class)) {
            return ClassDescriptor.tpFloat;
        } else if (c.equals(Double.class)) {
            return ClassDescriptor.tpDouble;
        } else if (c.equals(String.class)) {
            return ClassDescriptor.tpString;
        } else if (c.equals(Date.class)) {
            return ClassDescriptor.tpDate;
        } else {
            return ClassDescriptor.tpObject;
        }
    }

    int[] types;

    AltBtreeCompoundIndex() {
    }

    AltBtreeCompoundIndex(final Class[] keyTypes, final boolean unique) {
        this.unique = unique;
        type = ClassDescriptor.tpValue;
        types = new int[keyTypes.length];
        for (int i = 0; i < keyTypes.length; i++) {
            types[i] = getCompoundKeyComponentType(keyTypes[i]);
        }
    }

    private Key convertKey(final Key key) {
        return convertKey(key, true);
    }

    private Key convertKey(final Key key, final boolean prefix) {
        if (key == null) {
            return null;
        }
        if (key.type != ClassDescriptor.tpArrayOfObject) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        Object[] keyComponents = (Object[]) key.oval;
        if (!prefix && keyComponents.length != types.length || keyComponents.length > types.length) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        boolean isCopy = false;
        for (int i = 0; i < keyComponents.length; i++) {
            final int type = types[i];
            if (type == ClassDescriptor.tpObject || type == ClassDescriptor.tpBoolean) {
                if (!isCopy) {
                    final Object[] newKeyComponents = new Object[keyComponents.length];
                    System.arraycopy(keyComponents, 0, newKeyComponents, 0, keyComponents.length);
                    keyComponents = newKeyComponents;
                    isCopy = true;
                }
                keyComponents[i] = type == ClassDescriptor.tpObject ? (Object) new Integer(keyComponents[i] == null
                        ? 0 : getStorage().getOid(keyComponents[i])) : (Object) new Byte(
                                (byte) (((Boolean) keyComponents[i]).booleanValue() ? 1 : 0));

            }
        }
        return new Key(new CompoundKey(keyComponents), key.inclusion != 0);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key from, final Key till, final int order) {
        return super.entryIterator(convertKey(from), convertKey(till), order);
    }

    @Override
    public T get(final Key key) {
        return super.get(convertKey(key));
    }

    @Override
    public Class[] getKeyTypes() {
        final Class[] keyTypes = new Class[types.length];
        for (int i = 0; i < keyTypes.length; i++) {
            keyTypes[i] = mapKeyType(types[i]);
        }
        return keyTypes;
    }

    @Override
    public ArrayList<T> getList(final Key from, final Key till) {
        return super.getList(convertKey(from), convertKey(till));
    }

    @Override
    public IterableIterator<T> iterator(final Key from, final Key till, final int order) {
        return super.iterator(convertKey(from), convertKey(till), order);
    }

    @Override
    public boolean put(final Key key, final T obj) {
        return super.put(convertKey(key, false), obj);
    }

    @Override
    public T remove(final Key key) {
        return super.remove(convertKey(key, false));
    }

    @Override
    public void remove(final Key key, final T obj) {
        super.remove(convertKey(key, false), obj);
    }

    @Override
    public T set(final Key key, final T obj) {
        return super.set(convertKey(key, false), obj);
    }
}
