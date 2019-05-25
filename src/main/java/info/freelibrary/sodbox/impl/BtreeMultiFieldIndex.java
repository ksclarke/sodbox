
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

class MultiFieldValue implements Comparable<MultiFieldValue> {

    Comparable[] myValues;

    Object myObject;

    MultiFieldValue(final Object aObject, final Comparable[] aValues) {
        myObject = aObject;
        myValues = aValues;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(final MultiFieldValue aMultiFieldValue) {
        for (int index = 0; index < myValues.length; index++) {
            final int diff = myValues[index].compareTo(aMultiFieldValue.myValues[index]);

            if (diff != 0) {
                return diff;
            }
        }

        return 0;
    }

}

class BtreeMultiFieldIndex<T> extends Btree<T> implements FieldIndex<T> {

    String myClassName;

    String[] myFieldName;

    int[] myTypes;

    transient Class myClass;

    transient Field[] myField;

    BtreeMultiFieldIndex() {
    }

    BtreeMultiFieldIndex(final Class aClass, final String[] aFieldName, final boolean aUniqueRestriction) {
        myClass = aClass;
        isUniqueKeyIndex = aUniqueRestriction;
        myFieldName = aFieldName;
        myClassName = ClassDescriptor.getClassName(aClass);
        locateFields();
        myType = ClassDescriptor.TP_ARRAY_OF_BYTES;
        myTypes = new int[aFieldName.length];

        for (int i = 0; i < myTypes.length; i++) {
            myTypes[i] = checkType(myField[i].getType());
        }
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

    @Override
    int compareByteArrays(final byte[] aKey, final byte[] aItem, final int aOffset, final int aLength) {
        final byte[] a1 = aKey;
        final byte[] a2 = aItem;

        int o1 = 0;
        int o2 = aOffset;

        for (int index = 0; index < myField.length && o1 < aKey.length; index++) {
            int diff = 0;

            switch (myTypes[index]) {
                case ClassDescriptor.TP_BOOLEAN:
                case ClassDescriptor.TP_BYTE:
                    diff = a1[o1++] - a2[o2++];
                    break;
                case ClassDescriptor.TP_SHORT:
                    diff = Bytes.unpack2(a1, o1) - Bytes.unpack2(a2, o2);
                    o1 += 2;
                    o2 += 2;
                    break;
                case ClassDescriptor.TP_CHAR:
                    diff = (char) Bytes.unpack2(a1, o1) - (char) Bytes.unpack2(a2, o2);
                    o1 += 2;
                    o2 += 2;
                    break;
                case ClassDescriptor.TP_INT:
                case ClassDescriptor.TP_OBJECT:
                case ClassDescriptor.TP_ENUM: {
                    final int i1 = Bytes.unpack4(a1, o1);
                    final int i2 = Bytes.unpack4(a2, o2);

                    diff = i1 < i2 ? -1 : i1 == i2 ? 0 : 1;
                    o1 += 4;
                    o2 += 4;
                    break;
                }
                case ClassDescriptor.TP_LONG:
                case ClassDescriptor.TP_DATE: {
                    final long l1 = Bytes.unpack8(a1, o1);
                    final long l2 = Bytes.unpack8(a2, o2);

                    diff = l1 < l2 ? -1 : l1 == l2 ? 0 : 1;
                    o1 += 8;
                    o2 += 8;
                    break;
                }
                case ClassDescriptor.TP_FLOAT: {
                    final float f1 = Float.intBitsToFloat(Bytes.unpack4(a1, o1));
                    final float f2 = Float.intBitsToFloat(Bytes.unpack4(a2, o2));

                    diff = f1 < f2 ? -1 : f1 == f2 ? 0 : 1;
                    o1 += 4;
                    o2 += 4;
                    break;
                }
                case ClassDescriptor.TP_DOUBLE: {
                    final double d1 = Double.longBitsToDouble(Bytes.unpack8(a1, o1));
                    final double d2 = Double.longBitsToDouble(Bytes.unpack8(a2, o2));

                    diff = d1 < d2 ? -1 : d1 == d2 ? 0 : 1;
                    o1 += 8;
                    o2 += 8;
                    break;
                }
                case ClassDescriptor.TP_STRING: {
                    final int len1 = Bytes.unpack4(a1, o1);
                    final int len2 = Bytes.unpack4(a2, o2);

                    o1 += 4;
                    o2 += 4;

                    int len = len1 < len2 ? len1 : len2;

                    while (--len >= 0) {
                        diff = (char) Bytes.unpack2(a1, o1) - (char) Bytes.unpack2(a2, o2);

                        if (diff != 0) {
                            return diff;
                        }

                        o1 += 2;
                        o2 += 2;
                    }

                    diff = len1 - len2;
                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_BYTES: {
                    final int length1 = Bytes.unpack4(a1, o1);
                    final int length2 = Bytes.unpack4(a2, o2);

                    o1 += 4;
                    o2 += 4;

                    int length = length1 < length2 ? length1 : length2;

                    while (--length >= 0) {
                        diff = a1[o1++] - a2[o2++];

                        if (diff != 0) {
                            return diff;
                        }
                    }

                    diff = length1 - length2;
                    break;
                }
                default:
                    Assert.failed("Invalid type ");
            }

            if (diff != 0) {
                return diff;
            }
        }

        return 0;
    }

    String convertString(final Object aStrValue) {
        return (String) aStrValue;
    }

    @Override
    Object unpackByteArrayKey(final Page aPage, final int aPosition) {
        int offs = BtreePage.FIRST_KEY_OFFSET + BtreePage.getKeyStrOffs(aPage, aPosition);

        final byte[] data = aPage.myData;
        final Object values[] = new Object[myField.length];

        for (int index = 0; index < myField.length; index++) {
            Object value = null;

            switch (myTypes[index]) {
                case ClassDescriptor.TP_BOOLEAN:
                    value = Boolean.valueOf(data[offs++] != 0);
                    break;
                case ClassDescriptor.TP_BYTE:
                    value = new Byte(data[offs++]);
                    break;
                case ClassDescriptor.TP_SHORT:
                    value = new Short(Bytes.unpack2(data, offs));
                    offs += 2;
                    break;
                case ClassDescriptor.TP_CHAR:
                    value = new Character((char) Bytes.unpack2(data, offs));
                    offs += 2;
                    break;
                case ClassDescriptor.TP_INT:
                    value = new Integer(Bytes.unpack4(data, offs));
                    offs += 4;
                    break;
                case ClassDescriptor.TP_OBJECT: {
                    final int oid = Bytes.unpack4(data, offs);
                    value = oid == 0 ? null : ((StorageImpl) getStorage()).lookupObject(oid, null);
                    offs += 4;
                    break;
                }
                case ClassDescriptor.TP_LONG:
                    value = new Long(Bytes.unpack8(data, offs));
                    offs += 8;
                    break;
                case ClassDescriptor.TP_ENUM:
                    value = myField[index].getType().getEnumConstants()[Bytes.unpack4(data, offs)];
                    offs += 4;
                    break;
                case ClassDescriptor.TP_DATE: {
                    final long msec = Bytes.unpack8(data, offs);
                    value = msec == -1 ? null : new Date(msec);
                    offs += 8;
                    break;
                }
                case ClassDescriptor.TP_FLOAT:
                    value = new Float(Float.intBitsToFloat(Bytes.unpack4(data, offs)));
                    offs += 4;
                    break;
                case ClassDescriptor.TP_DOUBLE:
                    value = new Double(Double.longBitsToDouble(Bytes.unpack8(data, offs)));
                    offs += 8;
                    break;
                case ClassDescriptor.TP_STRING: {
                    final int length = Bytes.unpack4(data, offs);
                    final char[] chars = new char[length];

                    offs += 4;

                    for (int j = 0; j < length; j++) {
                        chars[j] = (char) Bytes.unpack2(data, offs);
                        offs += 2;
                    }

                    value = new String(chars);
                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_BYTES: {
                    final int length = Bytes.unpack4(data, offs);
                    final byte[] bytes = new byte[length];

                    offs += 4;

                    System.arraycopy(data, offs, bytes, 0, length);
                    offs += length;
                    break;
                }
                default:
                    Assert.failed("Invalid type  ");
            }

            values[index] = value;
        }

        return values;
    }

    private Key extractKey(final Object aObject) {
        try {
            final ByteBuffer buf = new ByteBuffer();

            int dest = 0;

            for (int index = 0; index < myField.length; index++) {
                final Field field = myField[index];

                switch (myTypes[index]) {
                    case ClassDescriptor.TP_BOOLEAN:
                        buf.extend(dest + 1);
                        buf.myByteArray[dest++] = (byte) (field.getBoolean(aObject) ? 1 : 0);
                        break;
                    case ClassDescriptor.TP_BYTE:
                        buf.extend(dest + 1);
                        buf.myByteArray[dest++] = field.getByte(aObject);
                        break;
                    case ClassDescriptor.TP_SHORT:
                        buf.extend(dest + 2);
                        Bytes.pack2(buf.myByteArray, dest, field.getShort(aObject));
                        dest += 2;
                        break;
                    case ClassDescriptor.TP_CHAR:
                        buf.extend(dest + 2);
                        Bytes.pack2(buf.myByteArray, dest, (short) field.getChar(aObject));
                        dest += 2;
                        break;
                    case ClassDescriptor.TP_INT:
                        buf.extend(dest + 4);
                        Bytes.pack4(buf.myByteArray, dest, field.getInt(aObject));
                        dest += 4;
                        break;
                    case ClassDescriptor.TP_OBJECT: {
                        final Object obj = field.get(aObject);

                        buf.extend(dest + 4);
                        Bytes.pack4(buf.myByteArray, dest, getStorage().makePersistent(obj));
                        dest += 4;
                        break;
                    }
                    case ClassDescriptor.TP_LONG:
                        buf.extend(dest + 8);
                        Bytes.pack8(buf.myByteArray, dest, field.getLong(aObject));
                        dest += 8;
                        break;
                    case ClassDescriptor.TP_DATE: {
                        final Date date = (Date) field.get(aObject);
                        buf.extend(dest + 8);
                        Bytes.pack8(buf.myByteArray, dest, date == null ? -1 : date.getTime());
                        dest += 8;
                        break;
                    }
                    case ClassDescriptor.TP_FLOAT:
                        buf.extend(dest + 4);
                        Bytes.pack4(buf.myByteArray, dest, Float.floatToIntBits(field.getFloat(aObject)));
                        dest += 4;
                        break;
                    case ClassDescriptor.TP_DOUBLE:
                        buf.extend(dest + 8);
                        Bytes.pack8(buf.myByteArray, dest, Double.doubleToLongBits(field.getDouble(aObject)));
                        dest += 8;
                        break;
                    case ClassDescriptor.TP_ENUM:
                        buf.extend(dest + 4);
                        Bytes.pack4(buf.myByteArray, dest, ((Enum) field.get(aObject)).ordinal());
                        dest += 4;
                        break;
                    case ClassDescriptor.TP_STRING: {
                        buf.extend(dest + 4);

                        final String str = convertString(field.get(aObject));

                        if (str != null) {
                            final int len = str.length();

                            Bytes.pack4(buf.myByteArray, dest, len);
                            dest += 4;
                            buf.extend(dest + len * 2);

                            for (int j = 0; j < len; j++) {
                                Bytes.pack2(buf.myByteArray, dest, (short) str.charAt(j));
                                dest += 2;
                            }
                        } else {
                            Bytes.pack4(buf.myByteArray, dest, 0);
                            dest += 4;
                        }

                        break;
                    }
                    case ClassDescriptor.TP_ARRAY_OF_BYTES: {
                        buf.extend(dest + 4);

                        final byte[] arr = (byte[]) field.get(aObject);

                        if (arr != null) {
                            final int len = arr.length;

                            Bytes.pack4(buf.myByteArray, dest, len);
                            dest += 4;
                            buf.extend(dest + len);
                            System.arraycopy(arr, 0, buf.myByteArray, dest, len);
                            dest += len;
                        } else {
                            Bytes.pack4(buf.myByteArray, dest, 0);
                            dest += 4;
                        }

                        break;
                    }
                    default:
                        Assert.failed("Invalid  type");
                }
            }

            return new Key(buf.toArray());
        } catch (final Exception details) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, details);
        }
    }

    private Key convertKey(final Key aKey) {
        if (aKey == null) {
            return null;
        }

        if (aKey.myType != ClassDescriptor.TP_ARRAY_OF_OBJECTS) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }

        final Object[] values = (Object[]) aKey.myObjectValue;
        final ByteBuffer buf = new ByteBuffer();

        int dest = 0;

        for (int index = 0; index < values.length; index++) {
            final Object value = values[index];

            switch (myTypes[index]) {
                case ClassDescriptor.TP_BOOLEAN:
                    buf.extend(dest + 1);
                    buf.myByteArray[dest++] = (byte) (((Boolean) value).booleanValue() ? 1 : 0);
                    break;
                case ClassDescriptor.TP_BYTE:
                    buf.extend(dest + 1);
                    buf.myByteArray[dest++] = ((Number) value).byteValue();
                    break;
                case ClassDescriptor.TP_SHORT:
                    buf.extend(dest + 2);
                    Bytes.pack2(buf.myByteArray, dest, ((Number) value).shortValue());
                    dest += 2;
                    break;
                case ClassDescriptor.TP_CHAR:
                    buf.extend(dest + 2);
                    Bytes.pack2(buf.myByteArray, dest, value instanceof Number ? ((Number) value).shortValue()
                            : (short) ((Character) value).charValue());
                    dest += 2;
                    break;
                case ClassDescriptor.TP_INT:
                    buf.extend(dest + 4);
                    Bytes.pack4(buf.myByteArray, dest, ((Number) value).intValue());
                    dest += 4;
                    break;
                case ClassDescriptor.TP_OBJECT:
                    buf.extend(dest + 4);
                    Bytes.pack4(buf.myByteArray, dest, getStorage().getOid(value));
                    dest += 4;
                    break;
                case ClassDescriptor.TP_LONG:
                    buf.extend(dest + 8);
                    Bytes.pack8(buf.myByteArray, dest, ((Number) value).longValue());
                    dest += 8;
                    break;
                case ClassDescriptor.TP_DATE:
                    buf.extend(dest + 8);
                    Bytes.pack8(buf.myByteArray, dest, value == null ? -1 : ((Date) value).getTime());
                    dest += 8;
                    break;
                case ClassDescriptor.TP_FLOAT:
                    buf.extend(dest + 4);
                    Bytes.pack4(buf.myByteArray, dest, Float.floatToIntBits(((Number) value).floatValue()));
                    dest += 4;
                    break;
                case ClassDescriptor.TP_DOUBLE:
                    buf.extend(dest + 8);
                    Bytes.pack8(buf.myByteArray, dest, Double.doubleToLongBits(((Number) value).doubleValue()));
                    dest += 8;
                    break;
                case ClassDescriptor.TP_ENUM:
                    buf.extend(dest + 4);
                    Bytes.pack4(buf.myByteArray, dest, ((Enum) value).ordinal());
                    dest += 4;
                    break;
                case ClassDescriptor.TP_STRING: {
                    buf.extend(dest + 4);

                    if (value != null) {
                        final String str = convertString(value);
                        final int len = str.length();

                        Bytes.pack4(buf.myByteArray, dest, len);
                        dest += 4;
                        buf.extend(dest + len * 2);

                        for (int j = 0; j < len; j++) {
                            Bytes.pack2(buf.myByteArray, dest, (short) str.charAt(j));
                            dest += 2;
                        }
                    } else {
                        Bytes.pack4(buf.myByteArray, dest, 0);
                        dest += 4;
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_BYTES: {
                    buf.extend(dest + 4);

                    if (value != null) {
                        final byte[] arr = (byte[]) value;
                        final int len = arr.length;

                        Bytes.pack4(buf.myByteArray, dest, len);
                        dest += 4;
                        buf.extend(dest + len);
                        System.arraycopy(arr, 0, buf.myByteArray, dest, len);
                        dest += len;
                    } else {
                        Bytes.pack4(buf.myByteArray, dest, 0);
                        dest += 4;
                    }

                    break;
                }
                default:
                    Assert.failed("Invalid type");
            }
        }

        return new Key(buf.toArray(), aKey.myInclusion != 0);
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
        final MultiFieldValue[] array = new MultiFieldValue[aCollection.size()];
        final Iterator<? extends T> iterator = aCollection.iterator();
        try {
            for (int index = 0; iterator.hasNext(); index++) {
                final T obj = iterator.next();
                final Comparable[] values = new Comparable[myField.length];

                for (int j = 0; j < values.length; j++) {
                    values[j] = (Comparable) myField[j].get(obj);
                }

                array[index] = new MultiFieldValue(obj, values);
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

        if (isUniqueKeyIndex) {
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

        if (isUniqueKeyIndex) {
            return super.get(key) != null;
        } else {
            final Object[] mbrs = get(key, key);

            for (int i = 0; i < mbrs.length; i++) {
                if (mbrs[i].equals(aObject)) {
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

        if (myRoot != 0) {
            BtreePage.find((StorageImpl) getStorage(), myRoot, convertKey(aFrom), convertKey(aTo), this, myHeight,
                    list);
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
        final T[] arr = (T[]) Array.newInstance(myClass, myNumOfElems);

        if (myRoot != 0) {
            BtreePage.traverseForward((StorageImpl) getStorage(), myRoot, myType, myHeight, arr, 0);
        }

        return arr;
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
}

class BtreeCaseInsensitiveMultiFieldIndex<T> extends BtreeMultiFieldIndex<T> {

    BtreeCaseInsensitiveMultiFieldIndex() {
    }

    BtreeCaseInsensitiveMultiFieldIndex(final Class aClass, final String[] aFieldNames,
            final boolean aUniqueRestriction) {
        super(aClass, aFieldNames, aUniqueRestriction);
    }

    @Override
    String convertString(final Object aStringValue) {
        return ((String) aStringValue).toLowerCase();
    }

    @Override
    public boolean isCaseInsensitive() {
        return true;
    }

}
