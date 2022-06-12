
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.IValue;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.StorageError;

class BtreeCompoundIndex<T> extends Btree<T> implements Index<T> {

    int[] myTypes;

    BtreeCompoundIndex() {
    }

    BtreeCompoundIndex(final Class[] aKeyTypesArray, final boolean aUniqueRestriction) {
        isUniqueKeyIndex = aUniqueRestriction;
        myType = ClassDescriptor.TP_ARRAY_OF_BYTES;
        myTypes = new int[aKeyTypesArray.length];

        for (int index = 0; index < aKeyTypesArray.length; index++) {
            myTypes[index] = getCompoundKeyComponentType(aKeyTypesArray[index]);
        }
    }

    BtreeCompoundIndex(final int[] aTypes, final boolean aUniqueRestriction) {
        myTypes = aTypes;
        isUniqueKeyIndex = aUniqueRestriction;
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
        } else if (aClass.equals(byte[].class)) {
            return ClassDescriptor.TP_ARRAY_OF_BYTES;
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

    @Override
    int compareByteArrays(final byte[] aKey, final byte[] aItem, final int aOffset, final int aLength) {
        final byte[] a1 = aKey;
        final byte[] a2 = aItem;

        int o1 = 0;
        int o2 = aOffset;

        for (int index = 0; index < myTypes.length && o1 < aKey.length; index++) {
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
                    final int len1 = Bytes.unpack4(a1, o1);
                    final int len2 = Bytes.unpack4(a2, o2);

                    o1 += 4;
                    o2 += 4;

                    int len = len1 < len2 ? len1 : len2;

                    while (--len >= 0) {
                        diff = a1[o1++] - a2[o2++];

                        if (diff != 0) {
                            return diff;
                        }
                    }

                    diff = len1 - len2;
                    break;
                }
                default:
                    Assert.failed("Invalid type: " + myTypes[index]);
            }

            if (diff != 0) {
                return diff;
            }
        }

        return 0;
    }

    @Override
    Object unpackByteArrayKey(final Page aPage, final int aPosition) {
        int offs = BtreePage.FIRST_KEY_OFFSET + BtreePage.getKeyStrOffs(aPage, aPosition);

        final byte[] data = aPage.myData;
        final Object values[] = new Object[myTypes.length];

        for (int index = 0; index < myTypes.length; index++) {
            Object v = null;

            switch (myTypes[index]) {
                case ClassDescriptor.TP_BOOLEAN:
                    v = Boolean.valueOf(data[offs++] != 0);
                    break;
                case ClassDescriptor.TP_BYTE:
                    v = new Byte(data[offs++]);
                    break;
                case ClassDescriptor.TP_SHORT:
                    v = new Short(Bytes.unpack2(data, offs));
                    offs += 2;
                    break;
                case ClassDescriptor.TP_CHAR:
                    v = new Character((char) Bytes.unpack2(data, offs));
                    offs += 2;
                    break;
                case ClassDescriptor.TP_INT:
                    v = new Integer(Bytes.unpack4(data, offs));
                    offs += 4;
                    break;
                case ClassDescriptor.TP_OBJECT: {
                    final int oid = Bytes.unpack4(data, offs);
                    v = oid == 0 ? null : ((StorageImpl) getStorage()).lookupObject(oid, null);
                    offs += 4;
                    break;
                }
                case ClassDescriptor.TP_LONG:
                    v = new Long(Bytes.unpack8(data, offs));
                    offs += 8;
                    break;
                case ClassDescriptor.TP_DATE: {
                    final long msec = Bytes.unpack8(data, offs);
                    v = msec == -1 ? null : new Date(msec);
                    offs += 8;
                    break;
                }
                case ClassDescriptor.TP_FLOAT:
                    v = new Float(Float.intBitsToFloat(Bytes.unpack4(data, offs)));
                    offs += 4;
                    break;
                case ClassDescriptor.TP_DOUBLE:
                    v = new Double(Double.longBitsToDouble(Bytes.unpack8(data, offs)));
                    offs += 8;
                    break;
                case ClassDescriptor.TP_STRING: {
                    final int len = Bytes.unpack4(data, offs);
                    final char[] sval = new char[len];

                    offs += 4;

                    for (int j = 0; j < len; j++) {
                        sval[j] = (char) Bytes.unpack2(data, offs);
                        offs += 2;
                    }

                    v = new String(sval);
                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_BYTES: {
                    final int len = Bytes.unpack4(data, offs);
                    final byte[] bval = new byte[len];

                    offs += 4;
                    System.arraycopy(data, offs, bval, 0, len);
                    offs += len;
                    break;
                }
                default:
                    Assert.failed("Invalid type:" + myTypes[index]);
            }

            values[index] = v;
        }

        return values;
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

        final Object[] values = (Object[]) aKey.myObjectValue;

        if (!aPrefix && values.length != myTypes.length || values.length > myTypes.length) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }

        final ByteBuffer buf = new ByteBuffer();
        int dst = 0;

        for (int index = 0; index < values.length; index++) {
            final Object v = values[index];

            switch (myTypes[index]) {
                case ClassDescriptor.TP_BOOLEAN:
                    buf.extend(dst + 1);
                    buf.myByteArray[dst++] = (byte) (((Boolean) v).booleanValue() ? 1 : 0);
                    break;
                case ClassDescriptor.TP_BYTE:
                    buf.extend(dst + 1);
                    buf.myByteArray[dst++] = ((Number) v).byteValue();
                    break;
                case ClassDescriptor.TP_SHORT:
                    buf.extend(dst + 2);
                    Bytes.pack2(buf.myByteArray, dst, ((Number) v).shortValue());
                    dst += 2;
                    break;
                case ClassDescriptor.TP_CHAR:
                    buf.extend(dst + 2);
                    Bytes.pack2(buf.myByteArray, dst, v instanceof Number ? ((Number) v).shortValue()
                            : (short) ((Character) v).charValue());
                    dst += 2;
                    break;
                case ClassDescriptor.TP_INT:
                    buf.extend(dst + 4);
                    Bytes.pack4(buf.myByteArray, dst, ((Number) v).intValue());
                    dst += 4;
                    break;
                case ClassDescriptor.TP_OBJECT:
                    buf.extend(dst + 4);
                    Bytes.pack4(buf.myByteArray, dst, v == null ? 0 : getStorage().getOid(v));
                    dst += 4;
                    break;
                case ClassDescriptor.TP_LONG:
                    buf.extend(dst + 8);
                    Bytes.pack8(buf.myByteArray, dst, ((Number) v).longValue());
                    dst += 8;
                    break;
                case ClassDescriptor.TP_DATE:
                    buf.extend(dst + 8);
                    Bytes.pack8(buf.myByteArray, dst, v == null ? -1 : ((Date) v).getTime());
                    dst += 8;
                    break;
                case ClassDescriptor.TP_FLOAT:
                    buf.extend(dst + 4);
                    Bytes.pack4(buf.myByteArray, dst, Float.floatToIntBits(((Number) v).floatValue()));
                    dst += 4;
                    break;
                case ClassDescriptor.TP_DOUBLE:
                    buf.extend(dst + 8);
                    Bytes.pack8(buf.myByteArray, dst, Double.doubleToLongBits(((Number) v).doubleValue()));
                    dst += 8;
                    break;
                case ClassDescriptor.TP_ENUM:
                    buf.extend(dst + 4);
                    Bytes.pack4(buf.myByteArray, dst, ((Enum) v).ordinal());
                    dst += 4;
                    break;
                case ClassDescriptor.TP_STRING: {
                    buf.extend(dst + 4);

                    if (v != null) {
                        final String str = (String) v;
                        final int len = str.length();

                        Bytes.pack4(buf.myByteArray, dst, len);
                        dst += 4;
                        buf.extend(dst + len * 2);

                        for (int j = 0; j < len; j++) {
                            Bytes.pack2(buf.myByteArray, dst, (short) str.charAt(j));
                            dst += 2;
                        }
                    } else {
                        Bytes.pack4(buf.myByteArray, dst, 0);
                        dst += 4;
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_BYTES: {
                    buf.extend(dst + 4);

                    if (v != null) {
                        final byte[] arr = (byte[]) v;
                        final int len = arr.length;

                        Bytes.pack4(buf.myByteArray, dst, len);
                        dst += 4;
                        buf.extend(dst + len);
                        System.arraycopy(arr, 0, buf.myByteArray, dst, len);
                        dst += len;
                    } else {
                        Bytes.pack4(buf.myByteArray, dst, 0);
                        dst += 4;
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
}
