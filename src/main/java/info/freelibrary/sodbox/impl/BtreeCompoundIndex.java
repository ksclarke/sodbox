
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
        } else if (c.equals(byte[].class)) {
            return ClassDescriptor.tpArrayOfByte;
        } else if (IValue.class.isAssignableFrom(c)) {
            return ClassDescriptor.tpValue;
        } else {
            return ClassDescriptor.tpObject;
        }
    }

    int[] types;

    BtreeCompoundIndex() {
    }

    BtreeCompoundIndex(final Class[] keyTypes, final boolean unique) {
        this.unique = unique;
        type = ClassDescriptor.tpArrayOfByte;
        types = new int[keyTypes.length];
        for (int i = 0; i < keyTypes.length; i++) {
            types[i] = getCompoundKeyComponentType(keyTypes[i]);
        }
    }

    BtreeCompoundIndex(final int[] types, final boolean unique) {
        this.types = types;
        this.unique = unique;
    }

    @Override
    int compareByteArrays(final byte[] key, final byte[] item, final int offs, final int lengtn) {
        int o1 = 0;
        int o2 = offs;
        final byte[] a1 = key;
        final byte[] a2 = item;
        for (int i = 0; i < types.length && o1 < key.length; i++) {
            int diff = 0;
            switch (types[i]) {
                case ClassDescriptor.tpBoolean:
                case ClassDescriptor.tpByte:
                    diff = a1[o1++] - a2[o2++];
                    break;
                case ClassDescriptor.tpShort:
                    diff = Bytes.unpack2(a1, o1) - Bytes.unpack2(a2, o2);
                    o1 += 2;
                    o2 += 2;
                    break;
                case ClassDescriptor.tpChar:
                    diff = (char) Bytes.unpack2(a1, o1) - (char) Bytes.unpack2(a2, o2);
                    o1 += 2;
                    o2 += 2;
                    break;
                case ClassDescriptor.tpInt:
                case ClassDescriptor.tpObject:
                case ClassDescriptor.tpEnum: {
                    final int i1 = Bytes.unpack4(a1, o1);
                    final int i2 = Bytes.unpack4(a2, o2);
                    diff = i1 < i2 ? -1 : i1 == i2 ? 0 : 1;
                    o1 += 4;
                    o2 += 4;
                    break;
                }
                case ClassDescriptor.tpLong:
                case ClassDescriptor.tpDate: {
                    final long l1 = Bytes.unpack8(a1, o1);
                    final long l2 = Bytes.unpack8(a2, o2);
                    diff = l1 < l2 ? -1 : l1 == l2 ? 0 : 1;
                    o1 += 8;
                    o2 += 8;
                    break;
                }
                case ClassDescriptor.tpFloat: {
                    final float f1 = Float.intBitsToFloat(Bytes.unpack4(a1, o1));
                    final float f2 = Float.intBitsToFloat(Bytes.unpack4(a2, o2));
                    diff = f1 < f2 ? -1 : f1 == f2 ? 0 : 1;
                    o1 += 4;
                    o2 += 4;
                    break;
                }
                case ClassDescriptor.tpDouble: {
                    final double d1 = Double.longBitsToDouble(Bytes.unpack8(a1, o1));
                    final double d2 = Double.longBitsToDouble(Bytes.unpack8(a2, o2));
                    diff = d1 < d2 ? -1 : d1 == d2 ? 0 : 1;
                    o1 += 8;
                    o2 += 8;
                    break;
                }
                case ClassDescriptor.tpString: {
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
                case ClassDescriptor.tpArrayOfByte: {
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
                    Assert.failed("Invalid type");
            }
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
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
        final Object[] values = (Object[]) key.oval;
        if (!prefix && values.length != types.length || values.length > types.length) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        final ByteBuffer buf = new ByteBuffer();
        int dst = 0;
        for (int i = 0; i < values.length; i++) {
            final Object v = values[i];
            switch (types[i]) {
                case ClassDescriptor.tpBoolean:
                    buf.extend(dst + 1);
                    buf.arr[dst++] = (byte) (((Boolean) v).booleanValue() ? 1 : 0);
                    break;
                case ClassDescriptor.tpByte:
                    buf.extend(dst + 1);
                    buf.arr[dst++] = ((Number) v).byteValue();
                    break;
                case ClassDescriptor.tpShort:
                    buf.extend(dst + 2);
                    Bytes.pack2(buf.arr, dst, ((Number) v).shortValue());
                    dst += 2;
                    break;
                case ClassDescriptor.tpChar:
                    buf.extend(dst + 2);
                    Bytes.pack2(buf.arr, dst, v instanceof Number ? ((Number) v).shortValue()
                            : (short) ((Character) v).charValue());
                    dst += 2;
                    break;
                case ClassDescriptor.tpInt:
                    buf.extend(dst + 4);
                    Bytes.pack4(buf.arr, dst, ((Number) v).intValue());
                    dst += 4;
                    break;
                case ClassDescriptor.tpObject:
                    buf.extend(dst + 4);
                    Bytes.pack4(buf.arr, dst, v == null ? 0 : getStorage().getOid(v));
                    dst += 4;
                    break;
                case ClassDescriptor.tpLong:
                    buf.extend(dst + 8);
                    Bytes.pack8(buf.arr, dst, ((Number) v).longValue());
                    dst += 8;
                    break;
                case ClassDescriptor.tpDate:
                    buf.extend(dst + 8);
                    Bytes.pack8(buf.arr, dst, v == null ? -1 : ((Date) v).getTime());
                    dst += 8;
                    break;
                case ClassDescriptor.tpFloat:
                    buf.extend(dst + 4);
                    Bytes.pack4(buf.arr, dst, Float.floatToIntBits(((Number) v).floatValue()));
                    dst += 4;
                    break;
                case ClassDescriptor.tpDouble:
                    buf.extend(dst + 8);
                    Bytes.pack8(buf.arr, dst, Double.doubleToLongBits(((Number) v).doubleValue()));
                    dst += 8;
                    break;
                case ClassDescriptor.tpEnum:
                    buf.extend(dst + 4);
                    Bytes.pack4(buf.arr, dst, ((Enum) v).ordinal());
                    dst += 4;
                    break;
                case ClassDescriptor.tpString: {
                    buf.extend(dst + 4);
                    if (v != null) {
                        final String str = (String) v;
                        final int len = str.length();
                        Bytes.pack4(buf.arr, dst, len);
                        dst += 4;
                        buf.extend(dst + len * 2);
                        for (int j = 0; j < len; j++) {
                            Bytes.pack2(buf.arr, dst, (short) str.charAt(j));
                            dst += 2;
                        }
                    } else {
                        Bytes.pack4(buf.arr, dst, 0);
                        dst += 4;
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfByte: {
                    buf.extend(dst + 4);
                    if (v != null) {
                        final byte[] arr = (byte[]) v;
                        final int len = arr.length;
                        Bytes.pack4(buf.arr, dst, len);
                        dst += 4;
                        buf.extend(dst + len);
                        System.arraycopy(arr, 0, buf.arr, dst, len);
                        dst += len;
                    } else {
                        Bytes.pack4(buf.arr, dst, 0);
                        dst += 4;
                    }
                    break;
                }
                default:
                    Assert.failed("Invalid type");
            }
        }
        return new Key(buf.toArray(), key.inclusion != 0);
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

    @Override
    Object unpackByteArrayKey(final Page pg, final int pos) {
        int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, pos);
        final byte[] data = pg.data;
        final Object values[] = new Object[types.length];

        for (int i = 0; i < types.length; i++) {
            Object v = null;
            switch (types[i]) {
                case ClassDescriptor.tpBoolean:
                    v = Boolean.valueOf(data[offs++] != 0);
                    break;
                case ClassDescriptor.tpByte:
                    v = new Byte(data[offs++]);
                    break;
                case ClassDescriptor.tpShort:
                    v = new Short(Bytes.unpack2(data, offs));
                    offs += 2;
                    break;
                case ClassDescriptor.tpChar:
                    v = new Character((char) Bytes.unpack2(data, offs));
                    offs += 2;
                    break;
                case ClassDescriptor.tpInt:
                    v = new Integer(Bytes.unpack4(data, offs));
                    offs += 4;
                    break;
                case ClassDescriptor.tpObject: {
                    final int oid = Bytes.unpack4(data, offs);
                    v = oid == 0 ? null : ((StorageImpl) getStorage()).lookupObject(oid, null);
                    offs += 4;
                    break;
                }
                case ClassDescriptor.tpLong:
                    v = new Long(Bytes.unpack8(data, offs));
                    offs += 8;
                    break;
                case ClassDescriptor.tpDate: {
                    final long msec = Bytes.unpack8(data, offs);
                    v = msec == -1 ? null : new Date(msec);
                    offs += 8;
                    break;
                }
                case ClassDescriptor.tpFloat:
                    v = new Float(Float.intBitsToFloat(Bytes.unpack4(data, offs)));
                    offs += 4;
                    break;
                case ClassDescriptor.tpDouble:
                    v = new Double(Double.longBitsToDouble(Bytes.unpack8(data, offs)));
                    offs += 8;
                    break;
                case ClassDescriptor.tpString: {
                    final int len = Bytes.unpack4(data, offs);
                    offs += 4;
                    final char[] sval = new char[len];
                    for (int j = 0; j < len; j++) {
                        sval[j] = (char) Bytes.unpack2(data, offs);
                        offs += 2;
                    }
                    v = new String(sval);
                    break;
                }
                case ClassDescriptor.tpArrayOfByte: {
                    final int len = Bytes.unpack4(data, offs);
                    offs += 4;
                    final byte[] bval = new byte[len];
                    System.arraycopy(data, offs, bval, 0, len);
                    offs += len;
                    break;
                }
                default:
                    Assert.failed("Invalid type");
            }
            values[i] = v;
        }
        return values;
    }
}
