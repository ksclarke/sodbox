
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

class BtreeCaseInsensitiveMultiFieldIndex<T> extends BtreeMultiFieldIndex<T> {

    BtreeCaseInsensitiveMultiFieldIndex() {
    }

    BtreeCaseInsensitiveMultiFieldIndex(final Class cls, final String[] fieldNames, final boolean unique) {
        super(cls, fieldNames, unique);
    }

    @Override
    String convertString(final Object s) {
        return ((String) s).toLowerCase();
    }

    @Override
    public boolean isCaseInsensitive() {
        return true;
    }
}

class BtreeMultiFieldIndex<T> extends Btree<T> implements FieldIndex<T> {

    String className;

    String[] fieldName;

    int[] types;

    transient Class cls;

    transient Field[] fld;

    BtreeMultiFieldIndex() {
    }

    BtreeMultiFieldIndex(final Class cls, final String[] fieldName, final boolean unique) {
        this.cls = cls;
        this.unique = unique;
        this.fieldName = fieldName;
        this.className = ClassDescriptor.getClassName(cls);
        locateFields();
        type = ClassDescriptor.tpArrayOfByte;
        types = new int[fieldName.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = checkType(fld[i].getType());
        }
    }

    @Override
    public boolean addAll(final Collection<? extends T> c) {
        final MultiFieldValue[] arr = new MultiFieldValue[c.size()];
        final Iterator<? extends T> e = c.iterator();
        try {
            for (int i = 0; e.hasNext(); i++) {
                final T obj = e.next();
                final Comparable[] values = new Comparable[fld.length];
                for (int j = 0; j < values.length; j++) {
                    values[j] = (Comparable) fld[j].get(obj);
                }
                arr[i] = new MultiFieldValue(obj, values);
            }
        } catch (final Exception x) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        Arrays.sort(arr);
        for (final MultiFieldValue element : arr) {
            add((T) element.obj);
        }
        return arr.length > 0;
    }

    @Override
    public void append(final T obj) {
        throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
    }

    @Override
    int compareByteArrays(final byte[] key, final byte[] item, final int offs, final int lengtn) {
        int o1 = 0;
        int o2 = offs;
        final byte[] a1 = key;
        final byte[] a2 = item;
        for (int i = 0; i < fld.length && o1 < key.length; i++) {
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

    @Override
    public boolean contains(final Object obj) {
        final Key key = extractKey(obj);
        if (unique) {
            return super.get(key) != null;
        } else {
            final Object[] mbrs = get(key, key);
            for (final Object mbr : mbrs) {
                if (mbr.equals(obj)) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public boolean containsObject(final T obj) {
        final Key key = extractKey(obj);
        if (unique) {
            return super.get(key) != null;
        } else {
            final Object[] mbrs = get(key, key);
            for (final Object mbr : mbrs) {
                if (mbr == obj) {
                    return true;
                }
            }
            return false;
        }
    }

    private Key convertKey(final Key key) {
        if (key == null) {
            return null;
        }
        if (key.type != ClassDescriptor.tpArrayOfObject) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        final Object[] values = (Object[]) key.oval;
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
                    Bytes.pack4(buf.arr, dst, getStorage().getOid(v));
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
                        final String str = convertString(v);
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

    String convertString(final Object s) {
        return (String) s;
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key from, final Key till, final int order) {
        return super.entryIterator(convertKey(from), convertKey(till), order);
    }

    private Key extractKey(final Object obj) {
        try {
            final ByteBuffer buf = new ByteBuffer();
            int dst = 0;
            for (int i = 0; i < fld.length; i++) {
                final Field f = fld[i];
                switch (types[i]) {
                    case ClassDescriptor.tpBoolean:
                        buf.extend(dst + 1);
                        buf.arr[dst++] = (byte) (f.getBoolean(obj) ? 1 : 0);
                        break;
                    case ClassDescriptor.tpByte:
                        buf.extend(dst + 1);
                        buf.arr[dst++] = f.getByte(obj);
                        break;
                    case ClassDescriptor.tpShort:
                        buf.extend(dst + 2);
                        Bytes.pack2(buf.arr, dst, f.getShort(obj));
                        dst += 2;
                        break;
                    case ClassDescriptor.tpChar:
                        buf.extend(dst + 2);
                        Bytes.pack2(buf.arr, dst, (short) f.getChar(obj));
                        dst += 2;
                        break;
                    case ClassDescriptor.tpInt:
                        buf.extend(dst + 4);
                        Bytes.pack4(buf.arr, dst, f.getInt(obj));
                        dst += 4;
                        break;
                    case ClassDescriptor.tpObject: {
                        final Object p = f.get(obj);
                        buf.extend(dst + 4);
                        Bytes.pack4(buf.arr, dst, getStorage().makePersistent(p));
                        dst += 4;
                        break;
                    }
                    case ClassDescriptor.tpLong:
                        buf.extend(dst + 8);
                        Bytes.pack8(buf.arr, dst, f.getLong(obj));
                        dst += 8;
                        break;
                    case ClassDescriptor.tpDate: {
                        final Date d = (Date) f.get(obj);
                        buf.extend(dst + 8);
                        Bytes.pack8(buf.arr, dst, d == null ? -1 : d.getTime());
                        dst += 8;
                        break;
                    }
                    case ClassDescriptor.tpFloat:
                        buf.extend(dst + 4);
                        Bytes.pack4(buf.arr, dst, Float.floatToIntBits(f.getFloat(obj)));
                        dst += 4;
                        break;
                    case ClassDescriptor.tpDouble:
                        buf.extend(dst + 8);
                        Bytes.pack8(buf.arr, dst, Double.doubleToLongBits(f.getDouble(obj)));
                        dst += 8;
                        break;
                    case ClassDescriptor.tpEnum:
                        buf.extend(dst + 4);
                        Bytes.pack4(buf.arr, dst, ((Enum) f.get(obj)).ordinal());
                        dst += 4;
                        break;
                    case ClassDescriptor.tpString: {
                        buf.extend(dst + 4);
                        final String str = convertString(f.get(obj));
                        if (str != null) {
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
                        final byte[] arr = (byte[]) f.get(obj);
                        if (arr != null) {
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
            return new Key(buf.toArray());
        } catch (final Exception x) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
    }

    @Override
    public T get(final Key key) {
        return super.get(convertKey(key));
    }

    @Override
    public T[] get(final Key from, final Key till) {
        final ArrayList list = new ArrayList();
        if (root != 0) {
            BtreePage.find((StorageImpl) getStorage(), root, convertKey(from), convertKey(till), this, height, list);
        }
        return (T[]) list.toArray((T[]) Array.newInstance(cls, list.size()));
    }

    @Override
    public Class getIndexedClass() {
        return cls;
    }

    @Override
    public Field[] getKeyFields() {
        return fld;
    }

    @Override
    public T[] getPrefix(final String prefix) {
        throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
    }

    @Override
    public boolean isCaseInsensitive() {
        return false;
    }

    @Override
    public IterableIterator<T> iterator(final Key from, final Key till, final int order) {
        return super.iterator(convertKey(from), convertKey(till), order);
    }

    private final void locateFields() {
        fld = new Field[fieldName.length];
        for (int i = 0; i < fieldName.length; i++) {
            fld[i] = ClassDescriptor.locateField(cls, fieldName[i]);
            if (fld[i] == null) {
                throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName[i]);
            }
        }
    }

    @Override
    public void onLoad() {
        cls = ClassDescriptor.loadClass(getStorage(), className);
        locateFields();
    }

    @Override
    public T[] prefixSearch(final String key) {
        throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
    }

    @Override
    public boolean put(final T obj) {
        return super.put(extractKey(obj), obj);
    }

    public IterableIterator<T> queryByExample(final T obj) {
        final Key key = extractKey(obj);
        return iterator(key, key, ASCENT_ORDER);
    }

    @Override
    public T remove(final Key key) {
        return super.remove(convertKey(key));
    }

    @Override
    public boolean remove(final Object obj) {
        return super.removeIfExists(extractKey(obj), obj);
    }

    @Override
    public T set(final T obj) {
        return super.set(extractKey(obj), obj);
    }

    @Override
    public T[] toArray() {
        final T[] arr = (T[]) Array.newInstance(cls, nElems);
        if (root != 0) {
            BtreePage.traverseForward((StorageImpl) getStorage(), root, type, height, arr, 0);
        }
        return arr;
    }

    @Override
    Object unpackByteArrayKey(final Page pg, final int pos) {
        int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, pos);
        final byte[] data = pg.data;
        final Object values[] = new Object[fld.length];

        for (int i = 0; i < fld.length; i++) {
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
                case ClassDescriptor.tpEnum:
                    v = fld[i].getType().getEnumConstants()[Bytes.unpack4(data, offs)];
                    offs += 4;
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

class MultiFieldValue implements Comparable<MultiFieldValue> {

    Comparable[] values;

    Object obj;

    MultiFieldValue(final Object obj, final Comparable[] values) {
        this.obj = obj;
        this.values = values;
    }

    @Override
    public int compareTo(final MultiFieldValue f) {
        for (int i = 0; i < values.length; i++) {
            final int diff = values[i].compareTo(f.values[i]);
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }
}
