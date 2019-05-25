
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.IValue;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.StorageError;

class Btree<T> extends PersistentCollection<T> implements Index<T> {

    static final int SIZE_OF = ObjectHeader.SIZE_OF + 4 * 4 + 1;

    static final int OP_DONE = 0;

    static final int OP_OVERFLOW = 1;

    static final int OP_UNDERFLOW = 2;

    static final int OP_NOT_FOUND = 3;

    static final int OP_DUPLICATE = 4;

    static final int OP_OVERWRITE = 5;

    transient int myUpdateCounter;

    int myRoot;

    int myHeight;

    int myType;

    int myNumOfElems;

    boolean isUniqueKeyIndex;

    Btree() {
    }

    Btree(final Class aClass, final boolean aUniqueRestriction) {
        isUniqueKeyIndex = aUniqueRestriction;
        myType = checkType(aClass);
    }

    Btree(final int aType, final boolean aUniqueRestriction) {
        myType = aType;
        isUniqueKeyIndex = aUniqueRestriction;
    }

    Btree(final byte[] aObject, final int aOffset) {
        int offset = aOffset;

        myHeight = Bytes.unpack4(aObject, offset);
        offset += 4;
        myNumOfElems = Bytes.unpack4(aObject, offset);
        offset += 4;
        myRoot = Bytes.unpack4(aObject, offset);
        offset += 4;
        myType = Bytes.unpack4(aObject, offset);
        offset += 4;
        isUniqueKeyIndex = aObject[offset] != 0;
    }

    static int checkType(final Class aClass) {
        final int elemType = ClassDescriptor.getTypeCode(aClass);

        if (elemType > ClassDescriptor.TP_OBJECT && elemType != ClassDescriptor.TP_ENUM &&
                elemType != ClassDescriptor.TP_ARRAY_OF_BYTES) {
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, aClass);
        }

        return elemType;
    }

    int compareByteArrays(final byte[] aKey, final byte[] aItem, final int aOffset, final int aLength) {
        final int n = aKey.length >= aLength ? aLength : aKey.length;

        for (int index = 0; index < n; index++) {
            final int diff = aKey[index] - aItem[index + aOffset];

            if (diff != 0) {
                return diff;
            }
        }

        return aKey.length - aLength;
    }

    @Override
    public Class[] getKeyTypes() {
        return new Class[] { getKeyType() };
    }

    @Override
    public Class getKeyType() {
        return mapKeyType(myType);
    }

    static Class mapKeyType(final int aType) {
        switch (aType) {
            case ClassDescriptor.TP_BOOLEAN:
                return boolean.class;
            case ClassDescriptor.TP_BYTE:
                return byte.class;
            case ClassDescriptor.TP_CHAR:
                return char.class;
            case ClassDescriptor.TP_SHORT:
                return short.class;
            case ClassDescriptor.TP_INT:
                return int.class;
            case ClassDescriptor.TP_LONG:
                return long.class;
            case ClassDescriptor.TP_FLOAT:
                return float.class;
            case ClassDescriptor.TP_DOUBLE:
                return double.class;
            case ClassDescriptor.TP_ENUM:
                return Enum.class;
            case ClassDescriptor.TP_STRING:
                return String.class;
            case ClassDescriptor.TP_DATE:
                return Date.class;
            case ClassDescriptor.TP_OBJECT:
                return Object.class;
            case ClassDescriptor.TP_ARRAY_OF_BYTES:
                return byte[].class;
            default:
                return Comparable.class;
        }
    }

    Key checkKey(final Key aKey) {
        Key key = aKey;

        if (key != null) {
            if (key.myType != myType) {
                throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
            }

            if (myType == ClassDescriptor.TP_OBJECT && key.myIntValue == 0 && key.myObjectValue != null) {
                final Object obj = key.myObjectValue;
                key = new Key(obj, getStorage().makePersistent(obj), key.myInclusion != 0);
            }

            if (key.myObjectValue instanceof String) {
                key = new Key(((String) key.myObjectValue).toCharArray(), key.myInclusion != 0);
            }
        }

        return key;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(final Key aKey) {
        Key key = aKey;

        key = checkKey(key);

        if (myRoot != 0) {
            final ArrayList list = new ArrayList();

            BtreePage.find((StorageImpl) getStorage(), myRoot, key, key, this, myHeight, list);

            if (list.size() > 1) {
                throw new StorageError(StorageError.KEY_NOT_UNIQUE);
            } else if (list.size() == 0) {
                return null;
            } else {
                return (T) list.get(0);
            }
        }

        return null;
    }

    @Override
    public ArrayList<T> prefixSearchList(final String aKey) {
        if (ClassDescriptor.TP_STRING != myType) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }

        final ArrayList<T> list = new ArrayList<>();

        if (myRoot != 0) {
            BtreePage.prefixSearch((StorageImpl) getStorage(), myRoot, aKey.toCharArray(), myHeight, list);
        }

        return list;
    }

    @Override
    public Object[] prefixSearch(final String aKey) {
        final ArrayList<T> list = prefixSearchList(aKey);
        return list.toArray();
    }

    @Override
    public ArrayList<T> getList(final Key aFrom, final Key aTo) {
        final ArrayList<T> list = new ArrayList<>();

        if (myRoot != 0) {
            BtreePage.find((StorageImpl) getStorage(), myRoot, checkKey(aFrom), checkKey(aTo), this, myHeight, list);
        }

        return list;
    }

    @Override
    public ArrayList<T> getList(final Object aFrom, final Object aTo) {
        return getList(getKeyFromObject(myType, aFrom), getKeyFromObject(myType, aTo));
    }

    @Override
    public T get(final Object aKey) {
        return get(getKeyFromObject(myType, aKey));
    }

    @Override
    public Object[] get(final Key aFrom, final Key aTo) {
        final ArrayList<T> list = getList(aFrom, aTo);
        return list.toArray();
    }

    @Override
    public Object[] get(final Object aFrom, final Object aTo) {
        return get(getKeyFromObject(myType, aFrom), getKeyFromObject(myType, aTo));
    }

    @Override
    public boolean put(final Key aKey, final T aObject) {
        return insert(aKey, aObject, false) >= 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T set(final Key aKey, final T aObject) {
        final int oid = insert(aKey, aObject, true);
        return (T) (oid != 0 ? ((StorageImpl) getStorage()).lookupObject(oid, null) : null);
    }

    final int insert(final Key aKey, final T aObject, final boolean aOverwrite) {
        final StorageImpl db = (StorageImpl) getStorage();

        Key key = aKey;

        if (db == null) {
            throw new StorageError(StorageError.DELETED_OBJECT);
        }

        key = checkKey(key);

        final BtreeKey ins = new BtreeKey(key, db.makePersistent(aObject));

        if (myRoot == 0) {
            myRoot = BtreePage.allocate(db, 0, myType, ins);
            myHeight = 1;
        } else {
            final int result = BtreePage.insert(db, myRoot, this, ins, myHeight, isUniqueKeyIndex, aOverwrite);

            if (result == OP_OVERFLOW) {
                myRoot = BtreePage.allocate(db, myRoot, myType, ins);
                myHeight += 1;
            } else if (result == OP_DUPLICATE) {
                return -1;
            } else if (result == OP_OVERWRITE) {
                return ins.myOldOID;
            }
        }

        myUpdateCounter += 1;
        myNumOfElems += 1;
        modify();

        return 0;
    }

    @Override
    public void remove(final Key aKey, final T aObject) {
        remove(new BtreeKey(checkKey(aKey), getStorage().getOid(aObject)));
    }

    @Override
    public boolean unlink(final Key aKey, final T aObject) {
        return removeIfExists(aKey, aObject);
    }

    boolean removeIfExists(final Key aKey, final Object aObject) {
        return removeIfExists(new BtreeKey(checkKey(aKey), getStorage().getOid(aObject)));
    }

    void remove(final BtreeKey aRemove) {
        if (!removeIfExists(aRemove)) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
    }

    boolean removeIfExists(final BtreeKey aRemove) {
        final StorageImpl db = (StorageImpl) getStorage();

        if (db == null) {
            throw new StorageError(StorageError.DELETED_OBJECT);
        }

        if (myRoot == 0) {
            return false;
        }

        final int result = BtreePage.remove(db, myRoot, this, aRemove, myHeight);

        if (result == OP_NOT_FOUND) {
            return false;
        }

        myNumOfElems -= 1;

        if (result == OP_UNDERFLOW) {
            final Page pg = db.getPage(myRoot);

            if (BtreePage.getnItems(pg) == 0) {
                int newRoot = 0;

                if (myHeight != 1) {
                    newRoot = myType == ClassDescriptor.TP_STRING || myType == ClassDescriptor.TP_ARRAY_OF_BYTES
                            ? BtreePage.getKeyStrOid(pg, 0) : BtreePage.getReference(pg, BtreePage.MAX_ITEMS - 1);
                }

                db.freePage(myRoot);
                myRoot = newRoot;
                myHeight -= 1;
            }

            db.myPool.unfix(pg);
        } else if (result == OP_OVERFLOW) {
            myRoot = BtreePage.allocate(db, myRoot, myType, aRemove);
            myHeight += 1;
        }

        myUpdateCounter += 1;
        modify();

        return true;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public T remove(final Key aKey) {
        if (!isUniqueKeyIndex) {
            throw new StorageError(StorageError.KEY_NOT_UNIQUE);
        }

        final BtreeKey rk = new BtreeKey(checkKey(aKey), 0);
        final StorageImpl db = (StorageImpl) getStorage();

        remove(rk);

        return (T) db.lookupObject(rk.myOldOID, null);
    }

    static Key getKeyFromObject(final int aType, final Object aObject) {
        if (aObject == null) {
            return null;
        }

        switch (aType) {
            case ClassDescriptor.TP_BOOLEAN:
                return new Key(((Boolean) aObject).booleanValue());
            case ClassDescriptor.TP_BYTE:
                return new Key(((Number) aObject).byteValue());
            case ClassDescriptor.TP_CHAR:
                return new Key(((Character) aObject).charValue());
            case ClassDescriptor.TP_SHORT:
                return new Key(((Number) aObject).shortValue());
            case ClassDescriptor.TP_INT:
                return new Key(((Number) aObject).intValue());
            case ClassDescriptor.TP_LONG:
                return new Key(((Number) aObject).longValue());
            case ClassDescriptor.TP_FLOAT:
                return new Key(((Number) aObject).floatValue());
            case ClassDescriptor.TP_DOUBLE:
                return new Key(((Number) aObject).doubleValue());
            case ClassDescriptor.TP_STRING:
                return new Key((String) aObject);
            case ClassDescriptor.TP_DATE:
                return new Key((java.util.Date) aObject);
            case ClassDescriptor.TP_OBJECT:
                return new Key(aObject);
            case ClassDescriptor.TP_VALUE:
                return new Key((IValue) aObject);
            case ClassDescriptor.TP_ENUM:
                return new Key((Enum) aObject);
            case ClassDescriptor.TP_ARRAY_OF_BYTES:
                return new Key((byte[]) aObject);
            default:
                throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
        }
    }

    static Key getKeyFromObject(final Object aObject) {
        if (aObject == null) {
            return null;
        } else if (aObject instanceof Byte) {
            return new Key(((Byte) aObject).byteValue());
        } else if (aObject instanceof Short) {
            return new Key(((Short) aObject).shortValue());
        } else if (aObject instanceof Integer) {
            return new Key(((Integer) aObject).intValue());
        } else if (aObject instanceof Long) {
            return new Key(((Long) aObject).longValue());
        } else if (aObject instanceof Float) {
            return new Key(((Float) aObject).floatValue());
        } else if (aObject instanceof Double) {
            return new Key(((Double) aObject).doubleValue());
        } else if (aObject instanceof Boolean) {
            return new Key(((Boolean) aObject).booleanValue());
        } else if (aObject instanceof Character) {
            return new Key(((Character) aObject).charValue());
        } else if (aObject instanceof String) {
            return new Key((String) aObject);
        } else if (aObject instanceof java.util.Date) {
            return new Key((java.util.Date) aObject);
        } else if (aObject instanceof byte[]) {
            return new Key((byte[]) aObject);
        } else if (aObject instanceof Object[]) {
            return new Key((Object[]) aObject);
        } else if (aObject instanceof Enum) {
            return new Key((Enum) aObject);
        } else if (aObject instanceof IValue) {
            return new Key((IValue) aObject);
        } else {
            return new Key(aObject);
        }
    }

    @Override
    public ArrayList<T> getPrefixList(final String aPrefix) {
        return getList(new Key(aPrefix, true), new Key(aPrefix + Character.MAX_VALUE, false));
    }

    @Override
    public Object[] getPrefix(final String aPrefix) {
        return get(new Key(aPrefix, true), new Key(aPrefix + Character.MAX_VALUE, false));
    }

    @Override
    public boolean put(final Object aKey, final T aObject) {
        return put(getKeyFromObject(myType, aKey), aObject);
    }

    @Override
    public T set(final Object aKey, final T aObject) {
        return set(getKeyFromObject(myType, aKey), aObject);
    }

    @Override
    public void remove(final Object aKey, final T aObject) {
        remove(getKeyFromObject(myType, aKey), aObject);
    }

    @Override
    public T removeKey(final Object aKey) {
        return remove(getKeyFromObject(myType, aKey));
    }

    @Override
    public T remove(final String aKey) {
        return remove(new Key(aKey));
    }

    @Override
    public int size() {
        return myNumOfElems;
    }

    @Override
    public void clear() {
        if (myRoot != 0) {
            BtreePage.purge((StorageImpl) getStorage(), myRoot, myType, myHeight);

            myRoot = 0;
            myNumOfElems = 0;
            myHeight = 0;
            myUpdateCounter += 1;
            modify();
        }
    }

    @Override
    public Object[] toArray() {
        final Object[] array = new Object[myNumOfElems];

        if (myRoot != 0) {
            BtreePage.traverseForward((StorageImpl) getStorage(), myRoot, myType, myHeight, array, 0);
        }

        return array;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E> E[] toArray(final E[] aArray) {
        E[] array = aArray;

        if (array.length < myNumOfElems) {
            array = (E[]) Array.newInstance(array.getClass().getComponentType(), myNumOfElems);
        }

        if (myRoot != 0) {
            BtreePage.traverseForward((StorageImpl) getStorage(), myRoot, myType, myHeight, array, 0);
        }

        if (array.length > myNumOfElems) {
            array[myNumOfElems] = null;
        }

        return array;
    }

    @Override
    public void deallocate() {
        if (myRoot != 0) {
            BtreePage.purge((StorageImpl) getStorage(), myRoot, myType, myHeight);
        }

        super.deallocate();
    }

    public int markTree() {
        if (myRoot != 0) {
            return BtreePage.markPage((StorageImpl) getStorage(), myRoot, myType, myHeight);
        }

        return 0;
    }

    protected Object unpackEnum(final int aValue) {
        // Base B-Tree class has no info about particular enum type so it is not able to correctly unpack enum key
        return aValue;
    }

    public void export(final XMLExporter aExporter) throws IOException {
        if (myRoot != 0) {
            BtreePage.exportPage((StorageImpl) getStorage(), aExporter, myRoot, myType, myHeight);
        }
    }

    Object unpackKey(final StorageImpl aStorage, final Page aPage, final int aPosition) {
        final byte[] data = aPage.myData;
        final int offs = BtreePage.FIRST_KEY_OFFSET + aPosition * ClassDescriptor.SIZE_OF[myType];

        switch (myType) {
            case ClassDescriptor.TP_BOOLEAN:
                return Boolean.valueOf(data[offs] != 0);
            case ClassDescriptor.TP_BYTE:
                return Byte.valueOf(data[offs]);
            case ClassDescriptor.TP_SHORT:
                return Short.valueOf(Bytes.unpack2(data, offs));
            case ClassDescriptor.TP_CHAR:
                return Character.valueOf((char) Bytes.unpack2(data, offs));
            case ClassDescriptor.TP_INT:
                return Integer.valueOf(Bytes.unpack4(data, offs));
            case ClassDescriptor.TP_OBJECT:
                return aStorage.lookupObject(Bytes.unpack4(data, offs), null);
            case ClassDescriptor.TP_LONG:
                return Long.valueOf(Bytes.unpack8(data, offs));
            case ClassDescriptor.TP_DATE:
                return new Date(Bytes.unpack8(data, offs));
            case ClassDescriptor.TP_FLOAT:
                return Float.valueOf(Float.intBitsToFloat(Bytes.unpack4(data, offs)));
            case ClassDescriptor.TP_DOUBLE:
                return Double.valueOf(Double.longBitsToDouble(Bytes.unpack8(data, offs)));
            case ClassDescriptor.TP_ENUM:
                return unpackEnum(Bytes.unpack4(data, offs));
            case ClassDescriptor.TP_STRING:
                return unpackStrKey(aPage, aPosition);
            case ClassDescriptor.TP_ARRAY_OF_BYTES:
                return unpackByteArrayKey(aPage, aPosition);
            default:
                Assert.failed("Invalid type");
        }

        return null;
    }

    static String unpackStrKey(final Page aPage, final int aPosition) {
        final int length = BtreePage.getKeyStrSize(aPage, aPosition);

        int offset = BtreePage.FIRST_KEY_OFFSET + BtreePage.getKeyStrOffs(aPage, aPosition);

        final byte[] data = aPage.myData;
        final char[] stringVal = new char[length];

        for (int index = 0; index < length; index++) {
            stringVal[index] = (char) Bytes.unpack2(data, offset);
            offset += 2;
        }

        return new String(stringVal);
    }

    Object unpackByteArrayKey(final Page aPage, final int aPosition) {
        final int length = BtreePage.getKeyStrSize(aPage, aPosition);
        final int offset = BtreePage.FIRST_KEY_OFFSET + BtreePage.getKeyStrOffs(aPage, aPosition);
        final byte[] bytes = new byte[length];

        System.arraycopy(aPage.myData, offset, bytes, 0, length);

        return bytes;
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(null, null, ASCENT_ORDER);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator() {
        return entryIterator(null, null, ASCENT_ORDER);
    }

    final int compareByteArrays(final Key aKey, final Page aPage, final int aIndex) {
        return compareByteArrays((byte[]) aKey.myObjectValue, aPage.myData, BtreePage.getKeyStrOffs(aPage, aIndex) +
                BtreePage.FIRST_KEY_OFFSET, BtreePage.getKeyStrSize(aPage, aIndex));
    }

    @Override
    public IterableIterator<T> iterator(final Key aFrom, final Key aTo, final int aOrder) {
        return new BtreeSelectionIterator<>(checkKey(aFrom), checkKey(aTo), aOrder);
    }

    @Override
    public IterableIterator<T> prefixIterator(final String aPrefix) {
        return prefixIterator(aPrefix, ASCENT_ORDER);
    }

    @Override
    public IterableIterator<T> prefixIterator(final String aPrefix, final int aOrder) {
        return iterator(new Key(aPrefix), new Key(aPrefix + Character.MAX_VALUE, false), aOrder);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key aFrom, final Key aTo, final int aOrder) {
        return new BtreeSelectionEntryIterator(checkKey(aFrom), checkKey(aTo), aOrder);
    }

    @Override
    public IterableIterator<T> iterator(final Object aFrom, final Object aTo, final int aOrder) {
        return new BtreeSelectionIterator<>(checkKey(getKeyFromObject(myType, aFrom)), checkKey(getKeyFromObject(
                myType, aTo)), aOrder);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Object aFrom, final Object aTo,
            final int aOrder) {
        return new BtreeSelectionEntryIterator(checkKey(getKeyFromObject(myType, aFrom)), checkKey(getKeyFromObject(
                myType, aTo)), aOrder);
    }

    @Override
    public int indexOf(final Key aKey) {
        final PersistentIterator iterator = (PersistentIterator) iterator(null, aKey, DESCENT_ORDER);
        int i;

        for (i = -1; iterator.nextOID() != 0; i++) {
        }

        return i;
    }

    @Override
    public T getAt(final int aIndex) {
        final IterableIterator<Map.Entry<Object, T>> iterator;

        int index = aIndex;

        if (index < 0 || index >= myNumOfElems) {
            throw new IndexOutOfBoundsException("Position " + index + ", index size " + myNumOfElems);
        }

        if (index <= myNumOfElems / 2) {
            iterator = entryIterator(null, null, ASCENT_ORDER);

            while (--index >= 0) {
                iterator.next();
            }
        } else {
            iterator = entryIterator(null, null, DESCENT_ORDER);
            index -= myNumOfElems;

            while (++index < 0) {
                iterator.next();
            }
        }

        return iterator.next().getValue();
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final int aStart, final int aOrder) {
        return new BtreeEntryStartFromIterator(aStart, aOrder);
    }

    @Override
    public boolean isUnique() {
        return isUniqueKeyIndex;
    }

    class BtreeSelectionIterator<E> extends IterableIterator<E> implements PersistentIterator {

        int[] myPageStack;

        int[] myPositionStack;

        int myCurrentPage;

        int myCurrentPosition;

        int myStackPosition;

        int myEnd;

        Key myFrom;

        Key myTo;

        int myOrder;

        int myCounter;

        BtreeKey myNextKey;

        BtreeKey myCurrentKey;

        BtreeSelectionIterator(final Key aFrom, final Key aTo, final int aOrder) {
            myFrom = aFrom;
            myTo = aTo;
            myOrder = aOrder;
            reset();
        }

        void reset() {
            int i;
            int l;
            int r;

            myStackPosition = 0;
            myCounter = myUpdateCounter;

            if (myHeight == 0) {
                return;
            }

            int pageId = myRoot;
            final StorageImpl db = (StorageImpl) getStorage();

            if (db == null) {
                throw new StorageError(StorageError.DELETED_OBJECT);
            }

            int height = myHeight;

            myPageStack = new int[height];
            myPositionStack = new int[height];

            if (myType == ClassDescriptor.TP_STRING) {
                if (myOrder == ASCENT_ORDER) {
                    if (myFrom == null) {
                        while (--height >= 0) {
                            final Page page = db.getPage(pageId);

                            myPositionStack[myStackPosition] = 0;
                            myPageStack[myStackPosition] = pageId;

                            pageId = BtreePage.getKeyStrOid(page, 0);
                            myEnd = BtreePage.getnItems(page);
                            db.myPool.unfix(page);
                            myStackPosition += 1;
                        }
                    } else {
                        while (--height > 0) {
                            final Page page = db.getPage(pageId);

                            myPageStack[myStackPosition] = pageId;

                            l = 0;
                            r = BtreePage.getnItems(page);

                            while (l < r) {
                                i = l + r >> 1;

                                if (BtreePage.compareStr(myFrom, page, i) >= myFrom.myInclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            myPositionStack[myStackPosition] = r;
                            pageId = BtreePage.getKeyStrOid(page, r);
                            db.myPool.unfix(page);
                            myStackPosition += 1;
                        }

                        final Page page = db.getPage(pageId);

                        myPageStack[myStackPosition] = pageId;
                        l = 0;
                        myEnd = r = BtreePage.getnItems(page);

                        while (l < r) {
                            i = l + r >> 1;

                            if (BtreePage.compareStr(myFrom, page, i) >= myFrom.myInclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == myEnd) {
                            myStackPosition += 1;
                            gotoNextItem(page, r - 1);
                        } else {
                            myPositionStack[myStackPosition++] = r;
                            db.myPool.unfix(page);
                        }
                    }

                    if (myStackPosition != 0 && myTo != null) {
                        final Page pg = db.getPage(myPageStack[myStackPosition - 1]);

                        if (-BtreePage.compareStr(myTo, pg, myPositionStack[myStackPosition -
                                1]) >= myTo.myInclusion) {
                            myStackPosition = 0;
                        }

                        db.myPool.unfix(pg);
                    }
                } else { // descent order
                    if (myTo == null) {
                        while (--height > 0) {
                            final Page page = db.getPage(pageId);

                            myPageStack[myStackPosition] = pageId;
                            myPositionStack[myStackPosition] = BtreePage.getnItems(page);
                            pageId = BtreePage.getKeyStrOid(page, myPositionStack[myStackPosition]);
                            db.myPool.unfix(page);
                            myStackPosition += 1;
                        }

                        final Page page = db.getPage(pageId);

                        myPageStack[myStackPosition] = pageId;
                        myPositionStack[myStackPosition++] = BtreePage.getnItems(page) - 1;
                        db.myPool.unfix(page);
                    } else {
                        while (--height > 0) {
                            final Page page = db.getPage(pageId);

                            myPageStack[myStackPosition] = pageId;
                            l = 0;
                            r = BtreePage.getnItems(page);

                            while (l < r) {
                                i = l + r >> 1;

                                if (BtreePage.compareStr(myTo, page, i) >= 1 - myTo.myInclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            myPositionStack[myStackPosition] = r;
                            pageId = BtreePage.getKeyStrOid(page, r);
                            db.myPool.unfix(page);
                            myStackPosition += 1;
                        }

                        final Page page = db.getPage(pageId);

                        myPageStack[myStackPosition] = pageId;
                        l = 0;
                        r = BtreePage.getnItems(page);

                        while (l < r) {
                            i = l + r >> 1;

                            if (BtreePage.compareStr(myTo, page, i) >= 1 - myTo.myInclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == 0) {
                            myStackPosition += 1;
                            gotoNextItem(page, r);
                        } else {
                            myPositionStack[myStackPosition++] = r - 1;
                            db.myPool.unfix(page);
                        }
                    }

                    if (myStackPosition != 0 && myFrom != null) {
                        final Page pg = db.getPage(myPageStack[myStackPosition - 1]);

                        if (BtreePage.compareStr(myFrom, pg, myPositionStack[myStackPosition -
                                1]) >= myFrom.myInclusion) {
                            myStackPosition = 0;
                        }

                        db.myPool.unfix(pg);
                    }
                }
            } else if (myType == ClassDescriptor.TP_ARRAY_OF_BYTES) {
                if (myOrder == ASCENT_ORDER) {
                    if (myFrom == null) {
                        while (--height >= 0) {
                            final Page pg = db.getPage(pageId);

                            myPositionStack[myStackPosition] = 0;
                            myPageStack[myStackPosition] = pageId;
                            pageId = BtreePage.getKeyStrOid(pg, 0);
                            myEnd = BtreePage.getnItems(pg);
                            db.myPool.unfix(pg);
                            myStackPosition += 1;
                        }
                    } else {
                        while (--height > 0) {
                            final Page page = db.getPage(pageId);

                            myPageStack[myStackPosition] = pageId;
                            l = 0;
                            r = BtreePage.getnItems(page);

                            while (l < r) {
                                i = l + r >> 1;

                                if (compareByteArrays(myFrom, page, i) >= myFrom.myInclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            myPositionStack[myStackPosition] = r;
                            pageId = BtreePage.getKeyStrOid(page, r);
                            db.myPool.unfix(page);
                            myStackPosition += 1;
                        }

                        final Page page = db.getPage(pageId);

                        myPageStack[myStackPosition] = pageId;
                        l = 0;
                        myEnd = r = BtreePage.getnItems(page);

                        while (l < r) {
                            i = l + r >> 1;

                            if (compareByteArrays(myFrom, page, i) >= myFrom.myInclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == myEnd) {
                            myStackPosition += 1;
                            gotoNextItem(page, r - 1);
                        } else {
                            myPositionStack[myStackPosition++] = r;
                            db.myPool.unfix(page);
                        }
                    }

                    if (myStackPosition != 0 && myTo != null) {
                        final Page page = db.getPage(myPageStack[myStackPosition - 1]);

                        if (-compareByteArrays(myTo, page, myPositionStack[myStackPosition -
                                1]) >= myTo.myInclusion) {
                            myStackPosition = 0;
                        }

                        db.myPool.unfix(page);
                    }
                } else { // descent order
                    if (myTo == null) {
                        while (--height > 0) {
                            final Page page = db.getPage(pageId);

                            myPageStack[myStackPosition] = pageId;
                            myPositionStack[myStackPosition] = BtreePage.getnItems(page);
                            pageId = BtreePage.getKeyStrOid(page, myPositionStack[myStackPosition]);
                            db.myPool.unfix(page);
                            myStackPosition += 1;
                        }

                        final Page page = db.getPage(pageId);

                        myPageStack[myStackPosition] = pageId;
                        myPositionStack[myStackPosition++] = BtreePage.getnItems(page) - 1;
                        db.myPool.unfix(page);
                    } else {
                        while (--height > 0) {
                            final Page page = db.getPage(pageId);

                            myPageStack[myStackPosition] = pageId;
                            l = 0;
                            r = BtreePage.getnItems(page);

                            while (l < r) {
                                i = l + r >> 1;

                                if (compareByteArrays(myTo, page, i) >= 1 - myTo.myInclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            myPositionStack[myStackPosition] = r;
                            pageId = BtreePage.getKeyStrOid(page, r);
                            db.myPool.unfix(page);
                            myStackPosition += 1;
                        }

                        final Page page = db.getPage(pageId);

                        myPageStack[myStackPosition] = pageId;
                        l = 0;
                        r = BtreePage.getnItems(page);

                        while (l < r) {
                            i = l + r >> 1;

                            if (compareByteArrays(myTo, page, i) >= 1 - myTo.myInclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == 0) {
                            myStackPosition += 1;
                            gotoNextItem(page, r);
                        } else {
                            myPositionStack[myStackPosition++] = r - 1;
                            db.myPool.unfix(page);
                        }
                    }

                    if (myStackPosition != 0 && myFrom != null) {
                        final Page page = db.getPage(myPageStack[myStackPosition - 1]);

                        if (compareByteArrays(myFrom, page, myPositionStack[myStackPosition -
                                1]) >= myFrom.myInclusion) {
                            myStackPosition = 0;
                        }

                        db.myPool.unfix(page);
                    }
                }
            } else { // scalar type
                if (myOrder == ASCENT_ORDER) {
                    if (myFrom == null) {
                        while (--height >= 0) {
                            final Page page = db.getPage(pageId);

                            myPositionStack[myStackPosition] = 0;
                            myPageStack[myStackPosition] = pageId;
                            pageId = BtreePage.getReference(page, BtreePage.MAX_ITEMS - 1);
                            myEnd = BtreePage.getnItems(page);
                            db.myPool.unfix(page);
                            myStackPosition += 1;
                        }
                    } else {
                        while (--height > 0) {
                            final Page page = db.getPage(pageId);

                            myPageStack[myStackPosition] = pageId;
                            l = 0;
                            r = BtreePage.getnItems(page);

                            while (l < r) {
                                i = l + r >> 1;

                                if (BtreePage.compare(myFrom, page, i) >= myFrom.myInclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            myPositionStack[myStackPosition] = r;
                            pageId = BtreePage.getReference(page, BtreePage.MAX_ITEMS - 1 - r);
                            db.myPool.unfix(page);
                            myStackPosition += 1;
                        }

                        final Page page = db.getPage(pageId);

                        myPageStack[myStackPosition] = pageId;
                        l = 0;
                        r = myEnd = BtreePage.getnItems(page);

                        while (l < r) {
                            i = l + r >> 1;

                            if (BtreePage.compare(myFrom, page, i) >= myFrom.myInclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == myEnd) {
                            myStackPosition += 1;
                            gotoNextItem(page, r - 1);
                        } else {
                            myPositionStack[myStackPosition++] = r;
                            db.myPool.unfix(page);
                        }
                    }

                    if (myStackPosition != 0 && myTo != null) {
                        final Page pg = db.getPage(myPageStack[myStackPosition - 1]);

                        if (-BtreePage.compare(myTo, pg, myPositionStack[myStackPosition - 1]) >= myTo.myInclusion) {
                            myStackPosition = 0;
                        }

                        db.myPool.unfix(pg);
                    }
                } else { // descent order
                    if (myTo == null) {
                        while (--height > 0) {
                            final Page page = db.getPage(pageId);

                            myPageStack[myStackPosition] = pageId;
                            myPositionStack[myStackPosition] = BtreePage.getnItems(page);
                            pageId = BtreePage.getReference(page, BtreePage.MAX_ITEMS - 1 -
                                    myPositionStack[myStackPosition]);
                            db.myPool.unfix(page);
                            myStackPosition += 1;
                        }

                        final Page page = db.getPage(pageId);

                        myPageStack[myStackPosition] = pageId;
                        myPositionStack[myStackPosition++] = BtreePage.getnItems(page) - 1;
                        db.myPool.unfix(page);
                    } else {
                        while (--height > 0) {
                            final Page page = db.getPage(pageId);

                            myPageStack[myStackPosition] = pageId;
                            l = 0;
                            r = BtreePage.getnItems(page);

                            while (l < r) {
                                i = l + r >> 1;

                                if (BtreePage.compare(myTo, page, i) >= 1 - myTo.myInclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            myPositionStack[myStackPosition] = r;
                            pageId = BtreePage.getReference(page, BtreePage.MAX_ITEMS - 1 - r);
                            db.myPool.unfix(page);
                            myStackPosition += 1;
                        }

                        final Page page = db.getPage(pageId);

                        myPageStack[myStackPosition] = pageId;
                        l = 0;
                        r = BtreePage.getnItems(page);

                        while (l < r) {
                            i = l + r >> 1;

                            if (BtreePage.compare(myTo, page, i) >= 1 - myTo.myInclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == 0) {
                            myStackPosition += 1;
                            gotoNextItem(page, r);
                        } else {
                            myPositionStack[myStackPosition++] = r - 1;
                            db.myPool.unfix(page);
                        }
                    }

                    if (myStackPosition != 0 && myFrom != null) {
                        final Page page = db.getPage(myPageStack[myStackPosition - 1]);

                        if (BtreePage.compare(myFrom, page, myPositionStack[myStackPosition -
                                1]) >= myFrom.myInclusion) {
                            myStackPosition = 0;
                        }

                        db.myPool.unfix(page);
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (myCounter != myUpdateCounter) {
                if (((StorageImpl) getStorage()).myConcurrentIterator) {
                    refresh();
                } else {
                    throw new ConcurrentModificationException();
                }
            }

            return myStackPosition != 0;
        }

        @SuppressWarnings("unchecked")
        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final StorageImpl db = (StorageImpl) getStorage();
            final int position = myPositionStack[myStackPosition - 1];

            myCurrentPosition = position;
            myCurrentPage = myPageStack[myStackPosition - 1];

            final Page page = db.getPage(myCurrentPage);
            final E current = (E) getCurrent(page, position);

            if (db.myConcurrentIterator) {
                myCurrentKey = getCurrentKey(page, position);
            }

            gotoNextItem(page, position);

            return current;
        }

        @Override
        public int nextOID() {
            if (!hasNext()) {
                return 0;
            }

            final StorageImpl db = (StorageImpl) getStorage();
            final int position = myPositionStack[myStackPosition - 1];

            myCurrentPosition = position;
            myCurrentPage = myPageStack[myStackPosition - 1];

            final Page page = db.getPage(myCurrentPage);
            final int oid = getReference(page, position);

            if (db.myConcurrentIterator) {
                myCurrentKey = getCurrentKey(page, position);
            }

            gotoNextItem(page, position);

            return oid;
        }

        private int getReference(final Page aPage, final int aPosition) {
            return myType == ClassDescriptor.TP_STRING || myType == ClassDescriptor.TP_ARRAY_OF_BYTES ? BtreePage
                    .getKeyStrOid(aPage, aPosition) : BtreePage.getReference(aPage, BtreePage.MAX_ITEMS - 1 -
                            aPosition);
        }

        protected Object getCurrent(final Page aPage, final int aPosition) {
            final StorageImpl db = (StorageImpl) getStorage();
            return db.lookupObject(getReference(aPage, aPosition), null);
        }

        protected final void gotoNextItem(final Page aPage, final int aPosition) {
            final StorageImpl db = (StorageImpl) getStorage();

            Page page = aPage;
            int position = aPosition;

            if (myType == ClassDescriptor.TP_STRING) {
                if (myOrder == ASCENT_ORDER) {
                    if (++position == myEnd) {
                        while (--myStackPosition != 0) {
                            db.myPool.unfix(page);
                            position = myPositionStack[myStackPosition - 1];
                            page = db.getPage(myPageStack[myStackPosition - 1]);

                            if (++position <= BtreePage.getnItems(page)) {
                                myPositionStack[myStackPosition - 1] = position;

                                do {
                                    final int pageId = BtreePage.getKeyStrOid(page, position);

                                    db.myPool.unfix(page);
                                    page = db.getPage(pageId);
                                    myEnd = BtreePage.getnItems(page);
                                    myPageStack[myStackPosition] = pageId;
                                    myPositionStack[myStackPosition] = position = 0;
                                } while (++myStackPosition < myPageStack.length);

                                break;
                            }
                        }
                    } else {
                        myPositionStack[myStackPosition - 1] = position;
                    }

                    if (myStackPosition != 0 && myTo != null && -BtreePage.compareStr(myTo, page,
                            position) >= myTo.myInclusion) {
                        myStackPosition = 0;
                    }
                } else { // descent order
                    if (--position < 0) {
                        while (--myStackPosition != 0) {
                            db.myPool.unfix(page);
                            position = myPositionStack[myStackPosition - 1];
                            page = db.getPage(myPageStack[myStackPosition - 1]);

                            if (--position >= 0) {
                                myPositionStack[myStackPosition - 1] = position;

                                do {
                                    final int pageId = BtreePage.getKeyStrOid(page, position);

                                    db.myPool.unfix(page);
                                    page = db.getPage(pageId);
                                    myPageStack[myStackPosition] = pageId;
                                    myPositionStack[myStackPosition] = position = BtreePage.getnItems(page);
                                } while (++myStackPosition < myPageStack.length);

                                myPositionStack[myStackPosition - 1] = --position;
                                break;
                            }
                        }
                    } else {
                        myPositionStack[myStackPosition - 1] = position;
                    }

                    if (myStackPosition != 0 && myFrom != null && BtreePage.compareStr(myFrom, page,
                            position) >= myFrom.myInclusion) {
                        myStackPosition = 0;
                    }
                }
            } else if (myType == ClassDescriptor.TP_ARRAY_OF_BYTES) {
                if (myOrder == ASCENT_ORDER) {
                    if (++position == myEnd) {
                        while (--myStackPosition != 0) {
                            db.myPool.unfix(page);
                            position = myPositionStack[myStackPosition - 1];
                            page = db.getPage(myPageStack[myStackPosition - 1]);

                            if (++position <= BtreePage.getnItems(page)) {
                                myPositionStack[myStackPosition - 1] = position;

                                do {
                                    final int pageId = BtreePage.getKeyStrOid(page, position);

                                    db.myPool.unfix(page);
                                    page = db.getPage(pageId);
                                    myEnd = BtreePage.getnItems(page);
                                    myPageStack[myStackPosition] = pageId;
                                    myPositionStack[myStackPosition] = position = 0;
                                } while (++myStackPosition < myPageStack.length);

                                break;
                            }
                        }
                    } else {
                        myPositionStack[myStackPosition - 1] = position;
                    }

                    if (myStackPosition != 0 && myTo != null && -compareByteArrays(myTo, page,
                            position) >= myTo.myInclusion) {
                        myStackPosition = 0;
                    }
                } else { // descent order
                    if (--position < 0) {
                        while (--myStackPosition != 0) {
                            db.myPool.unfix(page);
                            position = myPositionStack[myStackPosition - 1];
                            page = db.getPage(myPageStack[myStackPosition - 1]);

                            if (--position >= 0) {
                                myPositionStack[myStackPosition - 1] = position;

                                do {
                                    final int pageId = BtreePage.getKeyStrOid(page, position);

                                    db.myPool.unfix(page);
                                    page = db.getPage(pageId);
                                    myPageStack[myStackPosition] = pageId;
                                    myPositionStack[myStackPosition] = position = BtreePage.getnItems(page);
                                } while (++myStackPosition < myPageStack.length);

                                myPositionStack[myStackPosition - 1] = --position;
                                break;
                            }
                        }
                    } else {
                        myPositionStack[myStackPosition - 1] = position;
                    }
                    if (myStackPosition != 0 && myFrom != null && compareByteArrays(myFrom, page,
                            position) >= myFrom.myInclusion) {
                        myStackPosition = 0;
                    }
                }
            } else { // scalar type
                if (myOrder == ASCENT_ORDER) {
                    if (++position == myEnd) {
                        while (--myStackPosition != 0) {
                            db.myPool.unfix(page);
                            position = myPositionStack[myStackPosition - 1];
                            page = db.getPage(myPageStack[myStackPosition - 1]);

                            if (++position <= BtreePage.getnItems(page)) {
                                myPositionStack[myStackPosition - 1] = position;

                                do {
                                    final int pageId = BtreePage.getReference(page, BtreePage.MAX_ITEMS - 1 -
                                            position);

                                    db.myPool.unfix(page);
                                    page = db.getPage(pageId);
                                    myEnd = BtreePage.getnItems(page);
                                    myPageStack[myStackPosition] = pageId;
                                    myPositionStack[myStackPosition] = position = 0;
                                } while (++myStackPosition < myPageStack.length);

                                break;
                            }
                        }
                    } else {
                        myPositionStack[myStackPosition - 1] = position;
                    }
                    if (myStackPosition != 0 && myTo != null && -BtreePage.compare(myTo, page,
                            position) >= myTo.myInclusion) {
                        myStackPosition = 0;
                    }
                } else { // descent order
                    if (--position < 0) {
                        while (--myStackPosition != 0) {
                            db.myPool.unfix(page);
                            position = myPositionStack[myStackPosition - 1];
                            page = db.getPage(myPageStack[myStackPosition - 1]);

                            if (--position >= 0) {
                                myPositionStack[myStackPosition - 1] = position;

                                do {
                                    final int pageId = BtreePage.getReference(page, BtreePage.MAX_ITEMS - 1 -
                                            position);

                                    db.myPool.unfix(page);
                                    page = db.getPage(pageId);
                                    myPageStack[myStackPosition] = pageId;
                                    myPositionStack[myStackPosition] = position = BtreePage.getnItems(page);
                                } while (++myStackPosition < myPageStack.length);

                                myPositionStack[myStackPosition - 1] = --position;
                                break;
                            }
                        }
                    } else {
                        myPositionStack[myStackPosition - 1] = position;
                    }

                    if (myStackPosition != 0 && myFrom != null && BtreePage.compare(myFrom, page,
                            position) >= myFrom.myInclusion) {
                        myStackPosition = 0;
                    }
                }
            }

            if (db.myConcurrentIterator && myStackPosition != 0) {
                myNextKey = getCurrentKey(page, position);
            }

            db.myPool.unfix(page);
        }

        private void refresh() {
            if (myStackPosition != 0) {
                if (myNextKey == null) {
                    reset();
                } else {
                    if (myOrder == ASCENT_ORDER) {
                        myFrom = myNextKey.myKey;
                    } else {
                        myTo = myNextKey.myKey;
                    }

                    final int next = myNextKey.myOID;

                    reset();

                    final StorageImpl db = (StorageImpl) getStorage();

                    while (true) {
                        final int position = myPositionStack[myStackPosition - 1];
                        final Page page = db.getPage(myPageStack[myStackPosition - 1]);
                        final int oid = myType == ClassDescriptor.TP_STRING ||
                                myType == ClassDescriptor.TP_ARRAY_OF_BYTES ? BtreePage.getKeyStrOid(page, position)
                                        : BtreePage.getReference(page, BtreePage.MAX_ITEMS - 1 - position);

                        if (oid != next) {
                            gotoNextItem(page, position);
                        } else {
                            db.myPool.unfix(page);
                            break;
                        }
                    }
                }
            }

            myCounter = myUpdateCounter;
        }

        BtreeKey getCurrentKey(final Page aPage, final int aPosition) {
            final BtreeKey key;

            switch (myType) {
                case ClassDescriptor.TP_STRING:
                    key = new BtreeKey(null, BtreePage.getKeyStrOid(aPage, aPosition));
                    key.getStr(aPage, aPosition);
                    break;
                case ClassDescriptor.TP_ARRAY_OF_BYTES:
                    key = new BtreeKey(null, BtreePage.getKeyStrOid(aPage, aPosition));
                    key.getByteArray(aPage, aPosition);
                    break;
                default:
                    key = new BtreeKey(null, BtreePage.getReference(aPage, BtreePage.MAX_ITEMS - 1 - aPosition));
                    key.extract(aPage, BtreePage.FIRST_KEY_OFFSET + aPosition * ClassDescriptor.SIZE_OF[myType],
                            myType);
            }

            return key;
        }

        @Override
        public void remove() {
            if (myCurrentPage == 0) {
                throw new NoSuchElementException();
            }

            final StorageImpl db = (StorageImpl) getStorage();

            if (!db.myConcurrentIterator) {
                if (myCounter != myUpdateCounter) {
                    throw new ConcurrentModificationException();
                }

                Page page = db.getPage(myCurrentPage);

                myCurrentKey = getCurrentKey(page, myCurrentPosition);
                db.myPool.unfix(page);

                if (myStackPosition != 0) {
                    final int position = myPositionStack[myStackPosition - 1];

                    page = db.getPage(myPageStack[myStackPosition - 1]);
                    myNextKey = getCurrentKey(page, position);
                    db.myPool.unfix(page);
                }
            }

            Btree.this.removeIfExists(myCurrentKey);

            refresh();
            myCurrentPage = 0;
        }

    }

    class BtreeSelectionEntryIterator extends BtreeSelectionIterator<Map.Entry<Object, T>> {

        BtreeSelectionEntryIterator(final Key aFrom, final Key aTo, final int aOrder) {
            super(aFrom, aTo, aOrder);
        }

        @Override
        protected Object getCurrent(final Page aPage, final int aPosition) {
            final StorageImpl storage = (StorageImpl) getStorage();

            switch (myType) {
                case ClassDescriptor.TP_STRING:
                    return new BtreeEntry<T>(storage, unpackStrKey(aPage, aPosition), BtreePage.getKeyStrOid(aPage,
                            aPosition));
                case ClassDescriptor.TP_ARRAY_OF_BYTES:
                    return new BtreeEntry<T>(storage, unpackByteArrayKey(aPage, aPosition), BtreePage.getKeyStrOid(
                            aPage, aPosition));
                default:
                    return new BtreeEntry<T>(storage, unpackKey(storage, aPage, aPosition), BtreePage.getReference(
                            aPage, BtreePage.MAX_ITEMS - 1 - aPosition));
            }
        }
    }

    class BtreeEntryStartFromIterator extends BtreeSelectionEntryIterator {

        int myStart;

        BtreeEntryStartFromIterator(final int aStart, final int aOrder) {
            super(null, null, aOrder);

            myStart = aStart;

            reset();
        }

        @Override
        void reset() {
            super.reset();

            int skip = myOrder == ASCENT_ORDER ? myStart : myNumOfElems - myStart - 1;

            while (--skip >= 0 && hasNext()) {
                next();
            }
        }

    }

    static class BtreeEntry<T> implements Map.Entry<Object, T> {

        private final Object myKey;

        private final StorageImpl myStorage;

        private final int myOID;

        BtreeEntry(final StorageImpl aStorage, final Object aKey, final int aOID) {
            myStorage = aStorage;
            myKey = aKey;
            myOID = aOID;
        }

        @Override
        public Object getKey() {
            return myKey;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T getValue() {
            return (T) myStorage.lookupObject(myOID, null);
        }

        @Override
        public T setValue(final T aValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(final Object aObject) {
            if (!(aObject instanceof Map.Entry)) {
                return false;
            }

            final Map.Entry entry = (Map.Entry) aObject;

            return (getKey() == null ? entry.getKey() == null : getKey().equals(entry.getKey())) &&
                    (getValue() == null ? entry.getValue() == null : getValue().equals(entry.getValue()));
        }

        @Override
        public int hashCode() {
            return (getKey() == null ? 0 : getKey().hashCode()) ^ (getValue() == null ? 0 : getValue().hashCode());
        }

        @Override
        public String toString() {
            return getKey() + "=" + getValue();
        }

    }
}
