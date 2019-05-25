
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Constants;
import info.freelibrary.sodbox.IValue;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.MessageCodes;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;
import info.freelibrary.util.Logger;
import info.freelibrary.util.LoggerFactory;

class RndBtree<T> extends PersistentCollection<T> implements Index<T> {

    static final int OP_DONE = 0;

    static final int OP_OVERFLOW = 1;

    static final int OP_UNDERFLOW = 2;

    static final int OP_NOT_FOUND = 3;

    static final int OP_DUPLICATE = 4;

    static final int OP_OVERWRITE = 5;

    private static final Logger LOGGER = LoggerFactory.getLogger(RndBtree.class, Constants.MESSAGES);

    int myHeight;

    int myType;

    int myElementCount;

    boolean isUniqueKeyIndex;

    BtreePage myRoot;

    transient int myUpdateCounter;

    RndBtree() {
    }

    RndBtree(final int aType, final boolean aUniqueKeyIndex) {
        myType = aType;
        isUniqueKeyIndex = aUniqueKeyIndex;
    }

    RndBtree(final Class aClass, final boolean aUniqueKeyIndex) {
        isUniqueKeyIndex = aUniqueKeyIndex;
        myType = checkType(aClass);
    }

    static int checkType(final Class aClass) {
        final int elemType = ClassDescriptor.getTypeCode(aClass);

        if (elemType > ClassDescriptor.TP_OBJECT && elemType != ClassDescriptor.TP_VALUE &&
                elemType != ClassDescriptor.TP_ENUM) {
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, aClass);
        }

        return elemType;
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
            case ClassDescriptor.TP_STRING:
                return String.class;
            case ClassDescriptor.TP_DATE:
                return Date.class;
            case ClassDescriptor.TP_OBJECT:
                return Object.class;
            case ClassDescriptor.TP_VALUE:
                return IValue.class;
            case ClassDescriptor.TP_ENUM:
                return Enum.class;
            default:
                return null;
        }
    }

    @Override
    public Class[] getKeyTypes() {
        return new Class[] { getKeyType() };
    }

    @Override
    public Class getKeyType() {
        return mapKeyType(myType);
    }

    @Override
    public T get(final Key aKey) {
        final Key key = checkKey(aKey);

        if (myRoot != null) {
            final ArrayList list = new ArrayList();

            myRoot.find(key, key, myHeight, list);

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

        if (myRoot != null) {
            ((BtreePageOfString) myRoot).prefixSearch(aKey, myHeight, list);
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

        if (myRoot != null) {
            myRoot.find(checkKey(aFrom), checkKey(aTo), myHeight, list);
        }

        return list;
    }

    @Override
    public ArrayList<T> getList(final Object aFrom, final Object aTo) {
        return getList(Btree.getKeyFromObject(myType, aFrom), Btree.getKeyFromObject(myType, aTo));
    }

    @Override
    public Object[] get(final Key aFrom, final Key aTo) {
        final ArrayList<T> list = getList(aFrom, aTo);
        return list.toArray();
    }

    @Override
    public Object[] get(final Object aFrom, final Object aTo) {
        return get(Btree.getKeyFromObject(myType, aFrom), Btree.getKeyFromObject(myType, aTo));
    }

    @Override
    public boolean put(final Key aKey, final T aObj) {
        return insert(aKey, aObj, false) == null;
    }

    @Override
    public T set(final Key aKey, final T aObj) {
        return insert(aKey, aObj, true);
    }

    final void allocateRootPage(final BtreeKey aInsert, final int aHeight) {
        final Storage storage = getStorage();

        BtreePage newRoot = null;

        switch (myType) {
            case ClassDescriptor.TP_BYTE:
                newRoot = new BtreePageOfByte(storage);
                break;
            case ClassDescriptor.TP_SHORT:
                newRoot = new BtreePageOfShort(storage);
                break;
            case ClassDescriptor.TP_CHAR:
                newRoot = new BtreePageOfChar(storage);
                break;
            case ClassDescriptor.TP_BOOLEAN:
                newRoot = new BtreePageOfBoolean(storage);
                break;
            case ClassDescriptor.TP_INT:
            case ClassDescriptor.TP_ENUM:
                newRoot = new BtreePageOfInt(storage);
                break;
            case ClassDescriptor.TP_LONG:
                newRoot = new BtreePageOfLong(storage);
                break;
            case ClassDescriptor.TP_FLOAT:
                newRoot = new BtreePageOfFloat(storage);
                break;
            case ClassDescriptor.TP_DOUBLE:
                newRoot = new BtreePageOfDouble(storage);
                break;
            case ClassDescriptor.TP_OBJECT:
                newRoot = new BtreePageOfObject(storage);
                break;
            case ClassDescriptor.TP_STRING:
                newRoot = new BtreePageOfString(storage);
                break;
            case ClassDescriptor.TP_VALUE:
                newRoot = new BtreePageOfValue(storage);
                break;
            default:
                Assert.failed(LOGGER.getMessage(MessageCodes.SB_027));
        }

        newRoot.insert(aInsert, 0, aHeight);
        newRoot.myItems.setObject(1, myRoot);

        if (aHeight != 0) {
            newRoot.countChildren(1, aHeight);
        }

        newRoot.myItemCount = 1;
        myRoot = newRoot;
    }

    final T insert(final Key aKey, final T aObj, final boolean aOverwrite) {
        final BtreeKey ins = new BtreeKey(checkKey(aKey), aObj);

        if (myRoot == null) {
            allocateRootPage(ins, 0);
            myHeight = 1;
        } else {
            final int result = myRoot.insert(ins, myHeight, isUniqueKeyIndex, aOverwrite);

            if (result == OP_OVERFLOW) {
                allocateRootPage(ins, myHeight);
                myHeight += 1;
            } else if (result == OP_DUPLICATE || result == OP_OVERWRITE) {
                return (T) ins.myOldNode;
            }
        }

        myUpdateCounter += 1;
        myElementCount += 1;

        modify();

        return null;
    }

    @Override
    public void remove(final Key aKey, final T aObj) {
        remove(new BtreeKey(checkKey(aKey), aObj));
    }

    @Override
    public boolean unlink(final Key aKey, final T aObj) {
        return removeIfExists(aKey, aObj);
    }

    boolean removeIfExists(final Key aKey, final Object aObj) {
        return removeIfExists(new BtreeKey(checkKey(aKey), aObj));
    }

    void remove(final BtreeKey aBtreeKey) {
        if (!removeIfExists(aBtreeKey)) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
    }

    boolean removeIfExists(final BtreeKey aBtreeKey) {
        if (myRoot == null) {
            return false;
        }

        final int result = myRoot.remove(aBtreeKey, myHeight);

        if (result == OP_NOT_FOUND) {
            return false;
        }

        myElementCount -= 1;

        if (result == OP_UNDERFLOW) {
            if (myRoot.myItemCount == 0) {
                BtreePage newRoot = null;

                if (myHeight != 1) {
                    newRoot = (BtreePage) myRoot.myItems.get(0);
                }

                myRoot.deallocate();
                myRoot = newRoot;
                myHeight -= 1;
            }
        }

        myUpdateCounter += 1;

        modify();

        return true;
    }

    @Override
    public T remove(final Key aKey) {
        if (!isUniqueKeyIndex) {
            throw new StorageError(StorageError.KEY_NOT_UNIQUE);
        }

        final BtreeKey btreeKey = new BtreeKey(checkKey(aKey), null);

        remove(btreeKey);

        return (T) btreeKey.myOldNode;
    }

    @Override
    public T get(final Object aKey) {
        return get(Btree.getKeyFromObject(myType, aKey));
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
    public boolean put(final Object aKey, final T aObj) {
        return put(Btree.getKeyFromObject(myType, aKey), aObj);
    }

    @Override
    public T set(final Object aKey, final T aObj) {
        return set(Btree.getKeyFromObject(myType, aKey), aObj);
    }

    @Override
    public void remove(final Object aKey, final T aObj) {
        remove(Btree.getKeyFromObject(myType, aKey), aObj);
    }

    @Override
    public T remove(final String aKey) {
        return remove(new Key(aKey));
    }

    @Override
    public T removeKey(final Object aKey) {
        return removeKey(Btree.getKeyFromObject(myType, aKey));
    }

    @Override
    public int size() {
        return myElementCount;
    }

    @Override
    public void clear() {
        if (myRoot != null) {
            myRoot.purge(myHeight);
            myRoot = null;
            myElementCount = 0;
            myHeight = 0;
            myUpdateCounter += 1;

            modify();
        }
    }

    @Override
    public Object[] toArray() {
        final Object[] array = new Object[myElementCount];

        if (myRoot != null) {
            myRoot.traverseForward(myHeight, array, 0);
        }

        return array;
    }

    @Override
    public <E> E[] toArray(final E[] aArray) {
        E[] array = aArray;

        if (array.length < myElementCount) {
            array = (E[]) Array.newInstance(array.getClass().getComponentType(), myElementCount);
        }

        if (myRoot != null) {
            myRoot.traverseForward(myHeight, array, 0);
        }

        if (array.length > myElementCount) {
            array[myElementCount] = null;
        }

        return array;
    }

    @Override
    public void deallocate() {
        if (myRoot != null) {
            myRoot.purge(myHeight);
        }

        super.deallocate();
    }

    Key checkKey(final Key aKey) {
        Key key = aKey;

        if (key != null) {
            if (key.myType != myType) {
                throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
            }

            if (myType == ClassDescriptor.TP_OBJECT && aKey.myIntValue == 0 && aKey.myObjectValue != null) {
                final Object obj = aKey.myObjectValue;
                key = new Key(obj, getStorage().makePersistent(obj), aKey.myInclusion != 0);
            }

            if (aKey.myObjectValue instanceof char[]) {
                key = new Key(new String((char[]) aKey.myObjectValue), aKey.myInclusion != 0);
            }
        }

        return aKey;
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(null, null, ASCENT_ORDER);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator() {
        return entryIterator(null, null, ASCENT_ORDER);
    }

    @Override
    public IterableIterator<T> iterator(final Key aFrom, final Key aTo, final int aOrder) {
        return new BtreeSelectionIterator<>(checkKey(aFrom), checkKey(aTo), aOrder);
    }

    @Override
    public IterableIterator<T> iterator(final Object aFrom, final Object aTo, final int aOrder) {
        return new BtreeSelectionIterator<>(checkKey(Btree.getKeyFromObject(myType, aFrom)), checkKey(Btree
                .getKeyFromObject(myType, aTo)), aOrder);
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
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Object aFrom, final Object aTo,
            final int aOrder) {
        return new BtreeSelectionEntryIterator(checkKey(Btree.getKeyFromObject(myType, aFrom)), checkKey(Btree
                .getKeyFromObject(myType, aTo)), aOrder);
    }

    @Override
    public T getAt(final int aIndex) {
        if (aIndex < 0 || aIndex >= myElementCount) {
            throw new IndexOutOfBoundsException("Position " + aIndex + ", index size " + myElementCount);
        }

        return (T) myRoot.getAt(aIndex, myHeight);
    }

    @Override
    public int indexOf(final Key aKey) {
        return myRoot != null ? myRoot.indexOf(aKey, myHeight) : -1;
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final int aStart, final int aOrder) {
        return new BtreeEntryStartFromIterator(aStart, aOrder);
    }

    @Override
    public boolean isUnique() {
        return isUniqueKeyIndex;
    }

    static class BtreeKey {

        Key myKey;

        Object myNode;

        Object myOldNode;

        BtreeKey(final Key aKey, final Object aNode) {
            myKey = aKey;
            myNode = aNode;
        }
    }

    abstract static class BtreePage extends Persistent {

        static final int BTREE_PAGE_SIZE = Page.PAGE_SIZE - ObjectHeader.SIZE_OF - 4 * 4;

        int myItemCount;

        Link myItems;

        int[] myChildCount;

        BtreePage(final Storage aStorage, final int aCount) {
            super(aStorage);

            myItems = aStorage.createLink(aCount);
            myItems.setSize(aCount);
            myChildCount = new int[aCount];
        }

        BtreePage() {
        }

        abstract Object getData();

        abstract Object getKeyValue(int aIndex);

        abstract Key getKey(int aIndex);

        abstract int compare(Key aKey, int aIndex);

        abstract void insert(BtreeKey aKey, int aIndex);

        abstract BtreePage clonePage();

        void clearKeyValue(final int aIndex) {
        }

        Object getAt(final int aIndex, final int aHeight) {
            int height = aHeight;
            int index = aIndex;

            if (--height == 0) {
                return myItems.get(index);
            } else {
                int jndex;

                for (jndex = 0; index >= myChildCount[jndex]; jndex++) {
                    index -= myChildCount[jndex];
                }

                return ((BtreePage) myItems.get(jndex)).getAt(index, height);
            }
        }

        int indexOf(final Key aKey, final int aHeight) {
            final int count = myItemCount;

            int left = 0;
            int right = count;
            final int height = aHeight - 1;

            while (left < right) {
                final int index = left + right >> 1;

                if (compare(aKey, index) > 0) {
                    left = index + 1;
                } else {
                    right = index;
                }
            }

            Assert.that(right == left);

            if (height == 0) {
                return right < count && compare(aKey, right) == 0 ? right : -1;
            } else {
                int position = ((BtreePage) myItems.get(right)).indexOf(aKey, height);

                if (position >= 0) {
                    while (--right >= 0) {
                        position += myChildCount[right];
                    }
                }

                return position;
            }
        }

        boolean find(final Key aFirstKey, final Key aLastKey, final int aHeight, final ArrayList aResult) {
            final int count = myItemCount;
            final int height = aHeight - 1;

            int left = 0;
            int right = count;

            if (aFirstKey != null) {
                while (left < right) {
                    final int i = left + right >> 1;

                    if (compare(aFirstKey, i) >= aFirstKey.myInclusion) {
                        left = i + 1;
                    } else {
                        right = i;
                    }
                }

                Assert.that(right == left);
            }

            if (aLastKey != null) {
                if (height == 0) {
                    while (left < count) {
                        if (-compare(aLastKey, left) >= aLastKey.myInclusion) {
                            return false;
                        }

                        aResult.add(myItems.get(left));
                        left += 1;
                    }

                    return true;
                } else {
                    do {
                        if (!((BtreePage) myItems.get(left)).find(aFirstKey, aLastKey, height, aResult)) {
                            return false;
                        }

                        if (left == count) {
                            return true;
                        }
                    } while (compare(aLastKey, left++) >= 0);

                    return false;
                }
            }
            if (height == 0) {
                while (left < count) {
                    aResult.add(myItems.get(left));
                    left += 1;
                }
            } else {
                do {
                    if (!((BtreePage) myItems.get(left)).find(aFirstKey, aLastKey, height, aResult)) {
                        return false;
                    }
                } while (++left <= count);
            }

            return true;
        }

        static void memcpyData(final BtreePage aDestPage, final int aDestIndex, final BtreePage aSrcPage,
                final int aSrcIndex, final int aLength) {
            System.arraycopy(aSrcPage.getData(), aSrcIndex, aDestPage.getData(), aDestIndex, aLength);
        }

        static void memcpyItems(final BtreePage aDestPage, final int aDestIndex, final BtreePage aSrcPage,
                final int aSrcIndex, final int aLength) {
            System.arraycopy(aSrcPage.myItems.toRawArray(), aSrcIndex, aDestPage.myItems.toRawArray(), aDestIndex,
                    aLength);
            System.arraycopy(aSrcPage.myChildCount, aSrcIndex, aDestPage.myChildCount, aDestIndex, aLength);
        }

        static void memcpy(final BtreePage aDestPage, final int aDestIndex, final BtreePage aSrcPage,
                final int aSrcIndex, final int aLength) {
            memcpyData(aDestPage, aDestIndex, aSrcPage, aSrcIndex, aLength);
            memcpyItems(aDestPage, aDestIndex, aSrcPage, aSrcIndex, aLength);
        }

        void memset(final int aIndex, final int aLength) {
            int length = aLength;
            int index = aIndex;

            while (--length >= 0) {
                myItems.setObject(index++, null);
            }
        }

        private void countChildren(final int aIndex, final int aHeight) {
            myChildCount[aIndex] = ((BtreePage) myItems.get(aIndex)).totalCount(aHeight);
        }

        private int totalCount(final int aHeight) {
            int height = aHeight;

            if (--height == 0) {
                return myItemCount;
            } else {
                int sum = 0;

                for (int index = myItemCount; index >= 0; index--) {
                    sum += myChildCount[index];
                }

                return sum;
            }
        }

        private void insert(final BtreeKey aKey, final int aIndex, final int aHeight) {
            insert(aKey, aIndex);

            if (aHeight != 0) {
                countChildren(aIndex, aHeight);
            }
        }

        int insert(final BtreeKey aInsert, final int aHeight, final boolean aUniqueKeyIndex,
                final boolean aOverwrite) {
            final int result;

            int left = 0;
            int height = aHeight;
            int itemCount = myItemCount;
            int right = itemCount;

            final int ahead = aUniqueKeyIndex ? 1 : 0;

            while (left < right) {
                final int i = left + right >> 1;

                if (compare(aInsert.myKey, i) >= ahead) {
                    left = i + 1;
                } else {
                    right = i;
                }
            }

            Assert.that(left == right);

            /* insert before e[r] */
            if (--height != 0) {
                result = ((BtreePage) myItems.get(right)).insert(aInsert, height, aUniqueKeyIndex, aOverwrite);

                Assert.that(result != OP_NOT_FOUND);

                if (result != OP_OVERFLOW) {
                    if (result == OP_DONE) {
                        modify();
                        myChildCount[right] += 1;
                    }

                    return result;
                }

                itemCount += 1;
            } else if (right < itemCount && compare(aInsert.myKey, right) == 0) {
                if (aOverwrite) {
                    aInsert.myOldNode = myItems.get(right);
                    modify();
                    myItems.setObject(right, aInsert.myNode);

                    return OP_OVERWRITE;
                } else if (aUniqueKeyIndex) {
                    aInsert.myOldNode = myItems.get(right);

                    return OP_DUPLICATE;
                }
            }

            final int max = myItems.size();

            modify();

            if (height != 0) {
                countChildren(right, height);
            }

            if (itemCount < max) {
                memcpy(this, right + 1, this, right, itemCount - right);

                insert(aInsert, right, height);
                myItemCount += 1;

                return OP_DONE;
            } else { /* page is full then divide page */
                final BtreePage b = clonePage();

                Assert.that(itemCount == max);

                final int m = (max + 1) / 2;

                if (right < m) {
                    memcpy(b, 0, this, 0, right);
                    memcpy(b, right + 1, this, right, m - right - 1);
                    memcpy(this, 0, this, m - 1, max - m + 1);

                    b.insert(aInsert, right, height);
                } else {
                    memcpy(b, 0, this, 0, m);
                    memcpy(this, 0, this, m, right - m);
                    memcpy(this, right - m + 1, this, right, max - right);

                    insert(aInsert, right - m, height);
                }

                memset(max - m + 1, m - 1);

                aInsert.myNode = b;
                aInsert.myKey = b.getKey(m - 1);

                if (height == 0) {
                    myItemCount = max - m + 1;
                    b.myItemCount = m;
                } else {
                    b.clearKeyValue(m - 1);
                    myItemCount = max - m;
                    b.myItemCount = m - 1;
                }

                return OP_OVERFLOW;
            }
        }

        int handlePageUnderflow(final int aRight, final BtreeKey aBtreeKey, final int aHeight) {
            final BtreePage btreePageA = (BtreePage) myItems.get(aRight);

            btreePageA.modify();
            modify();

            int btreePageAItemCount = btreePageA.myItemCount;

            if (aRight < myItemCount) { // exists greater page
                final BtreePage btreePageB = (BtreePage) myItems.get(aRight + 1);

                int btreePageBItemCount = btreePageB.myItemCount;

                Assert.that(btreePageBItemCount >= btreePageAItemCount);

                if (aHeight != 1) {
                    memcpyData(btreePageA, btreePageAItemCount, this, aRight, 1);
                    btreePageAItemCount += 1;
                    btreePageBItemCount += 1;
                }

                if (btreePageAItemCount + btreePageBItemCount > myItems.size()) {
                    // reallocation of nodes between pages a and b
                    final int index = btreePageBItemCount - (btreePageAItemCount + btreePageBItemCount >> 1);

                    btreePageB.modify();

                    memcpy(btreePageA, btreePageAItemCount, btreePageB, 0, index);
                    memcpy(btreePageB, 0, btreePageB, index, btreePageBItemCount - index);
                    memcpyData(this, aRight, btreePageA, btreePageAItemCount + index - 1, 1);

                    if (aHeight != 1) {
                        btreePageA.clearKeyValue(btreePageAItemCount + index - 1);
                    }

                    btreePageB.memset(btreePageBItemCount - index, index);
                    btreePageB.myItemCount -= index;
                    btreePageA.myItemCount += index;

                    countChildren(aRight, aHeight);
                    countChildren(aRight + 1, aHeight);

                    return OP_DONE;
                } else { // merge page b to a
                    memcpy(btreePageA, btreePageAItemCount, btreePageB, 0, btreePageBItemCount);
                    btreePageB.deallocate();

                    final int mergedChildCount = myChildCount[aRight + 1];

                    memcpyData(this, aRight, this, aRight + 1, myItemCount - aRight - 1);
                    memcpyItems(this, aRight + 1, this, aRight + 2, myItemCount - aRight - 1);

                    myItems.setObject(myItemCount, null);
                    btreePageA.myItemCount += btreePageBItemCount;
                    myItemCount -= 1;
                    myChildCount[aRight] += mergedChildCount - 1;

                    return myItemCount < myItems.size() / 3 ? OP_UNDERFLOW : OP_DONE;
                }
            } else { // page b is before a
                final BtreePage btreePageB = (BtreePage) myItems.get(aRight - 1);
                int btreePageBItemCount = btreePageB.myItemCount;

                Assert.that(btreePageBItemCount >= btreePageAItemCount);

                if (aHeight != 1) {
                    btreePageAItemCount += 1;
                    btreePageBItemCount += 1;
                }

                if (btreePageAItemCount + btreePageBItemCount > myItems.size()) {
                    // reallocation of nodes between pages a and b
                    final int index = btreePageBItemCount - (btreePageAItemCount + btreePageBItemCount >> 1);

                    btreePageB.modify();

                    memcpy(btreePageA, index, btreePageA, 0, btreePageAItemCount);
                    memcpy(btreePageA, 0, btreePageB, btreePageBItemCount - index, index);

                    if (aHeight != 1) {
                        memcpyData(btreePageA, index - 1, this, aRight - 1, 1);
                    }

                    memcpyData(this, aRight - 1, btreePageB, btreePageBItemCount - index - 1, 1);

                    if (aHeight != 1) {
                        btreePageB.clearKeyValue(btreePageBItemCount - index - 1);
                    }

                    btreePageB.memset(btreePageBItemCount - index, index);
                    btreePageB.myItemCount -= index;
                    btreePageA.myItemCount += index;

                    countChildren(aRight - 1, aHeight);
                    countChildren(aRight, aHeight);

                    return OP_DONE;
                } else { // merge page b to a
                    memcpy(btreePageA, btreePageBItemCount, btreePageA, 0, btreePageAItemCount);
                    memcpy(btreePageA, 0, btreePageB, 0, btreePageBItemCount);

                    if (aHeight != 1) {
                        memcpyData(btreePageA, btreePageBItemCount - 1, this, aRight - 1, 1);
                    }

                    btreePageB.deallocate();
                    myItems.setObject(aRight - 1, btreePageA);
                    myItems.setObject(myItemCount, null);
                    myChildCount[aRight - 1] += myChildCount[aRight] - 1;
                    btreePageA.myItemCount += btreePageBItemCount;
                    myItemCount -= 1;

                    return myItemCount < myItems.size() / 3 ? OP_UNDERFLOW : OP_DONE;
                }
            }
        }

        int remove(final BtreeKey aBtreeKey, final int aHeight) {
            int itemCount = myItemCount;
            int right = itemCount;
            int height = aHeight;
            int left = 0;
            int index;

            while (left < right) {
                index = left + right >> 1;

                if (compare(aBtreeKey.myKey, index) > 0) {
                    left = index + 1;
                } else {
                    right = index;
                }
            }

            if (--height == 0) {
                final Object node = aBtreeKey.myNode;

                while (right < itemCount) {
                    if (compare(aBtreeKey.myKey, right) == 0) {
                        if (node == null || myItems.containsElement(right, node)) {
                            aBtreeKey.myOldNode = myItems.get(right);

                            modify();

                            memcpy(this, right, this, right + 1, itemCount - right - 1);
                            myItemCount = --itemCount;
                            memset(itemCount, 1);

                            return itemCount < myItems.size() / 3 ? OP_UNDERFLOW : OP_DONE;
                        }
                    } else {
                        break;
                    }

                    right += 1;
                }

                return OP_NOT_FOUND;
            }
            do {
                switch (((BtreePage) myItems.get(right)).remove(aBtreeKey, height)) {
                    case OP_UNDERFLOW:
                        return handlePageUnderflow(right, aBtreeKey, height);
                    case OP_DONE:
                        modify();
                        myChildCount[right] -= 1;
                        return OP_DONE;
                    default:
                        // FIXME: do what here?
                }
            } while (++right <= itemCount);

            return OP_NOT_FOUND;
        }

        void purge(final int aHeight) {
            int height = aHeight;

            if (--height != 0) {
                int itemCount = myItemCount;

                do {
                    ((BtreePage) myItems.get(itemCount)).purge(height);
                } while (--itemCount >= 0);
            }

            super.deallocate();
        }

        int traverseForward(final int aHeight, final Object[] aResult, final int aPosition) {
            final int itemCount = myItemCount;

            int position = aPosition;
            int height = aHeight;
            int index;

            if (--height != 0) {
                for (index = 0; index <= itemCount; index++) {
                    position = ((BtreePage) myItems.get(index)).traverseForward(height, aResult, position);
                }
            } else {
                for (index = 0; index < itemCount; index++) {
                    aResult[position++] = myItems.get(index);
                }
            }

            return position;
        }
    }

    static class BtreePageOfByte extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4 + 1);

        byte[] myData;

        BtreePageOfByte(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new byte[MAX_ITEMS];
        }

        BtreePageOfByte() {
        }

        @Override
        Object getData() {
            return myData;
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return Byte.valueOf(myData[aIndex]);
        }

        @Override
        Key getKey(final int aIndex) {
            return new Key(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfByte(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return (byte) aKey.myIntValue - myData[aIndex];
        }

        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = (byte) aKey.myKey.myIntValue;
        }
    }

    static class BtreePageOfBoolean extends BtreePageOfByte {

        BtreePageOfBoolean() {
        }

        BtreePageOfBoolean(final Storage aStorage) {
            super(aStorage);
        }

        @Override
        Key getKey(final int aIndex) {
            return new Key(myData[aIndex] != 0);
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return Boolean.valueOf(myData[aIndex] != 0);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfBoolean(getStorage());
        }
    }

    static class BtreePageOfShort extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4 + 2);

        short[] myData;

        BtreePageOfShort(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new short[MAX_ITEMS];
        }

        BtreePageOfShort() {
        }

        @Override
        Object getData() {
            return myData;
        }

        @Override
        Key getKey(final int aIndex) {
            return new Key(myData[aIndex]);
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return Short.valueOf(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfShort(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return (short) aKey.myIntValue - myData[aIndex];
        }

        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = (short) aKey.myKey.myIntValue;
        }
    }

    static class BtreePageOfChar extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4 + 2);

        char[] myData;

        BtreePageOfChar(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new char[MAX_ITEMS];
        }

        BtreePageOfChar() {
        }

        @Override
        Object getData() {
            return myData;
        }

        @Override
        Key getKey(final int aIndex) {
            return new Key(myData[aIndex]);
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return Character.valueOf(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfChar(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return (char) aKey.myIntValue - myData[aIndex];
        }

        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = (char) aKey.myKey.myIntValue;
        }
    }

    static class BtreePageOfInt extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4 + 4);

        int[] myData;

        BtreePageOfInt(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new int[MAX_ITEMS];
        }

        BtreePageOfInt() {
        }

        @Override
        Object getData() {
            return myData;
        }

        @Override
        Key getKey(final int aIndex) {
            return new Key(myData[aIndex]);
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return Integer.valueOf(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfInt(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return aKey.myIntValue < myData[aIndex] ? -1 : aKey.myIntValue == myData[aIndex] ? 0 : 1;
        }

        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = aKey.myKey.myIntValue;
        }
    }

    static class BtreePageOfLong extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4 + 8);

        long[] myData;

        BtreePageOfLong(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new long[MAX_ITEMS];
        }

        BtreePageOfLong() {
        }

        @Override
        Object getData() {
            return myData;
        }

        @Override
        Key getKey(final int aIndex) {
            return new Key(myData[aIndex]);
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return Long.valueOf(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfLong(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return aKey.myLongValue < myData[aIndex] ? -1 : aKey.myLongValue == myData[aIndex] ? 0 : 1;
        }

        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = aKey.myKey.myLongValue;
        }
    }

    static class BtreePageOfFloat extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4 + 4);

        float[] myData;

        BtreePageOfFloat(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new float[MAX_ITEMS];
        }

        BtreePageOfFloat() {
        }

        @Override
        Object getData() {
            return myData;
        }

        @Override
        Key getKey(final int aIndex) {
            return new Key(myData[aIndex]);
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return Float.valueOf(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfFloat(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return (float) aKey.myDoubleValue < myData[aIndex] ? -1 : (float) aKey.myDoubleValue == myData[aIndex] ? 0
                    : 1;
        }

        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = (float) aKey.myKey.myDoubleValue;
        }
    }

    static class BtreePageOfDouble extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4 + 8);

        double[] myData;

        BtreePageOfDouble(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new double[MAX_ITEMS];
        }

        BtreePageOfDouble() {
        }

        @Override
        Object getData() {
            return myData;
        }

        @Override
        Key getKey(final int aIndex) {
            return new Key(myData[aIndex]);
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return Double.valueOf(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfDouble(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return aKey.myDoubleValue < myData[aIndex] ? -1 : aKey.myDoubleValue == myData[aIndex] ? 0 : 1;
        }

        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = aKey.myKey.myDoubleValue;
        }
    }

    static class BtreePageOfObject extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4 + 4);

        Link myData;

        BtreePageOfObject(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = aStorage.createLink(MAX_ITEMS);
            myData.setSize(MAX_ITEMS);
        }

        BtreePageOfObject() {
        }

        @Override
        Object getData() {
            return myData.toRawArray();
        }

        @Override
        Key getKey(final int aIndex) {
            return new Key(myData.getRaw(aIndex));
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return myData.get(aIndex);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfObject(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            final Object obj = myData.getRaw(aIndex);
            final int oid = getStorage().getOid(obj);

            return aKey.myIntValue < oid ? -1 : aKey.myIntValue == oid ? 0 : 1;
        }

        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData.setObject(aIndex, aKey.myKey.myObjectValue);
        }
    }

    static class BtreePageOfString extends BtreePage {

        static final int MAX_ITEMS = 100;

        String[] myData;

        BtreePageOfString(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new String[MAX_ITEMS];
        }

        BtreePageOfString() {
        }

        @Override
        Object getData() {
            return myData;
        }

        @Override
        Key getKey(final int aIndex) {
            return new Key(myData[aIndex]);
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return myData[aIndex];
        }

        @Override
        void clearKeyValue(final int aIndex) {
            myData[aIndex] = null;
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfString(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return ((String) aKey.myObjectValue).compareTo(myData[aIndex]);
        }

        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = (String) aKey.myKey.myObjectValue;
        }

        @Override
        void memset(final int aIndex, final int aLength) {
            int index = aIndex;
            int length = aLength;

            while (--length >= 0) {
                myItems.setObject(index, null);
                myData[index] = null;
                index += 1;
            }
        }

        boolean prefixSearch(final String aKey, final int aHeight, final ArrayList aResult) {
            final int itemCount = myItemCount;
            final int height = aHeight - 1;

            int right = itemCount;
            int left = 0;

            while (left < right) {
                final int index = left + right >> 1;

                if (!aKey.startsWith(myData[index]) && aKey.compareTo(myData[index]) > 0) {
                    left = index + 1;
                } else {
                    right = index;
                }
            }

            Assert.that(right == left);

            if (height == 0) {
                while (left < itemCount) {
                    if (aKey.compareTo(myData[left]) < 0) {
                        return false;
                    }

                    aResult.add(myItems.get(left));
                    left += 1;
                }
            } else {
                do {
                    if (!((BtreePageOfString) myItems.get(left)).prefixSearch(aKey, height, aResult)) {
                        return false;
                    }

                    if (left == itemCount) {
                        return true;
                    }
                } while (aKey.compareTo(myData[left++]) >= 0);

                return false;
            }

            return true;
        }
    }

    static class BtreePageOfValue extends BtreePage {

        static final int MAX_ITEMS = 100;

        Object[] myData;

        BtreePageOfValue(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new Object[MAX_ITEMS];
        }

        BtreePageOfValue() {
        }

        @Override
        Object getData() {
            return myData;
        }

        @Override
        Key getKey(final int aIndex) {
            return new Key((IValue) myData[aIndex]);
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return myData[aIndex];
        }

        @Override
        void clearKeyValue(final int aIndex) {
            myData[aIndex] = null;
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfValue(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return ((Comparable) aKey.myObjectValue).compareTo(myData[aIndex]);
        }

        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = aKey.myKey.myObjectValue;
        }
    }

    static class BtreeEntry<T> implements Map.Entry<Object, T> {

        private final BtreePage myPage;

        private final int myPosition;

        BtreeEntry(final BtreePage aPage, final int aPosition) {
            myPage = aPage;
            myPosition = aPosition;
        }

        @Override
        public Object getKey() {
            return myPage.getKeyValue(myPosition);
        }

        @Override
        public T getValue() {
            return (T) myPage.myItems.get(myPosition);
        }

        @Override
        public T setValue(final T aValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(final Object aObj) {
            if (!(aObj instanceof Map.Entry)) {
                return false;
            }

            final Map.Entry entry = (Map.Entry) aObj;

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

    class BtreeSelectionIterator<E> extends IterableIterator<E> implements PersistentIterator {

        BtreePage[] myPageStack;

        int[] myPositionStack;

        BtreePage myCurrentPage;

        int myCurrentPosition;

        int myStackPosition;

        int myEnd;

        Key myFrom;

        Key myTo;

        int myOrder;

        int myCounter;

        BtreeKey myCurrentKey;

        Key myNextKey;

        Object myNextObj;

        BtreeSelectionIterator(final Key aFrom, final Key aTo, final int aOrder) {
            myFrom = aFrom;
            myTo = aTo;
            myOrder = aOrder;

            reset();
        }

        BtreeSelectionIterator(final int aOrder) {
            myOrder = aOrder;
        }

        void reset() {
            int index;
            int left;
            int right;

            myStackPosition = 0;
            myCounter = myUpdateCounter;

            if (myHeight == 0) {
                return;
            }

            BtreePage page = myRoot;
            int height = myHeight;

            myPageStack = new BtreePage[height];
            myPositionStack = new int[height];

            if (myOrder == ASCENT_ORDER) {
                if (myFrom == null) {
                    while (--height > 0) {
                        myPositionStack[myStackPosition] = 0;
                        myPageStack[myStackPosition] = page;
                        page = (BtreePage) page.myItems.get(0);
                        myStackPosition += 1;
                    }

                    myPositionStack[myStackPosition] = 0;
                    myPageStack[myStackPosition] = page;
                    myEnd = page.myItemCount;
                    myStackPosition += 1;
                } else {
                    while (--height > 0) {
                        myPageStack[myStackPosition] = page;
                        left = 0;
                        right = page.myItemCount;

                        while (left < right) {
                            index = left + right >> 1;

                            if (page.compare(myFrom, index) >= myFrom.myInclusion) {
                                left = index + 1;
                            } else {
                                right = index;
                            }
                        }

                        Assert.that(right == left);

                        myPositionStack[myStackPosition] = right;
                        page = (BtreePage) page.myItems.get(right);
                        myStackPosition += 1;
                    }

                    myPageStack[myStackPosition] = page;
                    left = 0;
                    right = myEnd = page.myItemCount;

                    while (left < right) {
                        index = left + right >> 1;

                        if (page.compare(myFrom, index) >= myFrom.myInclusion) {
                            left = index + 1;
                        } else {
                            right = index;
                        }
                    }

                    Assert.that(right == left);

                    if (right == myEnd) {
                        myStackPosition += 1;
                        gotoNextItem(page, right - 1);
                    } else {
                        myPositionStack[myStackPosition++] = right;
                    }
                }

                if (myStackPosition != 0 && myTo != null) {
                    page = myPageStack[myStackPosition - 1];

                    if (-page.compare(myTo, myPositionStack[myStackPosition - 1]) >= myTo.myInclusion) {
                        myStackPosition = 0;
                    }
                }
            } else { // descent order
                if (myTo == null) {
                    while (--height > 0) {
                        myPageStack[myStackPosition] = page;
                        myPositionStack[myStackPosition] = page.myItemCount;
                        page = (BtreePage) page.myItems.get(page.myItemCount);
                        myStackPosition += 1;
                    }

                    myPageStack[myStackPosition] = page;
                    myPositionStack[myStackPosition++] = page.myItemCount - 1;
                } else {
                    while (--height > 0) {
                        myPageStack[myStackPosition] = page;
                        left = 0;
                        right = page.myItemCount;

                        while (left < right) {
                            index = left + right >> 1;

                            if (page.compare(myTo, index) >= 1 - myTo.myInclusion) {
                                left = index + 1;
                            } else {
                                right = index;
                            }
                        }

                        Assert.that(right == left);
                        myPositionStack[myStackPosition] = right;
                        page = (BtreePage) page.myItems.get(right);
                        myStackPosition += 1;
                    }

                    myPageStack[myStackPosition] = page;
                    left = 0;
                    right = page.myItemCount;

                    while (left < right) {
                        index = left + right >> 1;

                        if (page.compare(myTo, index) >= 1 - myTo.myInclusion) {
                            left = index + 1;
                        } else {
                            right = index;
                        }
                    }

                    Assert.that(right == left);

                    if (right == 0) {
                        myStackPosition += 1;
                        gotoNextItem(page, right);
                    } else {
                        myPositionStack[myStackPosition++] = right - 1;
                    }
                }

                if (myStackPosition != 0 && myFrom != null) {
                    page = myPageStack[myStackPosition - 1];

                    if (page.compare(myFrom, myPositionStack[myStackPosition - 1]) >= myFrom.myInclusion) {
                        myStackPosition = 0;
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

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final int position = myPositionStack[myStackPosition - 1];
            final BtreePage page = myPageStack[myStackPosition - 1];

            myCurrentPosition = position;
            myCurrentPage = page;

            final E current = (E) getCurrent(page, position);

            if (((StorageImpl) getStorage()).myConcurrentIterator) {
                myCurrentKey = new BtreeKey(page.getKey(position), page.myItems.getRaw(position));
            }

            gotoNextItem(page, position);

            return current;
        }

        @Override
        public int nextOID() {
            if (!hasNext()) {
                return 0;
            }

            final int position = myPositionStack[myStackPosition - 1];
            final BtreePage page = myPageStack[myStackPosition - 1];

            myCurrentPosition = position;
            myCurrentPage = page;

            final Object obj = page.myItems.getRaw(position);
            final int oid = getStorage().getOid(obj);

            if (((StorageImpl) getStorage()).myConcurrentIterator) {
                myCurrentKey = new BtreeKey(page.getKey(position), page.myItems.getRaw(position));
            }

            gotoNextItem(page, position);

            return oid;
        }

        protected Object getCurrent(final BtreePage aPage, final int aPosition) {
            return aPage.myItems.get(aPosition);
        }

        protected final void gotoNextItem(final BtreePage aPage, final int aPosition) {
            BtreePage page = aPage;
            int position = aPosition;

            if (myOrder == ASCENT_ORDER) {
                if (++position == myEnd) {
                    while (--myStackPosition != 0) {
                        position = myPositionStack[myStackPosition - 1];
                        page = myPageStack[myStackPosition - 1];

                        if (++position <= page.myItemCount) {
                            myPositionStack[myStackPosition - 1] = position;

                            do {
                                page = (BtreePage) page.myItems.get(position);
                                myEnd = page.myItemCount;
                                myPageStack[myStackPosition] = page;
                                myPositionStack[myStackPosition] = position = 0;
                            } while (++myStackPosition < myPageStack.length);

                            break;
                        }
                    }
                } else {
                    myPositionStack[myStackPosition - 1] = position;
                }
                if (myStackPosition != 0 && myTo != null && -page.compare(myTo, position) >= myTo.myInclusion) {
                    myStackPosition = 0;
                }
            } else { // descent order
                if (--position < 0) {
                    while (--myStackPosition != 0) {
                        position = myPositionStack[myStackPosition - 1];
                        page = myPageStack[myStackPosition - 1];

                        if (--position >= 0) {
                            myPositionStack[myStackPosition - 1] = position;

                            do {
                                page = (BtreePage) page.myItems.get(position);
                                myPageStack[myStackPosition] = page;
                                myPositionStack[myStackPosition] = position = page.myItemCount;
                            } while (++myStackPosition < myPageStack.length);

                            myPositionStack[myStackPosition - 1] = --position;
                            break;
                        }
                    }
                } else {
                    myPositionStack[myStackPosition - 1] = position;
                }

                if (myStackPosition != 0 && myFrom != null && page.compare(myFrom, position) >= myFrom.myInclusion) {
                    myStackPosition = 0;
                }
            }

            if (((StorageImpl) getStorage()).myConcurrentIterator && myStackPosition != 0) {
                myNextKey = page.getKey(position);
                myNextObj = page.myItems.getRaw(position);
            }
        }

        private void refresh() {
            if (myStackPosition != 0) {
                if (myNextKey == null) {
                    reset();
                } else {
                    if (myOrder == ASCENT_ORDER) {
                        myFrom = myNextKey;
                    } else {
                        myTo = myNextKey;
                    }

                    final Object next = myNextObj;

                    reset();

                    while (true) {
                        final int position = myPositionStack[myStackPosition - 1];
                        final BtreePage page = myPageStack[myStackPosition - 1];

                        if (!page.myItems.getRaw(position).equals(next)) {
                            gotoNextItem(page, position);
                        } else {
                            break;
                        }
                    }
                }
            }

            myCounter = myUpdateCounter;
        }

        @Override
        public void remove() {
            if (myCurrentPage == null) {
                throw new NoSuchElementException();
            }

            final StorageImpl db = (StorageImpl) getStorage();

            if (!db.myConcurrentIterator) {
                if (myCounter != myUpdateCounter) {
                    throw new ConcurrentModificationException();
                }

                myCurrentKey = new BtreeKey(myCurrentPage.getKey(myCurrentPosition), myCurrentPage.myItems.getRaw(
                        myCurrentPosition));

                if (myStackPosition != 0) {
                    final int position = myPositionStack[myStackPosition - 1];
                    final BtreePage page = myPageStack[myStackPosition - 1];

                    myNextKey = page.getKey(position);
                    myNextObj = page.myItems.getRaw(position);
                }
            }

            RndBtree.this.removeIfExists(myCurrentKey);
            refresh();
            myCurrentPage = null;
        }
    }

    class BtreeSelectionEntryIterator extends BtreeSelectionIterator<Map.Entry<Object, T>> {

        BtreeSelectionEntryIterator(final Key aFrom, final Key aTo, final int aOrder) {
            super(aFrom, aTo, aOrder);
        }

        BtreeSelectionEntryIterator(final int aOrder) {
            super(aOrder);
        }

        @Override
        protected Object getCurrent(final BtreePage aPage, final int aPosition) {
            return new BtreeEntry(aPage, aPosition);
        }
    }

    class BtreeEntryStartFromIterator extends BtreeSelectionEntryIterator {

        int myStart;

        BtreeEntryStartFromIterator(final int aStart, final int aOrder) {
            super(aOrder);

            myStart = aStart;
            reset();
        }

        @Override
        void reset() {
            myStackPosition = 0;
            myCounter = myUpdateCounter;

            if (myHeight == 0 || myStart >= myElementCount) {
                return;
            }

            BtreePage page = myRoot;

            int height = myHeight;
            int index = myStart;

            myPageStack = new BtreePage[height];
            myPositionStack = new int[height];

            while (--height > 0) {
                myPageStack[myStackPosition] = page;

                int jndex;

                for (jndex = 0; index >= page.myChildCount[jndex]; jndex++) {
                    index -= page.myChildCount[jndex];
                }

                myPositionStack[myStackPosition] = jndex;
                page = (BtreePage) page.myItems.get(jndex);
                myStackPosition += 1;
            }

            myPageStack[myStackPosition] = page;
            myPositionStack[myStackPosition++] = index;
            myEnd = page.myItemCount;
        }
    }
}
