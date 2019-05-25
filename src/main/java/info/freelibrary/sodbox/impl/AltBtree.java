
package info.freelibrary.sodbox.impl;

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
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

class AltBtree<T> extends PersistentCollection<T> implements Index<T> {

    static final int OP_DONE = 0;

    static final int OP_OVERFLOW = 1;

    static final int OP_UNDERFLOW = 2;

    static final int OP_NOT_FOUND = 3;

    static final int OP_DUPLICATE = 4;

    static final int OP_OVERWRITE = 5;

    transient int myUpdateCounter;

    int myHeight;

    int myType;

    int myNumOfElems;

    boolean myUnique;

    BtreePage myRoot;

    AltBtree() {
    }

    AltBtree(final Class aClass, final boolean aUnique) {
        myUnique = aUnique;
        myType = checkType(aClass);
    }

    AltBtree(final int aType, final boolean aUnique) {
        myType = aType;
        myUnique = aUnique;
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

            if (key.myObjectValue instanceof char[]) {
                key = new Key(new String((char[]) key.myObjectValue), key.myInclusion != 0);
            }
        }

        return key;
    }

    @Override
    public T get(final Key aKey) {
        Key key = aKey;

        key = checkKey(key);

        if (myRoot != null) {
            final ArrayList<T> list = new ArrayList<>();

            myRoot.find(key, key, myHeight, list);

            if (list.size() > 1) {
                throw new StorageError(StorageError.KEY_NOT_UNIQUE);
            } else if (list.size() == 0) {
                return null;
            } else {
                return list.get(0);
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
        return prefixSearchList(aKey).toArray();
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
        return getList(aFrom, aTo).toArray();
    }

    @Override
    public Object[] get(final Object aFrom, final Object aTo) {
        return get(Btree.getKeyFromObject(myType, aFrom), Btree.getKeyFromObject(myType, aTo));
    }

    @Override
    public boolean put(final Key aKey, final T aObject) {
        return insert(aKey, aObject, false) == null;
    }

    @Override
    public T set(final Key aKey, final T aObject) {
        return insert(aKey, aObject, true);
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
        return myUnique;
    }

    @SuppressWarnings("unchecked")
    final void allocateRootPage(final BtreeKey aInsert) {
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
            case ClassDescriptor.TP_DATE:
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
                Assert.failed("Invalid type");
        }

        newRoot.insert(aInsert, 0);
        newRoot.myItems.setObject(1, myRoot);
        newRoot.myNumOfItems = 1;
        myRoot = newRoot;
    }

    @SuppressWarnings("unchecked")
    final T insert(final Key aKey, final T aObject, final boolean aOverwrite) {
        final BtreeKey ins = new BtreeKey(checkKey(aKey), aObject);

        if (myRoot == null) {
            allocateRootPage(ins);
            myHeight = 1;
        } else {
            final int result = myRoot.insert(ins, myHeight, myUnique, aOverwrite);

            if (result == OP_OVERFLOW) {
                allocateRootPage(ins);
                myHeight += 1;
            } else if (result == OP_DUPLICATE || result == OP_OVERWRITE) {
                return (T) ins.myOldNode;
            }
        }

        myUpdateCounter += 1;
        myNumOfElems += 1;
        modify();

        return null;
    }

    @Override
    public void remove(final Key aKey, final T aObject) {
        remove(new BtreeKey(checkKey(aKey), aObject));
    }

    @Override
    public boolean unlink(final Key aKey, final T aObject) {
        return removeIfExists(aKey, aObject);
    }

    boolean removeIfExists(final Key aKey, final Object aObject) {
        return removeIfExists(new BtreeKey(checkKey(aKey), aObject));
    }

    void remove(final BtreeKey aRemove) {
        if (!removeIfExists(aRemove)) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
    }

    boolean removeIfExists(final BtreeKey aRemove) {
        if (myRoot == null) {
            return false;
        }

        final int result = myRoot.remove(aRemove, myHeight);

        if (result == OP_NOT_FOUND) {
            return false;
        }

        myNumOfElems -= 1;

        if (result == OP_UNDERFLOW) {
            if (myRoot.myNumOfItems == 0) {
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

    @SuppressWarnings("unchecked")
    @Override
    public T remove(final Key aKey) {
        if (!myUnique) {
            throw new StorageError(StorageError.KEY_NOT_UNIQUE);
        }

        final BtreeKey rk = new BtreeKey(checkKey(aKey), null);
        remove(rk);

        return (T) rk.myOldNode;
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
    public boolean put(final Object aKey, final T aObject) {
        return put(Btree.getKeyFromObject(myType, aKey), aObject);
    }

    @Override
    public T set(final Object aKey, final T aObject) {
        return set(Btree.getKeyFromObject(myType, aKey), aObject);
    }

    @Override
    public void remove(final Object aKey, final T aObject) {
        remove(Btree.getKeyFromObject(myType, aKey), aObject);
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
    public Iterator<T> iterator() {
        return iterator(null, null, ASCENT_ORDER);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator() {
        return entryIterator(null, null, ASCENT_ORDER);
    }

    @Override
    public int size() {
        return myNumOfElems;
    }

    @Override
    public void clear() {
        if (myRoot != null) {
            myRoot.purge(myHeight);
            myRoot = null;
            myNumOfElems = 0;
            myHeight = 0;
            myUpdateCounter += 1;
            modify();
        }
    }

    @Override
    public Object[] toArray() {
        final Object[] array = new Object[myNumOfElems];

        if (myRoot != null) {
            myRoot.traverseForward(myHeight, array, 0);
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

        if (myRoot != null) {
            myRoot.traverseForward(myHeight, array, 0);
        }

        if (array.length > myNumOfElems) {
            array[myNumOfElems] = null;
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

    @Override
    public Class[] getKeyTypes() {
        return new Class[] { getKeyType() };
    }

    @Override
    public Class getKeyType() {
        return mapKeyType(myType);
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

        static final int BTREE_PAGE_SIZE = Page.PAGE_SIZE - ObjectHeader.SIZE_OF - 4 * 3;

        int myNumOfItems;

        Link myItems;

        BtreePage() {
        }

        BtreePage(final Storage aStorage, final int aSize) {
            super(aStorage);

            myItems = aStorage.createLink(aSize);
            myItems.setSize(aSize);
        }

        abstract Object getData();

        abstract Object getKeyValue(int aIndex);

        abstract Key getKey(int aIndex);

        abstract int compare(Key aKey, int aIndex);

        abstract void insert(BtreeKey aKey, int aIndex);

        abstract BtreePage clonePage();

        void clearKeyValue(final int aIndex) {
        }

        @SuppressWarnings("unchecked")
        boolean find(final Key aFirstKey, final Key aLastKey, final int aHeight, final ArrayList aResult) {
            final int n = myNumOfItems;

            int height = aHeight;
            int l = 0;
            int r = n;

            height -= 1;

            if (aFirstKey != null) {
                while (l < r) {
                    final int i = l + r >> 1;

                    if (compare(aFirstKey, i) >= aFirstKey.myInclusion) {
                        l = i + 1;
                    } else {
                        r = i;
                    }
                }

                Assert.that(r == l);
            }

            if (aLastKey != null) {
                if (height == 0) {
                    while (l < n) {
                        if (-compare(aLastKey, l) >= aLastKey.myInclusion) {
                            return false;
                        }

                        aResult.add(myItems.get(l));
                        l += 1;
                    }

                    return true;
                } else {
                    do {
                        if (!((BtreePage) myItems.get(l)).find(aFirstKey, aLastKey, height, aResult)) {
                            return false;
                        }

                        if (l == n) {
                            return true;
                        }
                    } while (compare(aLastKey, l++) >= 0);

                    return false;
                }
            }

            if (height == 0) {
                while (l < n) {
                    aResult.add(myItems.get(l));
                    l += 1;
                }
            } else {
                do {
                    if (!((BtreePage) myItems.get(l)).find(aFirstKey, aLastKey, height, aResult)) {
                        return false;
                    }
                } while (++l <= n);
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
        }

        static void memcpy(final BtreePage aDestPage, final int aDestIndex, final BtreePage aSrcPage,
                final int aSrcIndex, final int aLength) {
            memcpyData(aDestPage, aDestIndex, aSrcPage, aSrcIndex, aLength);
            memcpyItems(aDestPage, aDestIndex, aSrcPage, aSrcIndex, aLength);
        }

        @SuppressWarnings("unchecked")
        void memset(final int aIndex, final int aLength) {
            int index = aIndex;
            int length = aLength;

            while (--length >= 0) {
                myItems.setObject(index++, null);
            }
        }

        @SuppressWarnings("unchecked")
        int insert(final BtreeKey aInsert, final int aHeight, final boolean aUnique, final boolean aOverwrite) {
            final int ahead = aUnique ? 1 : 0;
            final int result;

            int height = aHeight;
            int n = myNumOfItems;
            int r = n;
            int l = 0;

            while (l < r) {
                final int i = l + r >> 1;

                if (compare(aInsert.myKey, i) >= ahead) {
                    l = i + 1;
                } else {
                    r = i;
                }
            }

            Assert.that(l == r);

            /* insert before e[r] */
            if (--height != 0) {
                result = ((BtreePage) myItems.get(r)).insert(aInsert, height, aUnique, aOverwrite);

                Assert.that(result != OP_NOT_FOUND);

                if (result != OP_OVERFLOW) {
                    return result;
                }

                n += 1;
            } else if (r < n && compare(aInsert.myKey, r) == 0) {
                if (aOverwrite) {
                    aInsert.myOldNode = myItems.get(r);
                    modify();
                    myItems.setObject(r, aInsert.myNode);
                    return OP_OVERWRITE;
                } else if (aUnique) {
                    aInsert.myOldNode = myItems.get(r);
                    return OP_DUPLICATE;
                }
            }

            final int max = myItems.size();
            modify();

            if (n < max) {
                memcpy(this, r + 1, this, r, n - r);
                insert(aInsert, r);
                myNumOfItems += 1;
                return OP_DONE;
            } else { /* page is full then divide page */
                final BtreePage b = clonePage();
                Assert.that(n == max);
                final int m = (max + 1) / 2;

                if (r < m) {
                    memcpy(b, 0, this, 0, r);
                    memcpy(b, r + 1, this, r, m - r - 1);
                    memcpy(this, 0, this, m - 1, max - m + 1);
                    b.insert(aInsert, r);
                } else {
                    memcpy(b, 0, this, 0, m);
                    memcpy(this, 0, this, m, r - m);
                    memcpy(this, r - m + 1, this, r, max - r);
                    insert(aInsert, r - m);
                }

                memset(max - m + 1, m - 1);
                aInsert.myNode = b;
                aInsert.myKey = b.getKey(m - 1);

                if (height == 0) {
                    myNumOfItems = max - m + 1;
                    b.myNumOfItems = m;
                } else {
                    b.clearKeyValue(m - 1);
                    myNumOfItems = max - m;
                    b.myNumOfItems = m - 1;
                }

                return OP_OVERFLOW;
            }
        }

        @SuppressWarnings("unchecked")
        int handlePageUnderflow(final int aIndex, final BtreeKey aKey, final int aHeight) {
            final BtreePage a = (BtreePage) myItems.get(aIndex);

            a.modify();
            modify();

            int an = a.myNumOfItems;

            if (aIndex < myNumOfItems) { // exists greater page
                final BtreePage b = (BtreePage) myItems.get(aIndex + 1);
                int bn = b.myNumOfItems;

                Assert.that(bn >= an);

                if (aHeight != 1) {
                    memcpyData(a, an, this, aIndex, 1);
                    an += 1;
                    bn += 1;
                }
                if (an + bn > myItems.size()) {
                    // reallocation of nodes between pages a and b
                    final int i = bn - (an + bn >> 1);

                    b.modify();

                    memcpy(a, an, b, 0, i);
                    memcpy(b, 0, b, i, bn - i);
                    memcpyData(this, aIndex, a, an + i - 1, 1);

                    if (aHeight != 1) {
                        a.clearKeyValue(an + i - 1);
                    }

                    b.memset(bn - i, i);
                    b.myNumOfItems -= i;
                    a.myNumOfItems += i;

                    return OP_DONE;
                } else { // merge page b to a
                    memcpy(a, an, b, 0, bn);
                    b.deallocate();
                    memcpyData(this, aIndex, this, aIndex + 1, myNumOfItems - aIndex - 1);
                    memcpyItems(this, aIndex + 1, this, aIndex + 2, myNumOfItems - aIndex - 1);
                    myItems.setObject(myNumOfItems, null);
                    a.myNumOfItems += bn;
                    myNumOfItems -= 1;

                    return myNumOfItems < myItems.size() / 3 ? OP_UNDERFLOW : OP_DONE;
                }
            } else { // page b is before a
                final BtreePage b = (BtreePage) myItems.get(aIndex - 1);
                int bn = b.myNumOfItems;

                Assert.that(bn >= an);

                if (aHeight != 1) {
                    an += 1;
                    bn += 1;
                }

                if (an + bn > myItems.size()) {
                    // reallocation of nodes between pages a and b
                    final int i = bn - (an + bn >> 1);

                    b.modify();
                    memcpy(a, i, a, 0, an);
                    memcpy(a, 0, b, bn - i, i);

                    if (aHeight != 1) {
                        memcpyData(a, i - 1, this, aIndex - 1, 1);
                    }

                    memcpyData(this, aIndex - 1, b, bn - i - 1, 1);

                    if (aHeight != 1) {
                        b.clearKeyValue(bn - i - 1);
                    }

                    b.memset(bn - i, i);
                    b.myNumOfItems -= i;
                    a.myNumOfItems += i;

                    return OP_DONE;
                } else { // merge page b to a
                    memcpy(a, bn, a, 0, an);
                    memcpy(a, 0, b, 0, bn);

                    if (aHeight != 1) {
                        memcpyData(a, bn - 1, this, aIndex - 1, 1);
                    }

                    b.deallocate();
                    myItems.setObject(aIndex - 1, a);
                    myItems.setObject(myNumOfItems, null);
                    a.myNumOfItems += bn;
                    myNumOfItems -= 1;

                    return myNumOfItems < myItems.size() / 3 ? OP_UNDERFLOW : OP_DONE;
                }
            }
        }

        @SuppressWarnings("unchecked")
        int remove(final BtreeKey aRemove, final int aHeight) {
            int n = myNumOfItems;
            int height = aHeight;
            int l = 0;
            int r = n;
            int i;

            while (l < r) {
                i = l + r >> 1;

                if (compare(aRemove.myKey, i) > 0) {
                    l = i + 1;
                } else {
                    r = i;
                }
            }

            if (--height == 0) {
                final Object node = aRemove.myNode;

                while (r < n) {
                    if (compare(aRemove.myKey, r) == 0) {
                        if (node == null || myItems.containsElement(r, node)) {
                            aRemove.myOldNode = myItems.get(r);
                            modify();
                            memcpy(this, r, this, r + 1, n - r - 1);
                            myNumOfItems = --n;
                            memset(n, 1);
                            return n < myItems.size() / 3 ? OP_UNDERFLOW : OP_DONE;
                        }
                    } else {
                        break;
                    }

                    r += 1;
                }
                return OP_NOT_FOUND;
            }

            do {
                switch (((BtreePage) myItems.get(r)).remove(aRemove, height)) {
                    case OP_UNDERFLOW:
                        return handlePageUnderflow(r, aRemove, height);
                    case OP_DONE:
                        return OP_DONE;
                    default:
                }
            } while (++r <= n);

            return OP_NOT_FOUND;
        }

        void purge(final int aHeight) {
            int height = aHeight;

            if (--height != 0) {
                int n = myNumOfItems;

                do {
                    ((BtreePage) myItems.get(n)).purge(height);
                } while (--n >= 0);
            }

            super.deallocate();
        }

        int traverseForward(final int aHeight, final Object[] aResult, final int aPosition) {
            final int n = myNumOfItems;

            int position = aPosition;
            int height = aHeight;
            int i;

            if (--height != 0) {
                for (i = 0; i <= n; i++) {
                    position = ((BtreePage) myItems.get(i)).traverseForward(height, aResult, position);
                }
            } else {
                for (i = 0; i < n; i++) {
                    aResult[position++] = myItems.get(i);
                }
            }

            return position;
        }

    }

    static class BtreePageOfByte extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 1);

        byte[] myData;

        BtreePageOfByte() {
        }

        BtreePageOfByte(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new byte[MAX_ITEMS];
        }

        @Override
        Object getData() {
            return myData;
        }

        @Override
        Object getKeyValue(final int aIndex) {
            return new Byte(myData[aIndex]);
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

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 2);

        short[] myData;

        BtreePageOfShort() {
        }

        BtreePageOfShort(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new short[MAX_ITEMS];
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
            return new Short(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfShort(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return (short) aKey.myIntValue - myData[aIndex];
        }

        @SuppressWarnings("unchecked")
        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = (short) aKey.myKey.myIntValue;
        }

    }

    static class BtreePageOfChar extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 2);

        char[] myData;

        BtreePageOfChar() {
        }

        BtreePageOfChar(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new char[MAX_ITEMS];
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
            return new Character(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfChar(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return (char) aKey.myIntValue - myData[aIndex];
        }

        @SuppressWarnings("unchecked")
        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = (char) aKey.myKey.myIntValue;
        }

    }

    static class BtreePageOfInt extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4);

        int[] myData;

        BtreePageOfInt() {
        }

        BtreePageOfInt(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new int[MAX_ITEMS];
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
            return new Integer(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfInt(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return aKey.myIntValue < myData[aIndex] ? -1 : aKey.myIntValue == myData[aIndex] ? 0 : 1;
        }

        @SuppressWarnings("unchecked")
        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = aKey.myKey.myIntValue;
        }

    }

    static class BtreePageOfLong extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 8);

        long[] myData;

        BtreePageOfLong() {
        }

        BtreePageOfLong(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new long[MAX_ITEMS];
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
            return new Long(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfLong(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return aKey.myLongValue < myData[aIndex] ? -1 : aKey.myLongValue == myData[aIndex] ? 0 : 1;
        }

        @SuppressWarnings("unchecked")
        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = aKey.myKey.myLongValue;
        }

    }

    static class BtreePageOfFloat extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4);

        float[] myData;

        BtreePageOfFloat() {
        }

        BtreePageOfFloat(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new float[MAX_ITEMS];
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
            return new Float(myData[aIndex]);
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

        @SuppressWarnings("unchecked")
        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = (float) aKey.myKey.myDoubleValue;
        }

    }

    static class BtreePageOfDouble extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 8);

        double[] myData;

        BtreePageOfDouble() {
        }

        BtreePageOfDouble(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new double[MAX_ITEMS];
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
            return new Double(myData[aIndex]);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfDouble(getStorage());
        }

        @Override
        int compare(final Key aKey, final int aIndex) {
            return aKey.myDoubleValue < myData[aIndex] ? -1 : aKey.myDoubleValue == myData[aIndex] ? 0 : 1;
        }

        @SuppressWarnings("unchecked")
        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = aKey.myKey.myDoubleValue;
        }

    }

    static class BtreePageOfObject extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4);

        Link myData;

        BtreePageOfObject() {
        }

        BtreePageOfObject(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = aStorage.createLink(MAX_ITEMS);
            myData.setSize(MAX_ITEMS);
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
            final int oid = obj == null ? 0 : getStorage().getOid(obj);
            return aKey.myIntValue < oid ? -1 : aKey.myIntValue == oid ? 0 : 1;
        }

        @SuppressWarnings("unchecked")
        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData.setObject(aIndex, aKey.myKey.myObjectValue);
        }

    }

    static class BtreePageOfString extends BtreePage {

        static final int MAX_ITEMS = 100;

        String[] myData;

        BtreePageOfString() {
        }

        BtreePageOfString(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new String[MAX_ITEMS];
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

        @SuppressWarnings("unchecked")
        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = (String) aKey.myKey.myObjectValue;
        }

        @SuppressWarnings("unchecked")
        @Override
        void memset(final int aIndex, final int aLength) {
            int length = aLength;
            int index = aIndex;

            while (--length >= 0) {
                myItems.setObject(index, null);
                myData[index] = null;
                index += 1;
            }
        }

        @SuppressWarnings("unchecked")
        boolean prefixSearch(final String aKey, final int aHeight, final ArrayList aResult) {
            final int n = myNumOfItems;

            int height = aHeight;
            int l = 0;
            int r = n;

            height -= 1;

            while (l < r) {
                final int i = l + r >> 1;

                if (!aKey.startsWith(myData[i]) && aKey.compareTo(myData[i]) > 0) {
                    l = i + 1;
                } else {
                    r = i;
                }
            }

            Assert.that(r == l);

            if (height == 0) {
                while (l < n) {
                    if (aKey.compareTo(myData[l]) < 0) {
                        return false;
                    }

                    aResult.add(myItems.get(l));
                    l += 1;
                }
            } else {
                do {
                    if (!((BtreePageOfString) myItems.get(l)).prefixSearch(aKey, height, aResult)) {
                        return false;
                    }
                    if (l == n) {
                        return true;
                    }
                } while (aKey.compareTo(myData[l++]) >= 0);

                return false;
            }

            return true;
        }

    }

    static class BtreePageOfValue extends BtreePage {

        static final int MAX_ITEMS = 100;

        Object[] myData;

        BtreePageOfValue() {
        }

        BtreePageOfValue(final Storage aStorage) {
            super(aStorage, MAX_ITEMS);
            myData = new Object[MAX_ITEMS];
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

        @SuppressWarnings("unchecked")
        @Override
        int compare(final Key aKey, final int aIndex) {
            return ((Comparable) aKey.myObjectValue).compareTo(myData[aIndex]);
        }

        @SuppressWarnings("unchecked")
        @Override
        void insert(final BtreeKey aKey, final int aIndex) {
            myItems.setObject(aIndex, aKey.myNode);
            myData[aIndex] = aKey.myKey.myObjectValue;
        }

    }

    static class BtreeEntry<T> implements Map.Entry<Object, T> {

        private final int myPosition;

        private final BtreePage myPage;

        BtreeEntry(final BtreePage aPage, final int aPosition) {
            myPage = aPage;
            myPosition = aPosition;
        }

        @Override
        public Object getKey() {
            return myPage.getKeyValue(myPosition);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T getValue() {
            return (T) myPage.myItems.get(myPosition);
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

            final Map.Entry e = (Map.Entry) aObject;

            return (getKey() == null ? e.getKey() == null : getKey().equals(e.getKey())) && (getValue() == null ? e
                    .getValue() == null : getValue().equals(e.getValue()));
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

        void reset() {
            int i;
            int l;
            int r;

            myStackPosition = 0;
            myCounter = myUpdateCounter;

            if (myHeight == 0) {
                return;
            }

            BtreePage page = myRoot;
            int h = myHeight;

            myPageStack = new BtreePage[h];
            myPositionStack = new int[h];

            if (myOrder == ASCENT_ORDER) {
                if (myFrom == null) {
                    while (--h > 0) {
                        myPositionStack[myStackPosition] = 0;
                        myPageStack[myStackPosition] = page;
                        page = (BtreePage) page.myItems.get(0);
                        myStackPosition += 1;
                    }

                    myPositionStack[myStackPosition] = 0;
                    myPageStack[myStackPosition] = page;
                    myEnd = page.myNumOfItems;
                    myStackPosition += 1;
                } else {
                    while (--h > 0) {
                        myPageStack[myStackPosition] = page;
                        l = 0;
                        r = page.myNumOfItems;

                        while (l < r) {
                            i = l + r >> 1;

                            if (page.compare(myFrom, i) >= myFrom.myInclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        myPositionStack[myStackPosition] = r;
                        page = (BtreePage) page.myItems.get(r);
                        myStackPosition += 1;
                    }

                    myPageStack[myStackPosition] = page;
                    l = 0;
                    r = myEnd = page.myNumOfItems;

                    while (l < r) {
                        i = l + r >> 1;

                        if (page.compare(myFrom, i) >= myFrom.myInclusion) {
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
                    while (--h > 0) {
                        myPageStack[myStackPosition] = page;
                        myPositionStack[myStackPosition] = page.myNumOfItems;
                        page = (BtreePage) page.myItems.get(page.myNumOfItems);
                        myStackPosition += 1;
                    }

                    myPageStack[myStackPosition] = page;
                    myPositionStack[myStackPosition++] = page.myNumOfItems - 1;
                } else {
                    while (--h > 0) {
                        myPageStack[myStackPosition] = page;
                        l = 0;
                        r = page.myNumOfItems;

                        while (l < r) {
                            i = l + r >> 1;

                            if (page.compare(myTo, i) >= 1 - myTo.myInclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        myPositionStack[myStackPosition] = r;
                        page = (BtreePage) page.myItems.get(r);
                        myStackPosition += 1;
                    }

                    myPageStack[myStackPosition] = page;
                    l = 0;
                    r = page.myNumOfItems;

                    while (l < r) {
                        i = l + r >> 1;

                        if (page.compare(myTo, i) >= 1 - myTo.myInclusion) {
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

        @SuppressWarnings("unchecked")
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

                        if (++position <= page.myNumOfItems) {
                            myPositionStack[myStackPosition - 1] = position;

                            do {
                                page = (BtreePage) page.myItems.get(position);
                                myEnd = page.myNumOfItems;
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
                                myPositionStack[myStackPosition] = position = page.myNumOfItems;
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

            AltBtree.this.removeIfExists(myCurrentKey);

            refresh();
            myCurrentPage = null;
        }

    }

    class BtreeSelectionEntryIterator extends BtreeSelectionIterator<Map.Entry<Object, T>> {

        BtreeSelectionEntryIterator(final Key aFrom, final Key aTo, final int aOrder) {
            super(aFrom, aTo, aOrder);
        }

        @Override
        protected Object getCurrent(final BtreePage aPage, final int aPosition) {
            return new BtreeEntry(aPage, aPosition);
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

}
