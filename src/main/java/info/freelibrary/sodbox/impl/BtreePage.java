
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.util.ArrayList;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Key;

final class BtreePage {

    static final int FIRST_KEY_OFFSET = 4;

    static final int KEY_SPACE = Page.PAGE_SIZE - FIRST_KEY_OFFSET;

    static final int STRING_KEY_SIZE = 8;

    static final int MAX_ITEMS = KEY_SPACE / 4;

    private static final String ASSERTION_MSG = "String fits in the B-Tree page";

    private BtreePage() {
    }

    static int getnItems(final Page aPage) {
        return Bytes.unpack2(aPage.myData, 0);
    }

    static int getSize(final Page aPage) {
        return Bytes.unpack2(aPage.myData, 2);
    }

    static int getKeyStrOid(final Page aPage, final int aIndex) {
        return Bytes.unpack4(aPage.myData, FIRST_KEY_OFFSET + aIndex * 8);
    }

    static int getKeyStrSize(final Page aPage, final int aIndex) {
        return Bytes.unpack2(aPage.myData, FIRST_KEY_OFFSET + aIndex * 8 + 4);
    }

    static int getKeyStrOffs(final Page aPage, final int aIndex) {
        return Bytes.unpack2(aPage.myData, FIRST_KEY_OFFSET + aIndex * 8 + 6);
    }

    static int getReference(final Page aPage, final int aIndex) {
        return Bytes.unpack4(aPage.myData, FIRST_KEY_OFFSET + aIndex * 4);
    }

    static void setnItems(final Page aPage, final int aNumOfItems) {
        Bytes.pack2(aPage.myData, 0, (short) aNumOfItems);
    }

    static void setSize(final Page aPage, final int aSize) {
        Bytes.pack2(aPage.myData, 2, (short) aSize);
    }

    static void setKeyStrOid(final Page aPage, final int aIndex, final int aOID) {
        Bytes.pack4(aPage.myData, FIRST_KEY_OFFSET + aIndex * 8, aOID);
    }

    static void setKeyStrSize(final Page aPage, final int aIndex, final int aSize) {
        Bytes.pack2(aPage.myData, FIRST_KEY_OFFSET + aIndex * 8 + 4, (short) aSize);
    }

    static void setKeyStrOffs(final Page aPage, final int aIndex, final int aOffset) {
        Bytes.pack2(aPage.myData, FIRST_KEY_OFFSET + aIndex * 8 + 6, (short) aOffset);
    }

    static void setKeyStrChars(final Page aPage, final int aOffset, final char[] aChars) {
        final int len = aChars.length;

        int offset = aOffset;

        for (int index = 0; index < len; index++) {
            Bytes.pack2(aPage.myData, FIRST_KEY_OFFSET + offset, (short) aChars[index]);
            offset += 2;
        }
    }

    static void setKeyBytes(final Page aPage, final int aOffset, final byte[] aBytes) {
        System.arraycopy(aBytes, 0, aPage.myData, FIRST_KEY_OFFSET + aOffset, aBytes.length);
    }

    static void setReference(final Page aPage, final int aIndex, final int aOID) {
        Bytes.pack4(aPage.myData, FIRST_KEY_OFFSET + aIndex * 4, aOID);
    }

    static int compare(final Key aKey, final Page aPage, final int aIndex) {
        final long i8;
        final int i4;
        final float r4;
        final double r8;

        switch (aKey.myType) {
            case ClassDescriptor.TP_BOOLEAN:
            case ClassDescriptor.TP_BYTE:
                return (byte) aKey.myIntValue - aPage.myData[BtreePage.FIRST_KEY_OFFSET + aIndex];
            case ClassDescriptor.TP_SHORT:
                return (short) aKey.myIntValue - Bytes.unpack2(aPage.myData, BtreePage.FIRST_KEY_OFFSET + aIndex * 2);
            case ClassDescriptor.TP_CHAR:
                return (char) aKey.myIntValue - (char) Bytes.unpack2(aPage.myData, BtreePage.FIRST_KEY_OFFSET +
                        aIndex * 2);
            case ClassDescriptor.TP_OBJECT:
            case ClassDescriptor.TP_INT:
            case ClassDescriptor.TP_ENUM:
                i4 = Bytes.unpack4(aPage.myData, BtreePage.FIRST_KEY_OFFSET + aIndex * 4);
                return aKey.myIntValue < i4 ? -1 : aKey.myIntValue == i4 ? 0 : 1;
            case ClassDescriptor.TP_LONG:
            case ClassDescriptor.TP_DATE:
                i8 = Bytes.unpack8(aPage.myData, BtreePage.FIRST_KEY_OFFSET + aIndex * 8);
                return aKey.myLongValue < i8 ? -1 : aKey.myLongValue == i8 ? 0 : 1;
            case ClassDescriptor.TP_FLOAT:
                r4 = Float.intBitsToFloat(Bytes.unpack4(aPage.myData, BtreePage.FIRST_KEY_OFFSET + aIndex * 4));
                return aKey.myDoubleValue < r4 ? -1 : aKey.myDoubleValue == r4 ? 0 : 1;
            case ClassDescriptor.TP_DOUBLE:
                r8 = Double.longBitsToDouble(Bytes.unpack8(aPage.myData, BtreePage.FIRST_KEY_OFFSET + aIndex * 8));
                return aKey.myDoubleValue < r8 ? -1 : aKey.myDoubleValue == r8 ? 0 : 1;
            default:
                // FIXME
        }

        Assert.failed("Invalid type");
        return 0;
    }

    static int compareStr(final Key aKey, final Page aPage, final int aIndex) {
        final char[] chars = (char[]) aKey.myObjectValue;
        final int alen = chars.length;
        final int blen = BtreePage.getKeyStrSize(aPage, aIndex);
        final int minlen = alen < blen ? alen : blen;
        int offs = BtreePage.getKeyStrOffs(aPage, aIndex) + BtreePage.FIRST_KEY_OFFSET;
        final byte[] b = aPage.myData;

        for (int j = 0; j < minlen; j++) {
            final int diff = chars[j] - (char) Bytes.unpack2(b, offs);

            if (diff != 0) {
                return diff;
            }

            offs += 2;
        }

        return alen - blen;
    }

    static int comparePrefix(final char[] aKey, final Page aPage, final int aIndex) {
        final int alen = aKey.length;
        final int blen = BtreePage.getKeyStrSize(aPage, aIndex);
        final int minlen = alen < blen ? alen : blen;
        int offs = BtreePage.getKeyStrOffs(aPage, aIndex) + BtreePage.FIRST_KEY_OFFSET;
        final byte[] b = aPage.myData;

        for (int j = 0; j < minlen; j++) {
            final int diff = aKey[j] - (char) Bytes.unpack2(b, offs);

            if (diff != 0) {
                return diff;
            }

            offs += 2;
        }

        return minlen - blen;
    }

    @SuppressWarnings("unchecked")
    static boolean find(final StorageImpl aStorage, final int aPageID, final Key aFirstKey, final Key aLastKey,
            final Btree aBtree, final int aHeight, final ArrayList aResult) {
        final Page page = aStorage.getPage(aPageID);
        final int n = getnItems(page);
        final int height = aHeight - 1;

        int l = 0;
        int r = n;
        int oid;

        try {
            if (aBtree.myType == ClassDescriptor.TP_STRING) {
                if (aFirstKey != null) {
                    while (l < r) {
                        final int i = l + r >> 1;

                        if (compareStr(aFirstKey, page, i) >= aFirstKey.myInclusion) {
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
                            if (-compareStr(aLastKey, page, l) >= aLastKey.myInclusion) {
                                return false;
                            }

                            oid = getKeyStrOid(page, l);
                            aResult.add(aStorage.lookupObject(oid, null));
                            l += 1;
                        }
                    } else {
                        do {
                            if (!find(aStorage, getKeyStrOid(page, l), aFirstKey, aLastKey, aBtree, height,
                                    aResult)) {
                                return false;
                            }

                            if (l == n) {
                                return true;
                            }
                        } while (compareStr(aLastKey, page, l++) >= 0);
                        return false;
                    }
                } else {
                    if (height == 0) {
                        while (l < n) {
                            oid = getKeyStrOid(page, l);
                            aResult.add(aStorage.lookupObject(oid, null));
                            l += 1;
                        }
                    } else {
                        do {
                            if (!find(aStorage, getKeyStrOid(page, l), aFirstKey, aLastKey, aBtree, height,
                                    aResult)) {
                                return false;
                            }
                        } while (++l <= n);
                    }
                }
            } else if (aBtree.myType == ClassDescriptor.TP_ARRAY_OF_BYTES) {
                if (aFirstKey != null) {
                    while (l < r) {
                        final int i = l + r >> 1;

                        if (aBtree.compareByteArrays(aFirstKey, page, i) >= aFirstKey.myInclusion) {
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
                            if (-aBtree.compareByteArrays(aLastKey, page, l) >= aLastKey.myInclusion) {
                                return false;
                            }

                            oid = getKeyStrOid(page, l);
                            aResult.add(aStorage.lookupObject(oid, null));
                            l += 1;
                        }
                    } else {
                        do {
                            if (!find(aStorage, getKeyStrOid(page, l), aFirstKey, aLastKey, aBtree, height,
                                    aResult)) {
                                return false;
                            }

                            if (l == n) {
                                return true;
                            }
                        } while (aBtree.compareByteArrays(aLastKey, page, l++) >= 0);
                        return false;
                    }
                } else {
                    if (height == 0) {
                        while (l < n) {
                            oid = getKeyStrOid(page, l);
                            aResult.add(aStorage.lookupObject(oid, null));
                            l += 1;
                        }
                    } else {
                        do {
                            if (!find(aStorage, getKeyStrOid(page, l), aFirstKey, aLastKey, aBtree, height,
                                    aResult)) {
                                return false;
                            }
                        } while (++l <= n);
                    }
                }
            } else {
                if (aFirstKey != null) {
                    while (l < r) {
                        final int i = l + r >> 1;
                        if (compare(aFirstKey, page, i) >= aFirstKey.myInclusion) {
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
                            if (-compare(aLastKey, page, l) >= aLastKey.myInclusion) {
                                return false;
                            }

                            oid = getReference(page, MAX_ITEMS - 1 - l);
                            aResult.add(aStorage.lookupObject(oid, null));
                            l += 1;
                        }

                        return true;
                    } else {
                        do {
                            if (!find(aStorage, getReference(page, MAX_ITEMS - 1 - l), aFirstKey, aLastKey, aBtree,
                                    height, aResult)) {
                                return false;
                            }

                            if (l == n) {
                                return true;
                            }
                        } while (compare(aLastKey, page, l++) >= 0);
                        return false;
                    }
                }
                if (height == 0) {
                    while (l < n) {
                        oid = getReference(page, MAX_ITEMS - 1 - l);
                        aResult.add(aStorage.lookupObject(oid, null));
                        l += 1;
                    }
                } else {
                    do {
                        if (!find(aStorage, getReference(page, MAX_ITEMS - 1 - l), aFirstKey, aLastKey, aBtree,
                                height, aResult)) {
                            return false;
                        }
                    } while (++l <= n);
                }
            }
        } finally {
            aStorage.myPool.unfix(page);
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    static boolean prefixSearch(final StorageImpl aStorage, final int aPageId, final char[] aKey, final int aHeight,
            final ArrayList aResult) {
        final Page page = aStorage.getPage(aPageId);
        final int n = getnItems(page);
        final int height = aHeight - 1;

        int l = 0;
        int r = n;
        int oid;

        try {
            while (l < r) {
                final int i = l + r >> 1;

                if (comparePrefix(aKey, page, i) > 0) {
                    l = i + 1;
                } else {
                    r = i;
                }
            }

            Assert.that(r == l);

            if (height == 0) {
                while (l < n) {
                    if (comparePrefix(aKey, page, l) < 0) {
                        return false;
                    }

                    oid = getKeyStrOid(page, l);
                    aResult.add(aStorage.lookupObject(oid, null));
                    l += 1;
                }
            } else {
                do {
                    if (!prefixSearch(aStorage, getKeyStrOid(page, l), aKey, height, aResult)) {
                        return false;
                    }

                    if (l == n) {
                        return true;
                    }
                } while (comparePrefix(aKey, page, l++) >= 0);

                return false;
            }
        } finally {
            aStorage.myPool.unfix(page);
        }

        return true;
    }

    static int allocate(final StorageImpl aStorage, final int aRoot, final int aType, final BtreeKey aInsert) {
        final int pageId = aStorage.allocatePage();
        final Page page = aStorage.putPage(pageId);

        setnItems(page, 1);

        if (aType == ClassDescriptor.TP_STRING) {
            final char[] sval = (char[]) aInsert.myKey.myObjectValue;
            final int len = sval.length;

            setSize(page, len * 2);
            setKeyStrOffs(page, 0, KEY_SPACE - len * 2);
            setKeyStrSize(page, 0, len);
            setKeyStrOid(page, 0, aInsert.myOID);
            setKeyStrOid(page, 1, aRoot);
            setKeyStrChars(page, KEY_SPACE - len * 2, sval);
        } else if (aType == ClassDescriptor.TP_ARRAY_OF_BYTES) {
            final byte[] bytes = (byte[]) aInsert.myKey.myObjectValue;
            final int length = bytes.length;

            setSize(page, length);
            setKeyStrOffs(page, 0, KEY_SPACE - length);
            setKeyStrSize(page, 0, length);
            setKeyStrOid(page, 0, aInsert.myOID);
            setKeyStrOid(page, 1, aRoot);
            setKeyBytes(page, KEY_SPACE - length, bytes);
        } else {
            aInsert.pack(page, 0);
            setReference(page, MAX_ITEMS - 2, aRoot);
        }

        aStorage.myPool.unfix(page);

        return pageId;
    }

    static void memcpy(final Page aDestPage, final int aDestIndex, final Page aSrcPage, final int aSrcIndex,
            final int aLength, final int aItemSize) {
        System.arraycopy(aSrcPage.myData, FIRST_KEY_OFFSET + aSrcIndex * aItemSize, aDestPage.myData,
                FIRST_KEY_OFFSET + aDestIndex * aItemSize, aLength * aItemSize);
    }

    static int insert(final StorageImpl aStorage, final int aPageID, final Btree aBtree, final BtreeKey aInsert,
            final int aHeight, final boolean aUniqueRestriction, final boolean aOverwrite) {
        final int result;

        int pageID = aPageID;
        Page page = aStorage.getPage(pageID);
        int height = aHeight;
        int l = 0;
        int n = getnItems(page);
        int r = n;

        final int ahead = aUniqueRestriction ? 1 : 0;

        try {
            if (aBtree.myType == ClassDescriptor.TP_STRING) {
                while (l < r) {
                    final int i = l + r >> 1;

                    if (compareStr(aInsert.myKey, page, i) >= ahead) {
                        l = i + 1;
                    } else {
                        r = i;
                    }
                }

                Assert.that(l == r);

                if (--height != 0) {
                    result = insert(aStorage, getKeyStrOid(page, r), aBtree, aInsert, height, aUniqueRestriction,
                            aOverwrite);
                    Assert.that(result != Btree.OP_NOT_FOUND);

                    if (result != Btree.OP_OVERFLOW) {
                        return result;
                    }
                } else if (r < n && compareStr(aInsert.myKey, page, r) == 0) {
                    if (aOverwrite) {
                        aStorage.myPool.unfix(page);
                        page = null;
                        page = aStorage.putPage(pageID);
                        aInsert.myOldOID = getKeyStrOid(page, r);
                        setKeyStrOid(page, r, aInsert.myOID);
                        return Btree.OP_OVERWRITE;
                    } else if (aUniqueRestriction) {
                        return Btree.OP_DUPLICATE;
                    }
                }

                aStorage.myPool.unfix(page);
                page = null;
                page = aStorage.putPage(pageID);
                return insertStrKey(aStorage, page, r, aInsert, height);
            } else if (aBtree.myType == ClassDescriptor.TP_ARRAY_OF_BYTES) {
                while (l < r) {
                    final int i = l + r >> 1;

                    if (aBtree.compareByteArrays(aInsert.myKey, page, i) >= ahead) {
                        l = i + 1;
                    } else {
                        r = i;
                    }
                }

                Assert.that(l == r);

                if (--height != 0) {
                    result = insert(aStorage, getKeyStrOid(page, r), aBtree, aInsert, height, aUniqueRestriction,
                            aOverwrite);
                    Assert.that(result != Btree.OP_NOT_FOUND);

                    if (result != Btree.OP_OVERFLOW) {
                        return result;
                    }
                } else if (r < n && aBtree.compareByteArrays(aInsert.myKey, page, r) == 0) {
                    if (aOverwrite) {
                        aStorage.myPool.unfix(page);
                        page = null;
                        page = aStorage.putPage(pageID);
                        aInsert.myOldOID = getKeyStrOid(page, r);
                        setKeyStrOid(page, r, aInsert.myOID);
                        return Btree.OP_OVERWRITE;
                    } else if (aUniqueRestriction) {
                        return Btree.OP_DUPLICATE;
                    }
                }

                aStorage.myPool.unfix(page);
                page = null;
                page = aStorage.putPage(pageID);
                return insertByteArrayKey(aStorage, page, r, aInsert, height);
            } else {
                while (l < r) {
                    final int i = l + r >> 1;

                    if (compare(aInsert.myKey, page, i) >= ahead) {
                        l = i + 1;
                    } else {
                        r = i;
                    }
                }

                Assert.that(l == r);
                /* insert before e[r] */
                if (--height != 0) {
                    result = insert(aStorage, getReference(page, MAX_ITEMS - r - 1), aBtree, aInsert, height,
                            aUniqueRestriction, aOverwrite);

                    Assert.that(result != Btree.OP_NOT_FOUND);

                    if (result != Btree.OP_OVERFLOW) {
                        return result;
                    }

                    n += 1;
                } else if (r < n && compare(aInsert.myKey, page, r) == 0) {
                    if (aOverwrite) {
                        aStorage.myPool.unfix(page);
                        page = null;
                        page = aStorage.putPage(pageID);
                        aInsert.myOldOID = getReference(page, MAX_ITEMS - r - 1);
                        setReference(page, MAX_ITEMS - r - 1, aInsert.myOID);
                        return Btree.OP_OVERWRITE;
                    } else if (aUniqueRestriction) {
                        return Btree.OP_DUPLICATE;
                    }
                }

                aStorage.myPool.unfix(page);
                page = null;
                page = aStorage.putPage(pageID);

                final int itemSize = ClassDescriptor.SIZE_OF[aBtree.myType];
                final int max = KEY_SPACE / (4 + itemSize);

                if (n < max) {
                    memcpy(page, r + 1, page, r, n - r, itemSize);
                    memcpy(page, MAX_ITEMS - n - 1, page, MAX_ITEMS - n, n - r, 4);
                    aInsert.pack(page, r);
                    setnItems(page, getnItems(page) + 1);
                    return Btree.OP_DONE;
                } else { /* page is full then divide page */
                    pageID = aStorage.allocatePage();

                    final Page b = aStorage.putPage(pageID);
                    Assert.that(n == max);
                    final int m = (max + 1) / 2;

                    if (r < m) {
                        memcpy(b, 0, page, 0, r, itemSize);
                        memcpy(b, r + 1, page, r, m - r - 1, itemSize);
                        memcpy(page, 0, page, m - 1, max - m + 1, itemSize);
                        memcpy(b, MAX_ITEMS - r, page, MAX_ITEMS - r, r, 4);
                        aInsert.pack(b, r);
                        memcpy(b, MAX_ITEMS - m, page, MAX_ITEMS - m + 1, m - r - 1, 4);
                        memcpy(page, MAX_ITEMS - max + m - 1, page, MAX_ITEMS - max, max - m + 1, 4);
                    } else {
                        memcpy(b, 0, page, 0, m, itemSize);
                        memcpy(page, 0, page, m, r - m, itemSize);
                        memcpy(page, r - m + 1, page, r, max - r, itemSize);
                        memcpy(b, MAX_ITEMS - m, page, MAX_ITEMS - m, m, 4);
                        memcpy(page, MAX_ITEMS - r + m, page, MAX_ITEMS - r, r - m, 4);
                        aInsert.pack(page, r - m);
                        memcpy(page, MAX_ITEMS - max + m - 1, page, MAX_ITEMS - max, max - r, 4);
                    }

                    aInsert.myOID = pageID;
                    aInsert.extract(b, FIRST_KEY_OFFSET + (m - 1) * itemSize, aBtree.myType);

                    if (height == 0) {
                        setnItems(page, max - m + 1);
                        setnItems(b, m);
                    } else {
                        setnItems(page, max - m);
                        setnItems(b, m - 1);
                    }

                    aStorage.myPool.unfix(b);
                    return Btree.OP_OVERFLOW;
                }
            }
        } finally {
            if (page != null) {
                aStorage.myPool.unfix(page);
            }
        }
    }

    static int insertStrKey(final StorageImpl aStorage, final Page aPage, final int aStart, final BtreeKey aInsert,
            final int aHeight) {
        int numOfItems = getnItems(aPage);
        int size = getSize(aPage);

        final int n = aHeight != 0 ? numOfItems + 1 : numOfItems;
        final char[] chars = (char[]) aInsert.myKey.myObjectValue;
        final int length = chars.length;

        if (size + length * 2 + (n + 1) * STRING_KEY_SIZE <= KEY_SPACE) {
            memcpy(aPage, aStart + 1, aPage, aStart, n - aStart, STRING_KEY_SIZE);
            size += length * 2;
            setKeyStrOffs(aPage, aStart, KEY_SPACE - size);
            setKeyStrSize(aPage, aStart, length);
            setKeyStrOid(aPage, aStart, aInsert.myOID);
            setKeyStrChars(aPage, KEY_SPACE - size, chars);
            numOfItems += 1;
        } else { // page is full then divide page
            final int pageId = aStorage.allocatePage();
            final Page page = aStorage.putPage(pageId);

            int moved = 0;
            int inserted = length * 2 + STRING_KEY_SIZE;
            int prevDelta = (1 << 31) + 1;

            for (int bn = 0, i = 0;; bn += 1) {
                final int addSize;

                int j = numOfItems - i - 1;
                int keyLen = getKeyStrSize(aPage, i);
                int subSize;

                if (bn == aStart) {
                    keyLen = length;
                    inserted = 0;
                    addSize = length;

                    if (aHeight == 0) {
                        subSize = 0;
                        j += 1;
                    } else {
                        subSize = getKeyStrSize(aPage, i);
                    }
                } else {
                    addSize = subSize = keyLen;

                    if (aHeight != 0) {
                        if (i + 1 != aStart) {
                            subSize += getKeyStrSize(aPage, i + 1);
                            j -= 1;
                        } else {
                            inserted = 0;
                        }
                    }
                }

                final int delta = moved + addSize * 2 + (bn + 1) * STRING_KEY_SIZE - (j * STRING_KEY_SIZE + size -
                        subSize * 2 + inserted);

                if (delta >= -prevDelta) {
                    if (aHeight == 0) {
                        aInsert.getStr(page, bn - 1);
                    } else {
                        Assert.that(ASSERTION_MSG, moved + (bn + 1) * STRING_KEY_SIZE <= KEY_SPACE);

                        if (bn != aStart) {
                            aInsert.getStr(aPage, i);
                            setKeyStrOid(page, bn, getKeyStrOid(aPage, i));
                            size -= keyLen * 2;
                            i += 1;
                        } else {
                            setKeyStrOid(page, bn, aInsert.myOID);
                        }
                    }

                    numOfItems = compactifyStrings(aPage, i);

                    if (bn < aStart || bn == aStart && aHeight == 0) {
                        memcpy(aPage, aStart - i + 1, aPage, aStart - i, n - aStart, STRING_KEY_SIZE);

                        size += length * 2;
                        numOfItems += 1;

                        Assert.that(ASSERTION_MSG, size + (n - i + 1) * STRING_KEY_SIZE <= KEY_SPACE);

                        setKeyStrOffs(aPage, aStart - i, KEY_SPACE - size);
                        setKeyStrSize(aPage, aStart - i, length);
                        setKeyStrOid(aPage, aStart - i, aInsert.myOID);
                        setKeyStrChars(aPage, KEY_SPACE - size, chars);
                    }

                    setnItems(page, bn);
                    setSize(page, moved);
                    setSize(aPage, size);
                    setnItems(aPage, numOfItems);
                    aInsert.myOID = pageId;
                    aStorage.myPool.unfix(page);
                    return Btree.OP_OVERFLOW;
                }

                moved += keyLen * 2;
                prevDelta = delta;

                Assert.that(ASSERTION_MSG, moved + (bn + 1) * STRING_KEY_SIZE <= KEY_SPACE);

                setKeyStrSize(page, bn, keyLen);
                setKeyStrOffs(page, bn, KEY_SPACE - moved);

                if (bn == aStart) {
                    setKeyStrOid(page, bn, aInsert.myOID);
                    setKeyStrChars(page, KEY_SPACE - moved, chars);
                } else {
                    setKeyStrOid(page, bn, getKeyStrOid(aPage, i));
                    memcpy(page, KEY_SPACE - moved, aPage, getKeyStrOffs(aPage, i), keyLen * 2, 1);
                    size -= keyLen * 2;
                    i += 1;
                }
            }
        }

        setnItems(aPage, numOfItems);
        setSize(aPage, size);

        return size + STRING_KEY_SIZE * (numOfItems + 1) < KEY_SPACE / 3 ? Btree.OP_UNDERFLOW : Btree.OP_DONE;
    }

    static int insertByteArrayKey(final StorageImpl aStorage, final Page aPage, final int aStart,
            final BtreeKey aInsert, final int aHeight) {
        int nItems = getnItems(aPage);
        int size = getSize(aPage);

        final int n = aHeight != 0 ? nItems + 1 : nItems;
        final byte[] bytes = (byte[]) aInsert.myKey.myObjectValue;
        final int length = bytes.length;

        if (size + length + (n + 1) * STRING_KEY_SIZE <= KEY_SPACE) {
            memcpy(aPage, aStart + 1, aPage, aStart, n - aStart, STRING_KEY_SIZE);
            size += length;
            setKeyStrOffs(aPage, aStart, KEY_SPACE - size);
            setKeyStrSize(aPage, aStart, length);
            setKeyStrOid(aPage, aStart, aInsert.myOID);
            setKeyBytes(aPage, KEY_SPACE - size, bytes);
            nItems += 1;
        } else { // page is full then divide page
            final int pageId = aStorage.allocatePage();
            final Page b = aStorage.putPage(pageId);

            int moved = 0;
            int inserted = length + STRING_KEY_SIZE;
            int prevDelta = (1 << 31) + 1;

            for (int bn = 0, i = 0;; bn += 1) {
                final int addSize;

                int subSize;
                int j = nItems - i - 1;
                int keyLen = getKeyStrSize(aPage, i);

                if (bn == aStart) {
                    keyLen = length;
                    inserted = 0;
                    addSize = length;

                    if (aHeight == 0) {
                        subSize = 0;
                        j += 1;
                    } else {
                        subSize = getKeyStrSize(aPage, i);
                    }
                } else {
                    addSize = subSize = keyLen;

                    if (aHeight != 0) {
                        if (i + 1 != aStart) {
                            subSize += getKeyStrSize(aPage, i + 1);
                            j -= 1;
                        } else {
                            inserted = 0;
                        }
                    }
                }

                final int delta = moved + addSize + (bn + 1) * STRING_KEY_SIZE - (j * STRING_KEY_SIZE + size -
                        subSize + inserted);

                if (delta >= -prevDelta) {
                    if (aHeight == 0) {
                        aInsert.getByteArray(b, bn - 1);
                    } else {
                        Assert.that(ASSERTION_MSG, moved + (bn + 1) * STRING_KEY_SIZE <= KEY_SPACE);

                        if (bn != aStart) {
                            aInsert.getByteArray(aPage, i);
                            setKeyStrOid(b, bn, getKeyStrOid(aPage, i));
                            size -= keyLen;
                            i += 1;
                        } else {
                            setKeyStrOid(b, bn, aInsert.myOID);
                        }
                    }

                    nItems = compactifyByteArrays(aPage, i);

                    if (bn < aStart || bn == aStart && aHeight == 0) {
                        memcpy(aPage, aStart - i + 1, aPage, aStart - i, n - aStart, STRING_KEY_SIZE);
                        size += length;
                        nItems += 1;

                        Assert.that(ASSERTION_MSG, size + (n - i + 1) * STRING_KEY_SIZE <= KEY_SPACE);

                        setKeyStrOffs(aPage, aStart - i, KEY_SPACE - size);
                        setKeyStrSize(aPage, aStart - i, length);
                        setKeyStrOid(aPage, aStart - i, aInsert.myOID);
                        setKeyBytes(aPage, KEY_SPACE - size, bytes);
                    }

                    setnItems(b, bn);
                    setSize(b, moved);
                    setSize(aPage, size);
                    setnItems(aPage, nItems);
                    aInsert.myOID = pageId;
                    aStorage.myPool.unfix(b);
                    return Btree.OP_OVERFLOW;
                }

                moved += keyLen;
                prevDelta = delta;

                Assert.that(ASSERTION_MSG, moved + (bn + 1) * STRING_KEY_SIZE <= KEY_SPACE);

                setKeyStrSize(b, bn, keyLen);
                setKeyStrOffs(b, bn, KEY_SPACE - moved);

                if (bn == aStart) {
                    setKeyStrOid(b, bn, aInsert.myOID);
                    setKeyBytes(b, KEY_SPACE - moved, bytes);
                } else {
                    setKeyStrOid(b, bn, getKeyStrOid(aPage, i));
                    memcpy(b, KEY_SPACE - moved, aPage, getKeyStrOffs(aPage, i), keyLen, 1);
                    size -= keyLen;
                    i += 1;
                }
            }
        }

        setnItems(aPage, nItems);
        setSize(aPage, size);

        return size + STRING_KEY_SIZE * (nItems + 1) < KEY_SPACE / 3 ? Btree.OP_UNDERFLOW : Btree.OP_DONE;
    }

    static int compactifyStrings(final Page aPage, final int aCount) {
        final int[] size = new int[KEY_SPACE / 2 + 1];
        final int[] index = new int[KEY_SPACE / 2 + 1];

        int count = aCount;
        int i;
        int j;
        int offset;
        int length;
        int numOfItems = getnItems(aPage);

        if (count == 0) {
            return numOfItems;
        }

        int nZeroLengthStrings = 0;

        if (count < 0) {
            count = -count;

            for (i = 0; i < numOfItems - count; i++) {
                length = getKeyStrSize(aPage, i);

                if (length != 0) {
                    offset = getKeyStrOffs(aPage, i) >>> 1;
                    size[offset + length] = length;
                    index[offset + length] = i;
                } else {
                    nZeroLengthStrings += 1;
                }
            }

            for (; i < numOfItems; i++) {
                length = getKeyStrSize(aPage, i);

                if (length != 0) {
                    offset = getKeyStrOffs(aPage, i) >>> 1;
                    size[offset + length] = length;
                    index[offset + length] = -1;
                }
            }
        } else {
            for (i = 0; i < count; i++) {
                length = getKeyStrSize(aPage, i);

                if (length != 0) {
                    offset = getKeyStrOffs(aPage, i) >>> 1;
                    size[offset + length] = length;
                    index[offset + length] = -1;
                }
            }

            for (; i < numOfItems; i++) {
                length = getKeyStrSize(aPage, i);

                if (length != 0) {
                    offset = getKeyStrOffs(aPage, i) >>> 1;
                    size[offset + length] = length;
                    index[offset + length] = i - count;
                } else {
                    nZeroLengthStrings += 1;
                }

                setKeyStrOid(aPage, i - count, getKeyStrOid(aPage, i));
                setKeyStrSize(aPage, i - count, length);
            }

            setKeyStrOid(aPage, i - count, getKeyStrOid(aPage, i));
        }

        final int nItems = numOfItems -= count;

        numOfItems -= nZeroLengthStrings;

        for (offset = KEY_SPACE / 2, i = offset; numOfItems != 0; i -= length) {
            length = size[i];
            j = index[i];

            if (j >= 0) {
                offset -= length;
                numOfItems -= 1;

                setKeyStrOffs(aPage, j, offset * 2);

                if (offset != i - length) {
                    memcpy(aPage, offset, aPage, i - length, length, 2);
                }
            }
        }

        return nItems;
    }

    static int compactifyByteArrays(final Page aPage, final int aCount) {
        final int[] size = new int[KEY_SPACE + 1];
        final int[] index = new int[KEY_SPACE + 1];

        int count = aCount;
        int i;
        int j;
        int offset;
        int length;
        int numOfItems = getnItems(aPage);

        if (count == 0) {
            return numOfItems;
        }

        int nZeroLengthArrays = 0;

        if (count < 0) {
            count = -count;

            for (i = 0; i < numOfItems - count; i++) {
                length = getKeyStrSize(aPage, i);

                if (length != 0) {
                    offset = getKeyStrOffs(aPage, i);
                    size[offset + length] = length;
                    index[offset + length] = i;
                } else {
                    nZeroLengthArrays += 1;
                }
            }

            for (; i < numOfItems; i++) {
                length = getKeyStrSize(aPage, i);

                if (length != 0) {
                    offset = getKeyStrOffs(aPage, i);
                    size[offset + length] = length;
                    index[offset + length] = -1;
                }
            }
        } else {
            for (i = 0; i < count; i++) {
                length = getKeyStrSize(aPage, i);

                if (length != 0) {
                    offset = getKeyStrOffs(aPage, i);
                    size[offset + length] = length;
                    index[offset + length] = -1;
                }
            }

            for (; i < numOfItems; i++) {
                length = getKeyStrSize(aPage, i);

                if (length != 0) {
                    offset = getKeyStrOffs(aPage, i);
                    size[offset + length] = length;
                    index[offset + length] = i - count;
                } else {
                    nZeroLengthArrays += 1;
                }

                setKeyStrOid(aPage, i - count, getKeyStrOid(aPage, i));
                setKeyStrSize(aPage, i - count, length);
            }

            setKeyStrOid(aPage, i - count, getKeyStrOid(aPage, i));
        }

        final int nItems = numOfItems -= count;

        numOfItems -= nZeroLengthArrays;

        for (offset = KEY_SPACE, i = offset; numOfItems != 0; i -= length) {
            length = size[i];
            j = index[i];

            if (j >= 0) {
                offset -= length;
                numOfItems -= 1;
                setKeyStrOffs(aPage, j, offset);

                if (offset != i - length) {
                    memcpy(aPage, offset, aPage, i - length, length, 1);
                }
            }
        }

        return nItems;
    }

    static int removeStrKey(final Page aPage, final int aIndex) {
        final int len = getKeyStrSize(aPage, aIndex) * 2;
        final int offs = getKeyStrOffs(aPage, aIndex);

        int size = getSize(aPage);

        final int numOfItems = getnItems(aPage);

        if ((numOfItems + 1) * STRING_KEY_SIZE >= KEY_SPACE) {
            memcpy(aPage, aIndex, aPage, aIndex + 1, numOfItems - aIndex - 1, STRING_KEY_SIZE);
        } else {
            memcpy(aPage, aIndex, aPage, aIndex + 1, numOfItems - aIndex, STRING_KEY_SIZE);
        }

        if (len != 0) {
            memcpy(aPage, KEY_SPACE - size + len, aPage, KEY_SPACE - size, size - KEY_SPACE + offs, 1);

            for (int i = numOfItems; --i >= 0;) {
                if (getKeyStrOffs(aPage, i) < offs) {
                    setKeyStrOffs(aPage, i, getKeyStrOffs(aPage, i) + len);
                }
            }

            setSize(aPage, size -= len);
        }

        setnItems(aPage, numOfItems - 1);

        return size + STRING_KEY_SIZE * numOfItems < KEY_SPACE / 3 ? Btree.OP_UNDERFLOW : Btree.OP_DONE;
    }

    static int removeByteArrayKey(final Page aPage, final int aIndex) {
        final int length = getKeyStrSize(aPage, aIndex);
        final int offset = getKeyStrOffs(aPage, aIndex);
        final int itemCount = getnItems(aPage);

        int size = getSize(aPage);

        if ((itemCount + 1) * STRING_KEY_SIZE >= KEY_SPACE) {
            memcpy(aPage, aIndex, aPage, aIndex + 1, itemCount - aIndex - 1, STRING_KEY_SIZE);
        } else {
            memcpy(aPage, aIndex, aPage, aIndex + 1, itemCount - aIndex, STRING_KEY_SIZE);
        }

        if (length != 0) {
            memcpy(aPage, KEY_SPACE - size + length, aPage, KEY_SPACE - size, size - KEY_SPACE + offset, 1);

            for (int i = itemCount; --i >= 0;) {
                if (getKeyStrOffs(aPage, i) < offset) {
                    setKeyStrOffs(aPage, i, getKeyStrOffs(aPage, i) + length);
                }
            }

            setSize(aPage, size -= length);
        }

        setnItems(aPage, itemCount - 1);

        return size + STRING_KEY_SIZE * itemCount < KEY_SPACE / 3 ? Btree.OP_UNDERFLOW : Btree.OP_DONE;
    }

    static int replaceStrKey(final StorageImpl aStorage, final Page aPage, final int aIndex, final BtreeKey aInsert,
            final int aHeight) {
        aInsert.myOID = getKeyStrOid(aPage, aIndex);
        removeStrKey(aPage, aIndex);
        return insertStrKey(aStorage, aPage, aIndex, aInsert, aHeight);
    }

    static int replaceByteArrayKey(final StorageImpl aStorage, final Page aPage, final int aIndex,
            final BtreeKey aInsert, final int aHeight) {
        aInsert.myOID = getKeyStrOid(aPage, aIndex);
        removeByteArrayKey(aPage, aIndex);
        return insertByteArrayKey(aStorage, aPage, aIndex, aInsert, aHeight);
    }

    static int handlePageUnderflow(final StorageImpl aStorage, final Page aPage, final int aIndex, final int aType,
            final BtreeKey aRemove, final int aHeight) {
        final int nItems = getnItems(aPage);

        if (aType == ClassDescriptor.TP_STRING) {
            final Page a = aStorage.putPage(getKeyStrOid(aPage, aIndex));
            int an = getnItems(a);

            if (aIndex < nItems) { // exists greater page
                Page b = aStorage.getPage(getKeyStrOid(aPage, aIndex + 1));
                final int bn = getnItems(b);
                int merged_size = (an + bn) * STRING_KEY_SIZE + getSize(a) + getSize(b);

                if (aHeight != 1) {
                    merged_size += getKeyStrSize(aPage, aIndex) * 2 + STRING_KEY_SIZE * 2;
                }

                if (merged_size > KEY_SPACE) {
                    // reallocation of nodes between pages a and b
                    int i;
                    int j;
                    int k;

                    aStorage.myPool.unfix(b);
                    b = aStorage.putPage(getKeyStrOid(aPage, aIndex + 1));

                    int size_a = getSize(a);
                    int size_b = getSize(b);
                    int addSize;
                    int subSize;

                    if (aHeight != 1) {
                        addSize = getKeyStrSize(aPage, aIndex);
                        subSize = getKeyStrSize(b, 0);
                    } else {
                        addSize = subSize = getKeyStrSize(b, 0);
                    }

                    i = 0;

                    int prevDelta = an * STRING_KEY_SIZE + size_a - (bn * STRING_KEY_SIZE + size_b);

                    while (true) {
                        i += 1;

                        final int delta = (an + i) * STRING_KEY_SIZE + size_a + addSize * 2 - ((bn - i) *
                                STRING_KEY_SIZE + size_b - subSize * 2);

                        if (delta >= 0) {
                            if (delta >= -prevDelta) {
                                i -= 1;
                            }
                            break;
                        }

                        size_a += addSize * 2;
                        size_b -= subSize * 2;
                        prevDelta = delta;

                        if (aHeight != 1) {
                            addSize = subSize;
                            subSize = getKeyStrSize(b, i);
                        } else {
                            addSize = subSize = getKeyStrSize(b, i);
                        }
                    }

                    int result = Btree.OP_DONE;

                    if (i > 0) {
                        k = i;

                        if (aHeight != 1) {
                            final int len = getKeyStrSize(aPage, aIndex);

                            setSize(a, getSize(a) + len * 2);
                            setKeyStrOffs(a, an, KEY_SPACE - getSize(a));
                            setKeyStrSize(a, an, len);
                            memcpy(a, getKeyStrOffs(a, an), aPage, getKeyStrOffs(aPage, aIndex), len * 2, 1);
                            k -= 1;
                            an += 1;
                            setKeyStrOid(a, an + k, getKeyStrOid(b, k));
                            setSize(b, getSize(b) - getKeyStrSize(b, k) * 2);
                        }

                        for (j = 0; j < k; j++) {
                            final int len = getKeyStrSize(b, j);

                            setSize(a, getSize(a) + len * 2);
                            setSize(b, getSize(b) - len * 2);
                            setKeyStrOffs(a, an, KEY_SPACE - getSize(a));
                            setKeyStrSize(a, an, len);
                            setKeyStrOid(a, an, getKeyStrOid(b, j));
                            memcpy(a, getKeyStrOffs(a, an), b, getKeyStrOffs(b, j), len * 2, 1);
                            an += 1;
                        }

                        aRemove.getStr(b, i - 1);
                        result = replaceStrKey(aStorage, aPage, aIndex, aRemove, aHeight);
                        setnItems(a, an);
                        setnItems(b, compactifyStrings(b, i));
                    }

                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);

                    return result;
                } else { // merge page b to a
                    if (aHeight != 1) {
                        final int r_len = getKeyStrSize(aPage, aIndex);

                        setKeyStrSize(a, an, r_len);
                        setSize(a, getSize(a) + r_len * 2);
                        setKeyStrOffs(a, an, KEY_SPACE - getSize(a));
                        memcpy(a, getKeyStrOffs(a, an), aPage, getKeyStrOffs(aPage, aIndex), r_len * 2, 1);
                        an += 1;
                        setKeyStrOid(a, an + bn, getKeyStrOid(b, bn));
                    }

                    for (int i = 0; i < bn; i++, an++) {
                        setKeyStrSize(a, an, getKeyStrSize(b, i));
                        setKeyStrOffs(a, an, getKeyStrOffs(b, i) - getSize(a));
                        setKeyStrOid(a, an, getKeyStrOid(b, i));
                    }

                    setSize(a, getSize(a) + getSize(b));
                    setnItems(a, an);
                    memcpy(a, KEY_SPACE - getSize(a), b, KEY_SPACE - getSize(b), getSize(b), 1);
                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);
                    aStorage.freePage(getKeyStrOid(aPage, aIndex + 1));
                    setKeyStrOid(aPage, aIndex + 1, getKeyStrOid(aPage, aIndex));

                    return removeStrKey(aPage, aIndex);
                }
            } else { // page b is before a
                Page b = aStorage.getPage(getKeyStrOid(aPage, aIndex - 1));
                final int bn = getnItems(b);
                int merged_size = (an + bn) * STRING_KEY_SIZE + getSize(a) + getSize(b);

                if (aHeight != 1) {
                    merged_size += getKeyStrSize(aPage, aIndex - 1) * 2 + STRING_KEY_SIZE * 2;
                }

                if (merged_size > KEY_SPACE) {
                    // reallocation of nodes between pages a and b
                    int i;
                    int j;
                    int k;
                    int length;

                    aStorage.myPool.unfix(b);
                    b = aStorage.putPage(getKeyStrOid(aPage, aIndex - 1));

                    int size_a = getSize(a);
                    int size_b = getSize(b);
                    int addSize;
                    int subSize;

                    if (aHeight != 1) {
                        addSize = getKeyStrSize(aPage, aIndex - 1);
                        subSize = getKeyStrSize(b, bn - 1);
                    } else {
                        addSize = subSize = getKeyStrSize(b, bn - 1);
                    }

                    i = 0;

                    int prevDelta = an * STRING_KEY_SIZE + size_a - (bn * STRING_KEY_SIZE + size_b);

                    while (true) {
                        i += 1;

                        final int delta = (an + i) * STRING_KEY_SIZE + size_a + addSize * 2 - ((bn - i) *
                                STRING_KEY_SIZE + size_b - subSize * 2);

                        if (delta >= 0) {
                            if (delta >= -prevDelta) {
                                i -= 1;
                            }

                            break;
                        }

                        prevDelta = delta;
                        size_a += addSize * 2;
                        size_b -= subSize * 2;

                        if (aHeight != 1) {
                            addSize = subSize;
                            subSize = getKeyStrSize(b, bn - i - 1);
                        } else {
                            addSize = subSize = getKeyStrSize(b, bn - i - 1);
                        }
                    }

                    int result = Btree.OP_DONE;

                    if (i > 0) {
                        k = i;

                        Assert.that(i < bn);

                        if (aHeight != 1) {
                            setSize(b, getSize(b) - getKeyStrSize(b, bn - k) * 2);
                            memcpy(a, i, a, 0, an + 1, STRING_KEY_SIZE);
                            k -= 1;
                            setKeyStrOid(a, k, getKeyStrOid(b, bn));
                            length = getKeyStrSize(aPage, aIndex - 1);
                            setKeyStrSize(a, k, length);
                            setSize(a, getSize(a) + length * 2);
                            setKeyStrOffs(a, k, KEY_SPACE - getSize(a));
                            memcpy(a, getKeyStrOffs(a, k), aPage, getKeyStrOffs(aPage, aIndex - 1), length * 2, 1);
                        } else {
                            memcpy(a, i, a, 0, an, STRING_KEY_SIZE);
                        }

                        for (j = 0; j < k; j++) {
                            length = getKeyStrSize(b, bn - k + j);
                            setSize(a, getSize(a) + length * 2);
                            setSize(b, getSize(b) - length * 2);
                            setKeyStrOffs(a, j, KEY_SPACE - getSize(a));
                            setKeyStrSize(a, j, length);
                            setKeyStrOid(a, j, getKeyStrOid(b, bn - k + j));
                            memcpy(a, getKeyStrOffs(a, j), b, getKeyStrOffs(b, bn - k + j), length * 2, 1);
                        }

                        an += i;
                        setnItems(a, an);
                        aRemove.getStr(b, bn - k - 1);
                        result = replaceStrKey(aStorage, aPage, aIndex - 1, aRemove, aHeight);
                        setnItems(b, compactifyStrings(b, -i));
                    }

                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);

                    return result;
                } else { // merge page b to a
                    if (aHeight != 1) {
                        memcpy(a, bn + 1, a, 0, an + 1, STRING_KEY_SIZE);
                        final int length = getKeyStrSize(aPage, aIndex - 1);
                        setKeyStrSize(a, bn, length);
                        setSize(a, getSize(a) + length * 2);
                        setKeyStrOffs(a, bn, KEY_SPACE - getSize(a));
                        setKeyStrOid(a, bn, getKeyStrOid(b, bn));
                        memcpy(a, getKeyStrOffs(a, bn), aPage, getKeyStrOffs(aPage, aIndex - 1), length * 2, 1);
                        an += 1;
                    } else {
                        memcpy(a, bn, a, 0, an, STRING_KEY_SIZE);
                    }

                    for (int i = 0; i < bn; i++) {
                        setKeyStrOid(a, i, getKeyStrOid(b, i));
                        setKeyStrSize(a, i, getKeyStrSize(b, i));
                        setKeyStrOffs(a, i, getKeyStrOffs(b, i) - getSize(a));
                    }

                    an += bn;
                    setnItems(a, an);
                    setSize(a, getSize(a) + getSize(b));
                    memcpy(a, KEY_SPACE - getSize(a), b, KEY_SPACE - getSize(b), getSize(b), 1);
                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);
                    aStorage.freePage(getKeyStrOid(aPage, aIndex - 1));

                    return removeStrKey(aPage, aIndex - 1);
                }
            }
        } else if (aType == ClassDescriptor.TP_ARRAY_OF_BYTES) {
            final Page a = aStorage.putPage(getKeyStrOid(aPage, aIndex));

            int an = getnItems(a);

            if (aIndex < nItems) { // exists greater page
                Page b = aStorage.getPage(getKeyStrOid(aPage, aIndex + 1));
                final int bn = getnItems(b);
                int merged_size = (an + bn) * STRING_KEY_SIZE + getSize(a) + getSize(b);

                if (aHeight != 1) {
                    merged_size += getKeyStrSize(aPage, aIndex) + STRING_KEY_SIZE * 2;
                }

                if (merged_size > KEY_SPACE) {
                    // reallocation of nodes between pages a and b
                    int i;
                    int j;
                    int k;

                    aStorage.myPool.unfix(b);
                    b = aStorage.putPage(getKeyStrOid(aPage, aIndex + 1));

                    int size_a = getSize(a);
                    int size_b = getSize(b);
                    int prevDelta;
                    int addSize;
                    int subSize;

                    if (aHeight != 1) {
                        addSize = getKeyStrSize(aPage, aIndex);
                        subSize = getKeyStrSize(b, 0);
                    } else {
                        addSize = subSize = getKeyStrSize(b, 0);
                    }

                    i = 0;
                    prevDelta = an * STRING_KEY_SIZE + size_a - (bn * STRING_KEY_SIZE + size_b);

                    while (true) {
                        i += 1;

                        final int delta = (an + i) * STRING_KEY_SIZE + size_a + addSize - ((bn - i) *
                                STRING_KEY_SIZE + size_b - subSize);

                        if (delta >= 0) {
                            if (delta >= -prevDelta) {
                                i -= 1;
                            }

                            break;
                        }

                        size_a += addSize;
                        size_b -= subSize;
                        prevDelta = delta;

                        if (aHeight != 1) {
                            addSize = subSize;
                            subSize = getKeyStrSize(b, i);
                        } else {
                            addSize = subSize = getKeyStrSize(b, i);
                        }
                    }

                    int result = Btree.OP_DONE;

                    if (i > 0) {
                        k = i;

                        if (aHeight != 1) {
                            final int len = getKeyStrSize(aPage, aIndex);

                            setSize(a, getSize(a) + len);
                            setKeyStrOffs(a, an, KEY_SPACE - getSize(a));
                            setKeyStrSize(a, an, len);
                            memcpy(a, getKeyStrOffs(a, an), aPage, getKeyStrOffs(aPage, aIndex), len, 1);
                            k -= 1;
                            an += 1;
                            setKeyStrOid(a, an + k, getKeyStrOid(b, k));
                            setSize(b, getSize(b) - getKeyStrSize(b, k));
                        }

                        for (j = 0; j < k; j++) {
                            final int len = getKeyStrSize(b, j);

                            setSize(a, getSize(a) + len);
                            setSize(b, getSize(b) - len);
                            setKeyStrOffs(a, an, KEY_SPACE - getSize(a));
                            setKeyStrSize(a, an, len);
                            setKeyStrOid(a, an, getKeyStrOid(b, j));
                            memcpy(a, getKeyStrOffs(a, an), b, getKeyStrOffs(b, j), len, 1);
                            an += 1;
                        }

                        aRemove.getByteArray(b, i - 1);
                        result = replaceByteArrayKey(aStorage, aPage, aIndex, aRemove, aHeight);
                        setnItems(a, an);
                        setnItems(b, compactifyByteArrays(b, i));
                    }

                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);

                    return result;
                } else { // merge page b to a
                    if (aHeight != 1) {
                        final int r_len = getKeyStrSize(aPage, aIndex);

                        setKeyStrSize(a, an, r_len);
                        setSize(a, getSize(a) + r_len);
                        setKeyStrOffs(a, an, KEY_SPACE - getSize(a));
                        memcpy(a, getKeyStrOffs(a, an), aPage, getKeyStrOffs(aPage, aIndex), r_len, 1);
                        an += 1;
                        setKeyStrOid(a, an + bn, getKeyStrOid(b, bn));
                    }
                    for (int i = 0; i < bn; i++, an++) {
                        setKeyStrSize(a, an, getKeyStrSize(b, i));
                        setKeyStrOffs(a, an, getKeyStrOffs(b, i) - getSize(a));
                        setKeyStrOid(a, an, getKeyStrOid(b, i));
                    }

                    setSize(a, getSize(a) + getSize(b));
                    setnItems(a, an);
                    memcpy(a, KEY_SPACE - getSize(a), b, KEY_SPACE - getSize(b), getSize(b), 1);
                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);
                    aStorage.freePage(getKeyStrOid(aPage, aIndex + 1));
                    setKeyStrOid(aPage, aIndex + 1, getKeyStrOid(aPage, aIndex));

                    return removeByteArrayKey(aPage, aIndex);
                }
            } else { // page b is before a
                Page b = aStorage.getPage(getKeyStrOid(aPage, aIndex - 1));
                final int bn = getnItems(b);
                int merged_size = (an + bn) * STRING_KEY_SIZE + getSize(a) + getSize(b);

                if (aHeight != 1) {
                    merged_size += getKeyStrSize(aPage, aIndex - 1) + STRING_KEY_SIZE * 2;
                }

                if (merged_size > KEY_SPACE) {
                    // reallocation of nodes between pages a and b
                    int i;
                    int j;
                    int k;
                    int length;

                    aStorage.myPool.unfix(b);
                    b = aStorage.putPage(getKeyStrOid(aPage, aIndex - 1));

                    int size_a = getSize(a);
                    int size_b = getSize(b);
                    int prevDelta;
                    int addSize;
                    int subSize;

                    if (aHeight != 1) {
                        addSize = getKeyStrSize(aPage, aIndex - 1);
                        subSize = getKeyStrSize(b, bn - 1);
                    } else {
                        addSize = subSize = getKeyStrSize(b, bn - 1);
                    }

                    i = 0;
                    prevDelta = an * STRING_KEY_SIZE + size_a - (bn * STRING_KEY_SIZE + size_b);

                    while (true) {
                        i += 1;

                        final int delta = (an + i) * STRING_KEY_SIZE + size_a + addSize - ((bn - i) *
                                STRING_KEY_SIZE + size_b - subSize);

                        if (delta >= 0) {
                            if (delta >= -prevDelta) {
                                i -= 1;
                            }

                            break;
                        }

                        prevDelta = delta;
                        size_a += addSize;
                        size_b -= subSize;

                        if (aHeight != 1) {
                            addSize = subSize;
                            subSize = getKeyStrSize(b, bn - i - 1);
                        } else {
                            addSize = subSize = getKeyStrSize(b, bn - i - 1);
                        }
                    }

                    int result = Btree.OP_DONE;

                    if (i > 0) {
                        k = i;

                        Assert.that(i < bn);

                        if (aHeight != 1) {
                            setSize(b, getSize(b) - getKeyStrSize(b, bn - k));
                            memcpy(a, i, a, 0, an + 1, STRING_KEY_SIZE);
                            k -= 1;
                            setKeyStrOid(a, k, getKeyStrOid(b, bn));
                            length = getKeyStrSize(aPage, aIndex - 1);
                            setKeyStrSize(a, k, length);
                            setSize(a, getSize(a) + length);
                            setKeyStrOffs(a, k, KEY_SPACE - getSize(a));
                            memcpy(a, getKeyStrOffs(a, k), aPage, getKeyStrOffs(aPage, aIndex - 1), length, 1);
                        } else {
                            memcpy(a, i, a, 0, an, STRING_KEY_SIZE);
                        }

                        for (j = 0; j < k; j++) {
                            length = getKeyStrSize(b, bn - k + j);
                            setSize(a, getSize(a) + length);
                            setSize(b, getSize(b) - length);
                            setKeyStrOffs(a, j, KEY_SPACE - getSize(a));
                            setKeyStrSize(a, j, length);
                            setKeyStrOid(a, j, getKeyStrOid(b, bn - k + j));
                            memcpy(a, getKeyStrOffs(a, j), b, getKeyStrOffs(b, bn - k + j), length, 1);
                        }

                        an += i;
                        setnItems(a, an);
                        aRemove.getByteArray(b, bn - k - 1);
                        result = replaceByteArrayKey(aStorage, aPage, aIndex - 1, aRemove, aHeight);
                        setnItems(b, compactifyByteArrays(b, -i));
                    }

                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);

                    return result;
                } else { // merge page b to a
                    if (aHeight != 1) {
                        memcpy(a, bn + 1, a, 0, an + 1, STRING_KEY_SIZE);
                        final int length = getKeyStrSize(aPage, aIndex - 1);
                        setKeyStrSize(a, bn, length);
                        setSize(a, getSize(a) + length);
                        setKeyStrOffs(a, bn, KEY_SPACE - getSize(a));
                        setKeyStrOid(a, bn, getKeyStrOid(b, bn));
                        memcpy(a, getKeyStrOffs(a, bn), aPage, getKeyStrOffs(aPage, aIndex - 1), length, 1);
                        an += 1;
                    } else {
                        memcpy(a, bn, a, 0, an, STRING_KEY_SIZE);
                    }

                    for (int i = 0; i < bn; i++) {
                        setKeyStrOid(a, i, getKeyStrOid(b, i));
                        setKeyStrSize(a, i, getKeyStrSize(b, i));
                        setKeyStrOffs(a, i, getKeyStrOffs(b, i) - getSize(a));
                    }

                    an += bn;
                    setnItems(a, an);
                    setSize(a, getSize(a) + getSize(b));
                    memcpy(a, KEY_SPACE - getSize(a), b, KEY_SPACE - getSize(b), getSize(b), 1);
                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);
                    aStorage.freePage(getKeyStrOid(aPage, aIndex - 1));

                    return removeByteArrayKey(aPage, aIndex - 1);
                }
            }
        } else { // scalar types
            final Page a = aStorage.putPage(getReference(aPage, MAX_ITEMS - aIndex - 1));
            int an = getnItems(a);
            final int itemSize = ClassDescriptor.SIZE_OF[aType];

            if (aIndex < nItems) { // exists greater page
                Page b = aStorage.getPage(getReference(aPage, MAX_ITEMS - aIndex - 2));
                int bn = getnItems(b);
                Assert.that(bn >= an);

                if (aHeight != 1) {
                    memcpy(a, an, aPage, aIndex, 1, itemSize);
                    an += 1;
                    bn += 1;
                }

                final int merged_size = (an + bn) * (4 + itemSize);

                if (merged_size > KEY_SPACE) {
                    // reallocation of nodes between pages a and b
                    final int i = bn - (an + bn >> 1);

                    aStorage.myPool.unfix(b);
                    b = aStorage.putPage(getReference(aPage, MAX_ITEMS - aIndex - 2));
                    memcpy(a, an, b, 0, i, itemSize);
                    memcpy(b, 0, b, i, bn - i, itemSize);
                    memcpy(a, MAX_ITEMS - an - i, b, MAX_ITEMS - i, i, 4);
                    memcpy(b, MAX_ITEMS - bn + i, b, MAX_ITEMS - bn, bn - i, 4);
                    memcpy(aPage, aIndex, a, an + i - 1, 1, itemSize);
                    setnItems(b, getnItems(b) - i);
                    setnItems(a, getnItems(a) + i);
                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);

                    return Btree.OP_DONE;
                } else { // merge page b to a
                    memcpy(a, an, b, 0, bn, itemSize);
                    memcpy(a, MAX_ITEMS - an - bn, b, MAX_ITEMS - bn, bn, 4);
                    aStorage.freePage(getReference(aPage, MAX_ITEMS - aIndex - 2));
                    memcpy(aPage, MAX_ITEMS - nItems, aPage, MAX_ITEMS - nItems - 1, nItems - aIndex - 1, 4);
                    memcpy(aPage, aIndex, aPage, aIndex + 1, nItems - aIndex - 1, itemSize);
                    setnItems(a, getnItems(a) + bn);
                    setnItems(aPage, nItems - 1);
                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);

                    return nItems * (itemSize + 4) < KEY_SPACE / 3 ? Btree.OP_UNDERFLOW : Btree.OP_DONE;
                }
            } else { // page b is before a
                Page b = aStorage.getPage(getReference(aPage, MAX_ITEMS - aIndex));
                int bn = getnItems(b);
                Assert.that(bn >= an);

                if (aHeight != 1) {
                    an += 1;
                    bn += 1;
                }

                final int merged_size = (an + bn) * (4 + itemSize);

                if (merged_size > KEY_SPACE) {
                    // reallocation of nodes between pages a and b
                    final int i = bn - (an + bn >> 1);

                    aStorage.myPool.unfix(b);
                    b = aStorage.putPage(getReference(aPage, MAX_ITEMS - aIndex));
                    memcpy(a, i, a, 0, an, itemSize);
                    memcpy(a, 0, b, bn - i, i, itemSize);
                    memcpy(a, MAX_ITEMS - an - i, a, MAX_ITEMS - an, an, 4);
                    memcpy(a, MAX_ITEMS - i, b, MAX_ITEMS - bn, i, 4);

                    if (aHeight != 1) {
                        memcpy(a, i - 1, aPage, aIndex - 1, 1, itemSize);
                    }

                    memcpy(aPage, aIndex - 1, b, bn - i - 1, 1, itemSize);
                    setnItems(b, getnItems(b) - i);
                    setnItems(a, getnItems(a) + i);
                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);

                    return Btree.OP_DONE;
                } else { // merge page b to a
                    memcpy(a, bn, a, 0, an, itemSize);
                    memcpy(a, 0, b, 0, bn, itemSize);
                    memcpy(a, MAX_ITEMS - an - bn, a, MAX_ITEMS - an, an, 4);
                    memcpy(a, MAX_ITEMS - bn, b, MAX_ITEMS - bn, bn, 4);

                    if (aHeight != 1) {
                        memcpy(a, bn - 1, aPage, aIndex - 1, 1, itemSize);
                    }

                    aStorage.freePage(getReference(aPage, MAX_ITEMS - aIndex));
                    setReference(aPage, MAX_ITEMS - aIndex, getReference(aPage, MAX_ITEMS - aIndex - 1));
                    setnItems(a, getnItems(a) + bn);
                    setnItems(aPage, nItems - 1);
                    aStorage.myPool.unfix(a);
                    aStorage.myPool.unfix(b);

                    return nItems * (itemSize + 4) < KEY_SPACE / 3 ? Btree.OP_UNDERFLOW : Btree.OP_DONE;
                }
            }
        }
    }

    static int remove(final StorageImpl aStorage, final int aPageID, final Btree aBtree, final BtreeKey aRemove,
            final int aHeight) {
        Page pg = aStorage.getPage(aPageID);
        int height = aHeight;

        try {
            int n = getnItems(pg);
            int l = 0;
            int r = n;
            int i;

            if (aBtree.myType == ClassDescriptor.TP_STRING) {
                while (l < r) {
                    i = l + r >> 1;

                    if (compareStr(aRemove.myKey, pg, i) > 0) {
                        l = i + 1;
                    } else {
                        r = i;
                    }
                }

                if (--height != 0) {
                    do {
                        switch (remove(aStorage, getKeyStrOid(pg, r), aBtree, aRemove, height)) {
                            case Btree.OP_UNDERFLOW:
                                aStorage.myPool.unfix(pg);
                                pg = null;
                                pg = aStorage.putPage(aPageID);
                                return handlePageUnderflow(aStorage, pg, r, aBtree.myType, aRemove, height);
                            case Btree.OP_DONE:
                                return Btree.OP_DONE;
                            case Btree.OP_OVERFLOW:
                                aStorage.myPool.unfix(pg);
                                pg = null;
                                pg = aStorage.putPage(aPageID);
                                return insertStrKey(aStorage, pg, r, aRemove, height);
                            default:
                                // FIXME
                        }
                    } while (++r <= n);
                } else {
                    while (r < n) {
                        if (compareStr(aRemove.myKey, pg, r) == 0) {
                            final int oid = getKeyStrOid(pg, r);

                            if (oid == aRemove.myOID || aRemove.myOID == 0) {
                                aRemove.myOldOID = oid;
                                aStorage.myPool.unfix(pg);
                                pg = null;
                                pg = aStorage.putPage(aPageID);

                                return removeStrKey(pg, r);
                            }
                        } else {
                            break;
                        }

                        r += 1;
                    }
                }
            } else if (aBtree.myType == ClassDescriptor.TP_ARRAY_OF_BYTES) {
                while (l < r) {
                    i = l + r >> 1;

                    if (aBtree.compareByteArrays(aRemove.myKey, pg, i) > 0) {
                        l = i + 1;
                    } else {
                        r = i;
                    }
                }

                if (--height != 0) {
                    do {
                        switch (remove(aStorage, getKeyStrOid(pg, r), aBtree, aRemove, height)) {
                            case Btree.OP_UNDERFLOW:
                                aStorage.myPool.unfix(pg);
                                pg = null;
                                pg = aStorage.putPage(aPageID);
                                return handlePageUnderflow(aStorage, pg, r, aBtree.myType, aRemove, height);
                            case Btree.OP_DONE:
                                return Btree.OP_DONE;
                            case Btree.OP_OVERFLOW:
                                aStorage.myPool.unfix(pg);
                                pg = null;
                                pg = aStorage.putPage(aPageID);
                                return insertByteArrayKey(aStorage, pg, r, aRemove, height);
                            default:
                                // FIXME
                        }
                    } while (++r <= n);
                } else {
                    while (r < n) {
                        if (aBtree.compareByteArrays(aRemove.myKey, pg, r) == 0) {
                            final int oid = getKeyStrOid(pg, r);

                            if (oid == aRemove.myOID || aRemove.myOID == 0) {
                                aRemove.myOldOID = oid;
                                aStorage.myPool.unfix(pg);
                                pg = null;
                                pg = aStorage.putPage(aPageID);

                                return removeByteArrayKey(pg, r);
                            }
                        } else {
                            break;
                        }

                        r += 1;
                    }
                }
            } else { // scalar types
                final int itemSize = ClassDescriptor.SIZE_OF[aBtree.myType];

                while (l < r) {
                    i = l + r >> 1;

                    if (compare(aRemove.myKey, pg, i) > 0) {
                        l = i + 1;
                    } else {
                        r = i;
                    }
                }

                if (--height == 0) {
                    final int oid = aRemove.myOID;

                    while (r < n) {
                        if (compare(aRemove.myKey, pg, r) == 0) {
                            if (getReference(pg, MAX_ITEMS - r - 1) == oid || oid == 0) {
                                aRemove.myOldOID = getReference(pg, MAX_ITEMS - r - 1);
                                aStorage.myPool.unfix(pg);
                                pg = null;
                                pg = aStorage.putPage(aPageID);
                                memcpy(pg, r, pg, r + 1, n - r - 1, itemSize);
                                memcpy(pg, MAX_ITEMS - n + 1, pg, MAX_ITEMS - n, n - r - 1, 4);
                                setnItems(pg, --n);

                                return n * (itemSize + 4) < KEY_SPACE / 3 ? Btree.OP_UNDERFLOW : Btree.OP_DONE;
                            }
                        } else {
                            break;
                        }

                        r += 1;
                    }

                    return Btree.OP_NOT_FOUND;
                }
                do {
                    switch (remove(aStorage, getReference(pg, MAX_ITEMS - r - 1), aBtree, aRemove, height)) {
                        case Btree.OP_UNDERFLOW:
                            aStorage.myPool.unfix(pg);
                            pg = null;
                            pg = aStorage.putPage(aPageID);
                            return handlePageUnderflow(aStorage, pg, r, aBtree.myType, aRemove, height);
                        case Btree.OP_DONE:
                            return Btree.OP_DONE;
                        default:
                            // FIXME
                    }
                } while (++r <= n);
            }

            return Btree.OP_NOT_FOUND;
        } finally {
            if (pg != null) {
                aStorage.myPool.unfix(pg);
            }
        }
    }

    static void purge(final StorageImpl aStorage, final int aPageID, final int aType, final int aHeight) {
        int height = aHeight;

        if (--height != 0) {
            final Page pg = aStorage.getPage(aPageID);

            int n = getnItems(pg) + 1;

            if (aType == ClassDescriptor.TP_STRING || aType == ClassDescriptor.TP_ARRAY_OF_BYTES) { // page of strings
                while (--n >= 0) {
                    purge(aStorage, getKeyStrOid(pg, n), aType, height);
                }
            } else {
                while (--n >= 0) {
                    purge(aStorage, getReference(pg, MAX_ITEMS - n - 1), aType, height);
                }
            }

            aStorage.myPool.unfix(pg);
        }

        aStorage.freePage(aPageID);
    }

    static int traverseForward(final StorageImpl aStorage, final int aPageID, final int aType, final int aHeight,
            final Object[] aResult, final int aPosition) {
        final Page pg = aStorage.getPage(aPageID);

        int position = aPosition;
        int height = aHeight;
        int oid;

        try {
            final int n = getnItems(pg);

            int index;

            if (--height != 0) {
                if (aType == ClassDescriptor.TP_STRING || aType == ClassDescriptor.TP_ARRAY_OF_BYTES) { // page of
                                                                                                        // strings
                    for (index = 0; index <= n; index++) {
                        position = traverseForward(aStorage, getKeyStrOid(pg, index), aType, height, aResult,
                                position);
                    }
                } else {
                    for (index = 0; index <= n; index++) {
                        position = traverseForward(aStorage, getReference(pg, MAX_ITEMS - index - 1), aType, height,
                                aResult, position);
                    }
                }
            } else {
                if (aType == ClassDescriptor.TP_STRING || aType == ClassDescriptor.TP_ARRAY_OF_BYTES) { // page of
                                                                                                        // strings
                    for (index = 0; index < n; index++) {
                        oid = getKeyStrOid(pg, index);
                        aResult[position++] = aStorage.lookupObject(oid, null);
                    }
                } else { // page of scalars
                    for (index = 0; index < n; index++) {
                        oid = getReference(pg, MAX_ITEMS - 1 - index);
                        aResult[position++] = aStorage.lookupObject(oid, null);
                    }
                }
            }

            return position;
        } finally {
            aStorage.myPool.unfix(pg);
        }
    }

    static int markPage(final StorageImpl aStorage, final int aPageID, final int aType, final int aHeight) {
        final Page page = aStorage.getGCPage(aPageID);

        int height = aHeight;
        int pageCount = 1;

        try {
            final int n = getnItems(page);

            int index;

            if (--height != 0) {
                if (aType == ClassDescriptor.TP_STRING || aType == ClassDescriptor.TP_ARRAY_OF_BYTES) { // page of
                                                                                                        // strings
                    for (index = 0; index <= n; index++) {
                        pageCount += markPage(aStorage, getKeyStrOid(page, index), aType, height);
                    }
                } else {
                    for (index = 0; index <= n; index++) {
                        pageCount += markPage(aStorage, getReference(page, MAX_ITEMS - index - 1), aType, height);
                    }
                }
            } else {
                if (aType == ClassDescriptor.TP_STRING || aType == ClassDescriptor.TP_ARRAY_OF_BYTES) { // page of
                                                                                                        // strings
                    for (index = 0; index < n; index++) {
                        aStorage.markOid(getKeyStrOid(page, index));
                    }
                } else { // page of scalars
                    for (index = 0; index < n; index++) {
                        aStorage.markOid(getReference(page, MAX_ITEMS - 1 - index));
                    }
                }
            }
        } finally {
            aStorage.myPool.unfix(page);
        }

        return pageCount;
    }

    static void exportPage(final StorageImpl aStorage, final XMLExporter aExporter, final int aPageID,
            final int aType, final int aHeight) throws IOException {
        final Page page = aStorage.getPage(aPageID);

        int height = aHeight;

        try {
            final int itemCount = getnItems(page);

            int i;

            if (--height != 0) {
                if (aType == ClassDescriptor.TP_STRING || aType == ClassDescriptor.TP_ARRAY_OF_BYTES) { // page of
                                                                                                        // strings
                    for (i = 0; i <= itemCount; i++) {
                        exportPage(aStorage, aExporter, getKeyStrOid(page, i), aType, height);
                    }
                } else {
                    for (i = 0; i <= itemCount; i++) {
                        exportPage(aStorage, aExporter, getReference(page, MAX_ITEMS - i - 1), aType, height);
                    }
                }
            } else {
                if (aType == ClassDescriptor.TP_STRING || aType == ClassDescriptor.TP_ARRAY_OF_BYTES) { // page of
                                                                                                        // strings
                    for (i = 0; i < itemCount; i++) {
                        aExporter.exportAssoc(getKeyStrOid(page, i), page.myData, BtreePage.FIRST_KEY_OFFSET +
                                BtreePage.getKeyStrOffs(page, i), BtreePage.getKeyStrSize(page, i), aType);
                    }
                } else {
                    for (i = 0; i < itemCount; i++) {
                        aExporter.exportAssoc(getReference(page, MAX_ITEMS - 1 - i), page.myData,
                                BtreePage.FIRST_KEY_OFFSET + i * ClassDescriptor.SIZE_OF[aType],
                                ClassDescriptor.SIZE_OF[aType], aType);

                    }
                }
            }
        } finally {
            aStorage.myPool.unfix(page);
        }
    }

}
