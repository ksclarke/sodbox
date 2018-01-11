
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

    static class BtreeEntry<T> implements Map.Entry<Object, T> {

        private final int pos;

        private final BtreePage pg;

        BtreeEntry(final BtreePage pg, final int pos) {
            this.pg = pg;
            this.pos = pos;
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }

            final Map.Entry e = (Map.Entry) o;

            return (getKey() == null ? e.getKey() == null : getKey().equals(e.getKey())) && (getValue() == null ? e
                    .getValue() == null : getValue().equals(e.getValue()));
        }

        @Override
        public Object getKey() {
            return pg.getKeyValue(pos);
        }

        @Override
        public T getValue() {
            return (T) pg.items.get(pos);
        }

        @Override
        public int hashCode() {
            return (getKey() == null ? 0 : getKey().hashCode()) ^ (getValue() == null ? 0 : getValue().hashCode());
        }

        @Override
        public T setValue(final T value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return getKey() + "=" + getValue();
        }

    }

    class BtreeEntryStartFromIterator extends BtreeSelectionEntryIterator {

        int start;

        BtreeEntryStartFromIterator(final int start, final int order) {
            super(null, null, order);
            this.start = start;
            reset();
        }

        @Override
        void reset() {
            super.reset();
            int skip = order == ASCENT_ORDER ? start : nElems - start - 1;
            while (--skip >= 0 && hasNext()) {
                next();
            }
        }

    }

    static class BtreeKey {

        Key key;

        Object node;

        Object oldNode;

        BtreeKey(final Key key, final Object node) {
            this.key = key;
            this.node = node;
        }
    }

    static abstract class BtreePage extends Persistent {

        static final int BTREE_PAGE_SIZE = Page.pageSize - ObjectHeader.sizeof - 4 * 3;

        static void memcpy(final BtreePage dst_pg, final int dst_idx, final BtreePage src_pg, final int src_idx,
                final int len) {
            memcpyData(dst_pg, dst_idx, src_pg, src_idx, len);
            memcpyItems(dst_pg, dst_idx, src_pg, src_idx, len);
        }

        static void memcpyData(final BtreePage dst_pg, final int dst_idx, final BtreePage src_pg, final int src_idx,
                final int len) {
            System.arraycopy(src_pg.getData(), src_idx, dst_pg.getData(), dst_idx, len);
        }

        static void memcpyItems(final BtreePage dst_pg, final int dst_idx, final BtreePage src_pg, final int src_idx,
                final int len) {
            System.arraycopy(src_pg.items.toRawArray(), src_idx, dst_pg.items.toRawArray(), dst_idx, len);
        }

        int nItems;

        Link items;

        BtreePage() {
        }

        BtreePage(final Storage s, final int n) {
            super(s);

            items = s.createLink(n);
            items.setSize(n);
        }

        void clearKeyValue(final int i) {
        }

        abstract BtreePage clonePage();

        abstract int compare(Key key, int i);

        boolean find(final Key firstKey, final Key lastKey, int height, final ArrayList result) {
            int l = 0;
            final int n = nItems;
            int r = n;
            height -= 1;

            if (firstKey != null) {
                while (l < r) {
                    final int i = l + r >> 1;

                    if (compare(firstKey, i) >= firstKey.inclusion) {
                        l = i + 1;
                    } else {
                        r = i;
                    }
                }

                Assert.that(r == l);
            }

            if (lastKey != null) {
                if (height == 0) {
                    while (l < n) {
                        if (-compare(lastKey, l) >= lastKey.inclusion) {
                            return false;
                        }

                        result.add(items.get(l));
                        l += 1;
                    }

                    return true;
                } else {
                    do {
                        if (!((BtreePage) items.get(l)).find(firstKey, lastKey, height, result)) {
                            return false;
                        }
                        if (l == n) {
                            return true;
                        }
                    } while (compare(lastKey, l++) >= 0);

                    return false;
                }
            }
            if (height == 0) {
                while (l < n) {
                    result.add(items.get(l));
                    l += 1;
                }
            } else {
                do {
                    if (!((BtreePage) items.get(l)).find(firstKey, lastKey, height, result)) {
                        return false;
                    }
                } while (++l <= n);
            }

            return true;
        }

        abstract Object getData();

        abstract Key getKey(int i);

        abstract Object getKeyValue(int i);

        int handlePageUnderflow(final int r, final BtreeKey rem, final int height) {
            final BtreePage a = (BtreePage) items.get(r);

            a.modify();
            modify();

            int an = a.nItems;

            if (r < nItems) { // exists greater page
                final BtreePage b = (BtreePage) items.get(r + 1);
                int bn = b.nItems;

                Assert.that(bn >= an);

                if (height != 1) {
                    memcpyData(a, an, this, r, 1);
                    an += 1;
                    bn += 1;
                }
                if (an + bn > items.size()) {
                    // reallocation of nodes between pages a and b
                    final int i = bn - (an + bn >> 1);

                    b.modify();
                    memcpy(a, an, b, 0, i);
                    memcpy(b, 0, b, i, bn - i);
                    memcpyData(this, r, a, an + i - 1, 1);

                    if (height != 1) {
                        a.clearKeyValue(an + i - 1);
                    }

                    b.memset(bn - i, i);
                    b.nItems -= i;
                    a.nItems += i;

                    return op_done;
                } else { // merge page b to a
                    memcpy(a, an, b, 0, bn);
                    b.deallocate();
                    memcpyData(this, r, this, r + 1, nItems - r - 1);
                    memcpyItems(this, r + 1, this, r + 2, nItems - r - 1);
                    items.setObject(nItems, null);
                    a.nItems += bn;
                    nItems -= 1;

                    return nItems < items.size() / 3 ? op_underflow : op_done;
                }
            } else { // page b is before a
                final BtreePage b = (BtreePage) items.get(r - 1);
                int bn = b.nItems;

                Assert.that(bn >= an);

                if (height != 1) {
                    an += 1;
                    bn += 1;
                }

                if (an + bn > items.size()) {
                    // reallocation of nodes between pages a and b
                    final int i = bn - (an + bn >> 1);

                    b.modify();
                    memcpy(a, i, a, 0, an);
                    memcpy(a, 0, b, bn - i, i);

                    if (height != 1) {
                        memcpyData(a, i - 1, this, r - 1, 1);
                    }

                    memcpyData(this, r - 1, b, bn - i - 1, 1);

                    if (height != 1) {
                        b.clearKeyValue(bn - i - 1);
                    }

                    b.memset(bn - i, i);
                    b.nItems -= i;
                    a.nItems += i;

                    return op_done;
                } else { // merge page b to a
                    memcpy(a, bn, a, 0, an);
                    memcpy(a, 0, b, 0, bn);

                    if (height != 1) {
                        memcpyData(a, bn - 1, this, r - 1, 1);
                    }

                    b.deallocate();
                    items.setObject(r - 1, a);
                    items.setObject(nItems, null);
                    a.nItems += bn;
                    nItems -= 1;

                    return nItems < items.size() / 3 ? op_underflow : op_done;
                }
            }
        }

        abstract void insert(BtreeKey key, int i);

        int insert(final BtreeKey ins, int height, final boolean unique, final boolean overwrite) {
            int result;
            int l = 0, n = nItems, r = n;
            final int ahead = unique ? 1 : 0;

            while (l < r) {
                final int i = l + r >> 1;

                if (compare(ins.key, i) >= ahead) {
                    l = i + 1;
                } else {
                    r = i;
                }
            }

            Assert.that(l == r);

            /* insert before e[r] */
            if (--height != 0) {
                result = ((BtreePage) items.get(r)).insert(ins, height, unique, overwrite);

                Assert.that(result != op_not_found);

                if (result != op_overflow) {
                    return result;
                }

                n += 1;
            } else if (r < n && compare(ins.key, r) == 0) {
                if (overwrite) {
                    ins.oldNode = items.get(r);
                    modify();
                    items.setObject(r, ins.node);
                    return op_overwrite;
                } else if (unique) {
                    ins.oldNode = items.get(r);
                    return op_duplicate;
                }
            }

            final int max = items.size();
            modify();

            if (n < max) {
                memcpy(this, r + 1, this, r, n - r);
                insert(ins, r);
                nItems += 1;
                return op_done;
            } else { /* page is full then divide page */
                final BtreePage b = clonePage();
                Assert.that(n == max);
                final int m = (max + 1) / 2;

                if (r < m) {
                    memcpy(b, 0, this, 0, r);
                    memcpy(b, r + 1, this, r, m - r - 1);
                    memcpy(this, 0, this, m - 1, max - m + 1);
                    b.insert(ins, r);
                } else {
                    memcpy(b, 0, this, 0, m);
                    memcpy(this, 0, this, m, r - m);
                    memcpy(this, r - m + 1, this, r, max - r);
                    insert(ins, r - m);
                }

                memset(max - m + 1, m - 1);
                ins.node = b;
                ins.key = b.getKey(m - 1);

                if (height == 0) {
                    nItems = max - m + 1;
                    b.nItems = m;
                } else {
                    b.clearKeyValue(m - 1);
                    nItems = max - m;
                    b.nItems = m - 1;
                }

                return op_overflow;
            }
        }

        void memset(int i, int len) {
            while (--len >= 0) {
                items.setObject(i++, null);
            }
        }

        void purge(int height) {
            if (--height != 0) {
                int n = nItems;

                do {
                    ((BtreePage) items.get(n)).purge(height);
                } while (--n >= 0);
            }

            super.deallocate();
        }

        int remove(final BtreeKey rem, int height) {
            int i, n = nItems, l = 0, r = n;

            while (l < r) {
                i = l + r >> 1;

                if (compare(rem.key, i) > 0) {
                    l = i + 1;
                } else {
                    r = i;
                }
            }

            if (--height == 0) {
                final Object node = rem.node;

                while (r < n) {
                    if (compare(rem.key, r) == 0) {
                        if (node == null || items.containsElement(r, node)) {
                            rem.oldNode = items.get(r);
                            modify();
                            memcpy(this, r, this, r + 1, n - r - 1);
                            nItems = --n;
                            memset(n, 1);
                            return n < items.size() / 3 ? op_underflow : op_done;
                        }
                    } else {
                        break;
                    }

                    r += 1;
                }
                return op_not_found;
            }

            do {
                switch (((BtreePage) items.get(r)).remove(rem, height)) {
                    case op_underflow:
                        return handlePageUnderflow(r, rem, height);
                    case op_done:
                        return op_done;
                }
            } while (++r <= n);

            return op_not_found;
        }

        int traverseForward(int height, final Object[] result, int pos) {
            int i;
            final int n = nItems;

            if (--height != 0) {
                for (i = 0; i <= n; i++) {
                    pos = ((BtreePage) items.get(i)).traverseForward(height, result, pos);
                }
            } else {
                for (i = 0; i < n; i++) {
                    result[pos++] = items.get(i);
                }
            }

            return pos;
        }

    }

    static class BtreePageOfBoolean extends BtreePageOfByte {

        BtreePageOfBoolean() {
        }

        BtreePageOfBoolean(final Storage s) {
            super(s);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfBoolean(getStorage());
        }

        @Override
        Key getKey(final int i) {
            return new Key(data[i] != 0);
        }

        @Override
        Object getKeyValue(final int i) {
            return Boolean.valueOf(data[i] != 0);
        }

    }

    static class BtreePageOfByte extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 1);

        byte[] data;

        BtreePageOfByte() {
        }

        BtreePageOfByte(final Storage s) {
            super(s, MAX_ITEMS);
            data = new byte[MAX_ITEMS];
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfByte(getStorage());
        }

        @Override
        int compare(final Key key, final int i) {
            return (byte) key.ival - data[i];
        }

        @Override
        Object getData() {
            return data;
        }

        @Override
        Key getKey(final int i) {
            return new Key(data[i]);
        }

        @Override
        Object getKeyValue(final int i) {
            return new Byte(data[i]);
        }

        @Override
        void insert(final BtreeKey key, final int i) {
            items.setObject(i, key.node);
            data[i] = (byte) key.key.ival;
        }

    }

    static class BtreePageOfChar extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 2);

        char[] data;

        BtreePageOfChar() {
        }

        BtreePageOfChar(final Storage s) {
            super(s, MAX_ITEMS);
            data = new char[MAX_ITEMS];
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfChar(getStorage());
        }

        @Override
        int compare(final Key key, final int i) {
            return (char) key.ival - data[i];
        }

        @Override
        Object getData() {
            return data;
        }

        @Override
        Key getKey(final int i) {
            return new Key(data[i]);
        }

        @Override
        Object getKeyValue(final int i) {
            return new Character(data[i]);
        }

        @Override
        void insert(final BtreeKey key, final int i) {
            items.setObject(i, key.node);
            data[i] = (char) key.key.ival;
        }

    }

    static class BtreePageOfDouble extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 8);

        double[] data;

        BtreePageOfDouble() {
        }

        BtreePageOfDouble(final Storage s) {
            super(s, MAX_ITEMS);
            data = new double[MAX_ITEMS];
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfDouble(getStorage());
        }

        @Override
        int compare(final Key key, final int i) {
            return key.dval < data[i] ? -1 : key.dval == data[i] ? 0 : 1;
        }

        @Override
        Object getData() {
            return data;
        }

        @Override
        Key getKey(final int i) {
            return new Key(data[i]);
        }

        @Override
        Object getKeyValue(final int i) {
            return new Double(data[i]);
        }

        @Override
        void insert(final BtreeKey key, final int i) {
            items.setObject(i, key.node);
            data[i] = key.key.dval;
        }

    }

    static class BtreePageOfFloat extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4);

        float[] data;

        BtreePageOfFloat() {
        }

        BtreePageOfFloat(final Storage s) {
            super(s, MAX_ITEMS);
            data = new float[MAX_ITEMS];
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfFloat(getStorage());
        }

        @Override
        int compare(final Key key, final int i) {
            return (float) key.dval < data[i] ? -1 : (float) key.dval == data[i] ? 0 : 1;
        }

        @Override
        Object getData() {
            return data;
        }

        @Override
        Key getKey(final int i) {
            return new Key(data[i]);
        }

        @Override
        Object getKeyValue(final int i) {
            return new Float(data[i]);
        }

        @Override
        void insert(final BtreeKey key, final int i) {
            items.setObject(i, key.node);
            data[i] = (float) key.key.dval;
        }

    }

    static class BtreePageOfInt extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4);

        int[] data;

        BtreePageOfInt() {
        }

        BtreePageOfInt(final Storage s) {
            super(s, MAX_ITEMS);
            data = new int[MAX_ITEMS];
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfInt(getStorage());
        }

        @Override
        int compare(final Key key, final int i) {
            return key.ival < data[i] ? -1 : key.ival == data[i] ? 0 : 1;
        }

        @Override
        Object getData() {
            return data;
        }

        @Override
        Key getKey(final int i) {
            return new Key(data[i]);
        }

        @Override
        Object getKeyValue(final int i) {
            return new Integer(data[i]);
        }

        @Override
        void insert(final BtreeKey key, final int i) {
            items.setObject(i, key.node);
            data[i] = key.key.ival;
        }

    }

    static class BtreePageOfLong extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 8);

        long[] data;

        BtreePageOfLong() {
        }

        BtreePageOfLong(final Storage s) {
            super(s, MAX_ITEMS);
            data = new long[MAX_ITEMS];
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfLong(getStorage());
        }

        @Override
        int compare(final Key key, final int i) {
            return key.lval < data[i] ? -1 : key.lval == data[i] ? 0 : 1;
        }

        @Override
        Object getData() {
            return data;
        }

        @Override
        Key getKey(final int i) {
            return new Key(data[i]);
        }

        @Override
        Object getKeyValue(final int i) {
            return new Long(data[i]);
        }

        @Override
        void insert(final BtreeKey key, final int i) {
            items.setObject(i, key.node);
            data[i] = key.key.lval;
        }

    }

    static class BtreePageOfObject extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 4);

        Link data;

        BtreePageOfObject() {
        }

        BtreePageOfObject(final Storage s) {
            super(s, MAX_ITEMS);
            data = s.createLink(MAX_ITEMS);
            data.setSize(MAX_ITEMS);
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfObject(getStorage());
        }

        @Override
        int compare(final Key key, final int i) {
            final Object obj = data.getRaw(i);
            final int oid = obj == null ? 0 : getStorage().getOid(obj);
            return key.ival < oid ? -1 : key.ival == oid ? 0 : 1;
        }

        @Override
        Object getData() {
            return data.toRawArray();
        }

        @Override
        Key getKey(final int i) {
            return new Key(data.getRaw(i));
        }

        @Override
        Object getKeyValue(final int i) {
            return data.get(i);
        }

        @Override
        void insert(final BtreeKey key, final int i) {
            items.setObject(i, key.node);
            data.setObject(i, key.key.oval);
        }

    }

    static class BtreePageOfShort extends BtreePage {

        static final int MAX_ITEMS = BTREE_PAGE_SIZE / (4 + 2);

        short[] data;

        BtreePageOfShort() {
        }

        BtreePageOfShort(final Storage s) {
            super(s, MAX_ITEMS);
            data = new short[MAX_ITEMS];
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfShort(getStorage());
        }

        @Override
        int compare(final Key key, final int i) {
            return (short) key.ival - data[i];
        }

        @Override
        Object getData() {
            return data;
        }

        @Override
        Key getKey(final int i) {
            return new Key(data[i]);
        }

        @Override
        Object getKeyValue(final int i) {
            return new Short(data[i]);
        }

        @Override
        void insert(final BtreeKey key, final int i) {
            items.setObject(i, key.node);
            data[i] = (short) key.key.ival;
        }

    }

    static class BtreePageOfString extends BtreePage {

        static final int MAX_ITEMS = 100;

        String[] data;

        BtreePageOfString() {
        }

        BtreePageOfString(final Storage s) {
            super(s, MAX_ITEMS);
            data = new String[MAX_ITEMS];
        }

        @Override
        void clearKeyValue(final int i) {
            data[i] = null;
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfString(getStorage());
        }

        @Override
        int compare(final Key key, final int i) {
            return ((String) key.oval).compareTo(data[i]);
        }

        @Override
        Object getData() {
            return data;
        }

        @Override
        Key getKey(final int i) {
            return new Key(data[i]);
        }

        @Override
        Object getKeyValue(final int i) {
            return data[i];
        }

        @Override
        void insert(final BtreeKey key, final int i) {
            items.setObject(i, key.node);
            data[i] = (String) key.key.oval;
        }

        @Override
        void memset(int i, int len) {
            while (--len >= 0) {
                items.setObject(i, null);
                data[i] = null;
                i += 1;
            }
        }

        boolean prefixSearch(final String key, int height, final ArrayList result) {
            int l = 0;
            final int n = nItems;
            int r = n;

            height -= 1;

            while (l < r) {
                final int i = l + r >> 1;

                if (!key.startsWith(data[i]) && key.compareTo(data[i]) > 0) {
                    l = i + 1;
                } else {
                    r = i;
                }
            }

            Assert.that(r == l);

            if (height == 0) {
                while (l < n) {
                    if (key.compareTo(data[l]) < 0) {
                        return false;
                    }

                    result.add(items.get(l));
                    l += 1;
                }
            } else {
                do {
                    if (!((BtreePageOfString) items.get(l)).prefixSearch(key, height, result)) {
                        return false;
                    }
                    if (l == n) {
                        return true;
                    }
                } while (key.compareTo(data[l++]) >= 0);

                return false;
            }

            return true;
        }

    }

    static class BtreePageOfValue extends BtreePage {

        static final int MAX_ITEMS = 100;

        Object[] data;

        BtreePageOfValue() {
        }

        BtreePageOfValue(final Storage s) {
            super(s, MAX_ITEMS);
            data = new Object[MAX_ITEMS];
        }

        @Override
        void clearKeyValue(final int i) {
            data[i] = null;
        }

        @Override
        BtreePage clonePage() {
            return new BtreePageOfValue(getStorage());
        }

        @Override
        int compare(final Key key, final int i) {
            return ((Comparable) key.oval).compareTo(data[i]);
        }

        @Override
        Object getData() {
            return data;
        }

        @Override
        Key getKey(final int i) {
            return new Key((IValue) data[i]);
        }

        @Override
        Object getKeyValue(final int i) {
            return data[i];
        }

        @Override
        void insert(final BtreeKey key, final int i) {
            items.setObject(i, key.node);
            data[i] = key.key.oval;
        }

    }

    class BtreeSelectionEntryIterator extends BtreeSelectionIterator<Map.Entry<Object, T>> {

        BtreeSelectionEntryIterator(final Key from, final Key till, final int order) {
            super(from, till, order);
        }

        @Override
        protected Object getCurrent(final BtreePage pg, final int pos) {
            return new BtreeEntry(pg, pos);
        }
    }

    class BtreeSelectionIterator<E> extends IterableIterator<E> implements PersistentIterator {

        BtreePage[] pageStack;

        int[] posStack;

        BtreePage currPage;

        int currPos;

        int sp;

        int end;

        Key from;

        Key till;

        int order;

        int counter;

        BtreeKey currKey;

        Key nextKey;

        Object nextObj;

        BtreeSelectionIterator(final Key from, final Key till, final int order) {
            this.from = from;
            this.till = till;
            this.order = order;
            reset();
        }

        protected Object getCurrent(final BtreePage pg, final int pos) {
            return pg.items.get(pos);
        }

        protected final void gotoNextItem(BtreePage pg, int pos) {
            if (order == ASCENT_ORDER) {
                if (++pos == end) {
                    while (--sp != 0) {
                        pos = posStack[sp - 1];
                        pg = pageStack[sp - 1];

                        if (++pos <= pg.nItems) {
                            posStack[sp - 1] = pos;

                            do {
                                pg = (BtreePage) pg.items.get(pos);
                                end = pg.nItems;
                                pageStack[sp] = pg;
                                posStack[sp] = pos = 0;
                            } while (++sp < pageStack.length);

                            break;
                        }
                    }
                } else {
                    posStack[sp - 1] = pos;
                }
                if (sp != 0 && till != null && -pg.compare(till, pos) >= till.inclusion) {
                    sp = 0;
                }
            } else { // descent order
                if (--pos < 0) {
                    while (--sp != 0) {
                        pos = posStack[sp - 1];
                        pg = pageStack[sp - 1];

                        if (--pos >= 0) {
                            posStack[sp - 1] = pos;

                            do {
                                pg = (BtreePage) pg.items.get(pos);
                                pageStack[sp] = pg;
                                posStack[sp] = pos = pg.nItems;
                            } while (++sp < pageStack.length);

                            posStack[sp - 1] = --pos;
                            break;
                        }
                    }
                } else {
                    posStack[sp - 1] = pos;
                }
                if (sp != 0 && from != null && pg.compare(from, pos) >= from.inclusion) {
                    sp = 0;
                }
            }
            if (((StorageImpl) getStorage()).myConcurrentIterator && sp != 0) {
                nextKey = pg.getKey(pos);
                nextObj = pg.items.getRaw(pos);
            }
        }

        @Override
        public boolean hasNext() {
            if (counter != updateCounter) {
                if (((StorageImpl) getStorage()).myConcurrentIterator) {
                    refresh();
                } else {
                    throw new ConcurrentModificationException();
                }
            }

            return sp != 0;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final int pos = posStack[sp - 1];
            final BtreePage pg = pageStack[sp - 1];

            currPos = pos;
            currPage = pg;
            final E curr = (E) getCurrent(pg, pos);

            if (((StorageImpl) getStorage()).myConcurrentIterator) {
                currKey = new BtreeKey(pg.getKey(pos), pg.items.getRaw(pos));
            }

            gotoNextItem(pg, pos);

            return curr;
        }

        @Override
        public int nextOid() {
            if (!hasNext()) {
                return 0;
            }

            final int pos = posStack[sp - 1];
            final BtreePage pg = pageStack[sp - 1];

            currPos = pos;
            currPage = pg;

            final Object obj = pg.items.getRaw(pos);
            final int oid = getStorage().getOid(obj);

            if (((StorageImpl) getStorage()).myConcurrentIterator) {
                currKey = new BtreeKey(pg.getKey(pos), pg.items.getRaw(pos));
            }

            gotoNextItem(pg, pos);
            return oid;
        }

        private void refresh() {
            if (sp != 0) {
                if (nextKey == null) {
                    reset();
                } else {
                    if (order == ASCENT_ORDER) {
                        from = nextKey;
                    } else {
                        till = nextKey;
                    }

                    final Object next = nextObj;
                    reset();

                    while (true) {
                        final int pos = posStack[sp - 1];
                        final BtreePage pg = pageStack[sp - 1];

                        if (!pg.items.getRaw(pos).equals(next)) {
                            gotoNextItem(pg, pos);
                        } else {
                            break;
                        }
                    }
                }
            }

            counter = updateCounter;
        }

        @Override
        public void remove() {
            if (currPage == null) {
                throw new NoSuchElementException();
            }

            final StorageImpl db = (StorageImpl) getStorage();

            if (!db.myConcurrentIterator) {
                if (counter != updateCounter) {
                    throw new ConcurrentModificationException();
                }

                currKey = new BtreeKey(currPage.getKey(currPos), currPage.items.getRaw(currPos));

                if (sp != 0) {
                    final int pos = posStack[sp - 1];
                    final BtreePage pg = pageStack[sp - 1];

                    nextKey = pg.getKey(pos);
                    nextObj = pg.items.getRaw(pos);
                }
            }

            AltBtree.this.removeIfExists(currKey);

            refresh();
            currPage = null;
        }

        void reset() {
            int i, l, r;

            sp = 0;
            counter = updateCounter;
            if (height == 0) {
                return;
            }

            BtreePage page = root;
            int h = height;

            pageStack = new BtreePage[h];
            posStack = new int[h];

            if (order == ASCENT_ORDER) {
                if (from == null) {
                    while (--h > 0) {
                        posStack[sp] = 0;
                        pageStack[sp] = page;
                        page = (BtreePage) page.items.get(0);
                        sp += 1;
                    }

                    posStack[sp] = 0;
                    pageStack[sp] = page;
                    end = page.nItems;
                    sp += 1;
                } else {
                    while (--h > 0) {
                        pageStack[sp] = page;
                        l = 0;
                        r = page.nItems;

                        while (l < r) {
                            i = l + r >> 1;

                            if (page.compare(from, i) >= from.inclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        posStack[sp] = r;
                        page = (BtreePage) page.items.get(r);
                        sp += 1;
                    }

                    pageStack[sp] = page;
                    l = 0;
                    r = end = page.nItems;

                    while (l < r) {
                        i = l + r >> 1;

                        if (page.compare(from, i) >= from.inclusion) {
                            l = i + 1;
                        } else {
                            r = i;
                        }
                    }

                    Assert.that(r == l);

                    if (r == end) {
                        sp += 1;
                        gotoNextItem(page, r - 1);
                    } else {
                        posStack[sp++] = r;
                    }
                }

                if (sp != 0 && till != null) {
                    page = pageStack[sp - 1];

                    if (-page.compare(till, posStack[sp - 1]) >= till.inclusion) {
                        sp = 0;
                    }
                }
            } else { // descent order
                if (till == null) {
                    while (--h > 0) {
                        pageStack[sp] = page;
                        posStack[sp] = page.nItems;
                        page = (BtreePage) page.items.get(page.nItems);
                        sp += 1;
                    }

                    pageStack[sp] = page;
                    posStack[sp++] = page.nItems - 1;
                } else {
                    while (--h > 0) {
                        pageStack[sp] = page;
                        l = 0;
                        r = page.nItems;

                        while (l < r) {
                            i = l + r >> 1;

                            if (page.compare(till, i) >= 1 - till.inclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        posStack[sp] = r;
                        page = (BtreePage) page.items.get(r);
                        sp += 1;
                    }

                    pageStack[sp] = page;
                    l = 0;
                    r = page.nItems;

                    while (l < r) {
                        i = l + r >> 1;

                        if (page.compare(till, i) >= 1 - till.inclusion) {
                            l = i + 1;
                        } else {
                            r = i;
                        }
                    }

                    Assert.that(r == l);

                    if (r == 0) {
                        sp += 1;
                        gotoNextItem(page, r);
                    } else {
                        posStack[sp++] = r - 1;
                    }
                }
                if (sp != 0 && from != null) {
                    page = pageStack[sp - 1];

                    if (page.compare(from, posStack[sp - 1]) >= from.inclusion) {
                        sp = 0;
                    }
                }
            }
        }

    }

    static final int op_done = 0;

    static final int op_overflow = 1;

    static final int op_underflow = 2;

    static final int op_not_found = 3;

    static final int op_duplicate = 4;

    static final int op_overwrite = 5;

    static int checkType(final Class c) {
        final int elemType = ClassDescriptor.getTypeCode(c);

        if (elemType > ClassDescriptor.tpObject && elemType != ClassDescriptor.tpValue &&
                elemType != ClassDescriptor.tpEnum) {
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, c);
        }

        return elemType;
    }

    static Class mapKeyType(final int type) {
        switch (type) {
            case ClassDescriptor.tpBoolean:
                return boolean.class;
            case ClassDescriptor.tpByte:
                return byte.class;
            case ClassDescriptor.tpChar:
                return char.class;
            case ClassDescriptor.tpShort:
                return short.class;
            case ClassDescriptor.tpInt:
                return int.class;
            case ClassDescriptor.tpLong:
                return long.class;
            case ClassDescriptor.tpFloat:
                return float.class;
            case ClassDescriptor.tpDouble:
                return double.class;
            case ClassDescriptor.tpString:
                return String.class;
            case ClassDescriptor.tpDate:
                return Date.class;
            case ClassDescriptor.tpObject:
                return Object.class;
            case ClassDescriptor.tpValue:
                return IValue.class;
            case ClassDescriptor.tpEnum:
                return Enum.class;
            default:
                return null;
        }
    }

    transient int updateCounter;

    int height;

    int type;

    int nElems;

    boolean unique;

    BtreePage root;

    AltBtree() {
    }

    AltBtree(final Class cls, final boolean unique) {
        this.unique = unique;
        type = checkType(cls);
    }

    AltBtree(final int type, final boolean unique) {
        this.type = type;
        this.unique = unique;
    }

    final void allocateRootPage(final BtreeKey ins) {
        final Storage s = getStorage();
        BtreePage newRoot = null;

        switch (type) {
            case ClassDescriptor.tpByte:
                newRoot = new BtreePageOfByte(s);
                break;
            case ClassDescriptor.tpShort:
                newRoot = new BtreePageOfShort(s);
                break;
            case ClassDescriptor.tpChar:
                newRoot = new BtreePageOfChar(s);
                break;
            case ClassDescriptor.tpBoolean:
                newRoot = new BtreePageOfBoolean(s);
                break;
            case ClassDescriptor.tpInt:
            case ClassDescriptor.tpEnum:
                newRoot = new BtreePageOfInt(s);
                break;
            case ClassDescriptor.tpLong:
            case ClassDescriptor.tpDate:
                newRoot = new BtreePageOfLong(s);
                break;
            case ClassDescriptor.tpFloat:
                newRoot = new BtreePageOfFloat(s);
                break;
            case ClassDescriptor.tpDouble:
                newRoot = new BtreePageOfDouble(s);
                break;
            case ClassDescriptor.tpObject:
                newRoot = new BtreePageOfObject(s);
                break;
            case ClassDescriptor.tpString:
                newRoot = new BtreePageOfString(s);
                break;
            case ClassDescriptor.tpValue:
                newRoot = new BtreePageOfValue(s);
                break;
            default:
                Assert.failed("Invalid type");
        }

        newRoot.insert(ins, 0);
        newRoot.items.setObject(1, root);
        newRoot.nItems = 1;
        root = newRoot;
    }

    Key checkKey(Key key) {
        if (key != null) {
            if (key.type != type) {
                throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
            }

            if (type == ClassDescriptor.tpObject && key.ival == 0 && key.oval != null) {
                final Object obj = key.oval;
                key = new Key(obj, getStorage().makePersistent(obj), key.inclusion != 0);
            }

            if (key.oval instanceof char[]) {
                key = new Key(new String((char[]) key.oval), key.inclusion != 0);
            }
        }

        return key;
    }

    @Override
    public void clear() {
        if (root != null) {
            root.purge(height);
            root = null;
            nElems = 0;
            height = 0;
            updateCounter += 1;
            modify();
        }
    }

    @Override
    public void deallocate() {
        if (root != null) {
            root.purge(height);
        }

        super.deallocate();
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator() {
        return entryIterator(null, null, ASCENT_ORDER);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final int start, final int order) {
        return new BtreeEntryStartFromIterator(start, order);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Key from, final Key till, final int order) {
        return new BtreeSelectionEntryIterator(checkKey(from), checkKey(till), order);
    }

    @Override
    public IterableIterator<Map.Entry<Object, T>> entryIterator(final Object from, final Object till,
            final int order) {
        return new BtreeSelectionEntryIterator(checkKey(Btree.getKeyFromObject(type, from)), checkKey(Btree
                .getKeyFromObject(type, till)), order);
    }

    @Override
    public T get(Key key) {
        key = checkKey(key);

        if (root != null) {
            final ArrayList<T> list = new ArrayList<T>();

            root.find(key, key, height, list);

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
    public Object[] get(final Key from, final Key till) {
        final ArrayList<T> list = getList(from, till);
        return list.toArray();
    }

    @Override
    public T get(final Object key) {
        return get(Btree.getKeyFromObject(type, key));
    }

    @Override
    public Object[] get(final Object from, final Object till) {
        return get(Btree.getKeyFromObject(type, from), Btree.getKeyFromObject(type, till));
    }

    @Override
    public T getAt(int i) {
        IterableIterator<Map.Entry<Object, T>> iterator;

        if (i < 0 || i >= nElems) {
            throw new IndexOutOfBoundsException("Position " + i + ", index size " + nElems);
        }

        if (i <= nElems / 2) {
            iterator = entryIterator(null, null, ASCENT_ORDER);

            while (--i >= 0) {
                iterator.next();
            }
        } else {
            iterator = entryIterator(null, null, DESCENT_ORDER);
            i -= nElems;

            while (++i < 0) {
                iterator.next();
            }
        }

        return iterator.next().getValue();
    }

    @Override
    public Class getKeyType() {
        return mapKeyType(type);
    }

    @Override
    public Class[] getKeyTypes() {
        return new Class[] { getKeyType() };
    }

    @Override
    public ArrayList<T> getList(final Key from, final Key till) {
        final ArrayList<T> list = new ArrayList<T>();

        if (root != null) {
            root.find(checkKey(from), checkKey(till), height, list);
        }

        return list;
    }

    @Override
    public ArrayList<T> getList(final Object from, final Object till) {
        return getList(Btree.getKeyFromObject(type, from), Btree.getKeyFromObject(type, till));
    }

    @Override
    public Object[] getPrefix(final String prefix) {
        return get(new Key(prefix, true), new Key(prefix + Character.MAX_VALUE, false));
    }

    @Override
    public ArrayList<T> getPrefixList(final String prefix) {
        return getList(new Key(prefix, true), new Key(prefix + Character.MAX_VALUE, false));
    }

    @Override
    public int indexOf(final Key key) {
        final PersistentIterator iterator = (PersistentIterator) iterator(null, key, DESCENT_ORDER);
        int i;

        for (i = -1; iterator.nextOid() != 0; i++) {
            ;
        }

        return i;
    }

    final T insert(final Key key, final T obj, final boolean overwrite) {
        final BtreeKey ins = new BtreeKey(checkKey(key), obj);

        if (root == null) {
            allocateRootPage(ins);
            height = 1;
        } else {
            final int result = root.insert(ins, height, unique, overwrite);

            if (result == op_overflow) {
                allocateRootPage(ins);
                height += 1;
            } else if (result == op_duplicate || result == op_overwrite) {
                return (T) ins.oldNode;
            }
        }

        updateCounter += 1;
        nElems += 1;
        modify();

        return null;
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(null, null, ASCENT_ORDER);
    }

    @Override
    public IterableIterator<T> iterator(final Key from, final Key till, final int order) {
        return new BtreeSelectionIterator<T>(checkKey(from), checkKey(till), order);
    }

    @Override
    public IterableIterator<T> iterator(final Object from, final Object till, final int order) {
        return new BtreeSelectionIterator<T>(checkKey(Btree.getKeyFromObject(type, from)), checkKey(Btree
                .getKeyFromObject(type, till)), order);
    }

    @Override
    public IterableIterator<T> prefixIterator(final String prefix) {
        return prefixIterator(prefix, ASCENT_ORDER);
    }

    @Override
    public IterableIterator<T> prefixIterator(final String prefix, final int order) {
        return iterator(new Key(prefix), new Key(prefix + Character.MAX_VALUE, false), order);
    }

    @Override
    public Object[] prefixSearch(final String key) {
        final ArrayList<T> list = prefixSearchList(key);
        return list.toArray();
    }

    @Override
    public ArrayList<T> prefixSearchList(final String key) {
        if (ClassDescriptor.tpString != type) {
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }

        final ArrayList<T> list = new ArrayList<T>();

        if (root != null) {
            ((BtreePageOfString) root).prefixSearch(key, height, list);
        }

        return list;
    }

    @Override
    public boolean put(final Key key, final T obj) {
        return insert(key, obj, false) == null;
    }

    @Override
    public boolean put(final Object key, final T obj) {
        return put(Btree.getKeyFromObject(type, key), obj);
    }

    void remove(final BtreeKey rem) {
        if (!removeIfExists(rem)) {
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
    }

    @Override
    public T remove(final Key key) {
        if (!unique) {
            throw new StorageError(StorageError.KEY_NOT_UNIQUE);
        }

        final BtreeKey rk = new BtreeKey(checkKey(key), null);
        remove(rk);

        return (T) rk.oldNode;
    }

    @Override
    public void remove(final Key key, final T obj) {
        remove(new BtreeKey(checkKey(key), obj));
    }

    @Override
    public void remove(final Object key, final T obj) {
        remove(Btree.getKeyFromObject(type, key), obj);
    }

    @Override
    public T remove(final String key) {
        return remove(new Key(key));
    }

    boolean removeIfExists(final BtreeKey rem) {
        if (root == null) {
            return false;
        }

        final int result = root.remove(rem, height);

        if (result == op_not_found) {
            return false;
        }

        nElems -= 1;

        if (result == op_underflow) {
            if (root.nItems == 0) {
                BtreePage newRoot = null;

                if (height != 1) {
                    newRoot = (BtreePage) root.items.get(0);
                }

                root.deallocate();
                root = newRoot;
                height -= 1;
            }
        }

        updateCounter += 1;
        modify();

        return true;
    }

    boolean removeIfExists(final Key key, final Object obj) {
        return removeIfExists(new BtreeKey(checkKey(key), obj));
    }

    @Override
    public T removeKey(final Object key) {
        return removeKey(Btree.getKeyFromObject(type, key));
    }

    @Override
    public T set(final Key key, final T obj) {
        return insert(key, obj, true);
    }

    @Override
    public T set(final Object key, final T obj) {
        return set(Btree.getKeyFromObject(type, key), obj);
    }

    @Override
    public int size() {
        return nElems;
    }

    @Override
    public Object[] toArray() {
        final Object[] arr = new Object[nElems];

        if (root != null) {
            root.traverseForward(height, arr, 0);
        }

        return arr;
    }

    @Override
    public <E> E[] toArray(E[] arr) {
        if (arr.length < nElems) {
            arr = (E[]) Array.newInstance(arr.getClass().getComponentType(), nElems);
        }

        if (root != null) {
            root.traverseForward(height, arr, 0);
        }

        if (arr.length > nElems) {
            arr[nElems] = null;
        }

        return arr;
    }

    @Override
    public boolean unlink(final Key key, final T obj) {
        return removeIfExists(key, obj);
    }
}
