
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
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.StorageError;

class Btree<T> extends PersistentCollection<T> implements Index<T> {

    static class BtreeEntry<T> implements Map.Entry<Object, T> {

        private final Object key;

        private final StorageImpl db;

        private final int oid;

        BtreeEntry(final StorageImpl db, final Object key, final int oid) {
            this.db = db;
            this.key = key;
            this.oid = oid;
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
            return key;
        }

        @Override
        public T getValue() {
            return (T) db.lookupObject(oid, null);
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

    class BtreeSelectionEntryIterator extends BtreeSelectionIterator<Map.Entry<Object, T>> {

        BtreeSelectionEntryIterator(final Key from, final Key till, final int order) {
            super(from, till, order);
        }

        @Override
        protected Object getCurrent(final Page pg, final int pos) {
            final StorageImpl db = (StorageImpl) getStorage();

            switch (type) {
                case ClassDescriptor.tpString:
                    return new BtreeEntry<T>(db, unpackStrKey(pg, pos), BtreePage.getKeyStrOid(pg, pos));
                case ClassDescriptor.tpArrayOfByte:
                    return new BtreeEntry<T>(db, unpackByteArrayKey(pg, pos), BtreePage.getKeyStrOid(pg, pos));
                default:
                    return new BtreeEntry<T>(db, unpackKey(db, pg, pos), BtreePage.getReference(pg,
                            BtreePage.maxItems - 1 - pos));
            }
        }
    }

    class BtreeSelectionIterator<E> extends IterableIterator<E> implements PersistentIterator {

        int[] pageStack;

        int[] posStack;

        int currPage;

        int currPos;

        int sp;

        int end;

        Key from;

        Key till;

        int order;

        int counter;

        BtreeKey nextKey;

        BtreeKey currKey;

        BtreeSelectionIterator(final Key from, final Key till, final int order) {
            this.from = from;
            this.till = till;
            this.order = order;
            reset();
        }

        protected Object getCurrent(final Page pg, final int pos) {
            final StorageImpl db = (StorageImpl) getStorage();
            return db.lookupObject(getReference(pg, pos), null);
        }

        BtreeKey getCurrentKey(final Page pg, final int pos) {
            BtreeKey key;

            switch (type) {
                case ClassDescriptor.tpString:
                    key = new BtreeKey(null, BtreePage.getKeyStrOid(pg, pos));
                    key.getStr(pg, pos);
                    break;
                case ClassDescriptor.tpArrayOfByte:
                    key = new BtreeKey(null, BtreePage.getKeyStrOid(pg, pos));
                    key.getByteArray(pg, pos);
                    break;
                default:
                    key = new BtreeKey(null, BtreePage.getReference(pg, BtreePage.maxItems - 1 - pos));
                    key.extract(pg, BtreePage.firstKeyOffs + pos * ClassDescriptor.sizeof[type], type);
            }

            return key;
        }

        private int getReference(final Page pg, final int pos) {
            return type == ClassDescriptor.tpString || type == ClassDescriptor.tpArrayOfByte ? BtreePage.getKeyStrOid(
                    pg, pos) : BtreePage.getReference(pg, BtreePage.maxItems - 1 - pos);
        }

        protected final void gotoNextItem(Page pg, int pos) {
            final StorageImpl db = (StorageImpl) getStorage();

            if (type == ClassDescriptor.tpString) {
                if (order == ASCENT_ORDER) {
                    if (++pos == end) {
                        while (--sp != 0) {
                            db.myPagePool.unfix(pg);
                            pos = posStack[sp - 1];
                            pg = db.getPage(pageStack[sp - 1]);

                            if (++pos <= BtreePage.getnItems(pg)) {
                                posStack[sp - 1] = pos;
                                do {
                                    final int pageId = BtreePage.getKeyStrOid(pg, pos);

                                    db.myPagePool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    end = BtreePage.getnItems(pg);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = 0;
                                } while (++sp < pageStack.length);

                                break;
                            }
                        }
                    } else {
                        posStack[sp - 1] = pos;
                    }

                    if (sp != 0 && till != null && -BtreePage.compareStr(till, pg, pos) >= till.inclusion) {
                        sp = 0;
                    }
                } else { // descent order
                    if (--pos < 0) {
                        while (--sp != 0) {
                            db.myPagePool.unfix(pg);
                            pos = posStack[sp - 1];
                            pg = db.getPage(pageStack[sp - 1]);

                            if (--pos >= 0) {
                                posStack[sp - 1] = pos;

                                do {
                                    final int pageId = BtreePage.getKeyStrOid(pg, pos);

                                    db.myPagePool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = BtreePage.getnItems(pg);
                                } while (++sp < pageStack.length);

                                posStack[sp - 1] = --pos;
                                break;
                            }
                        }
                    } else {
                        posStack[sp - 1] = pos;
                    }

                    if (sp != 0 && from != null && BtreePage.compareStr(from, pg, pos) >= from.inclusion) {
                        sp = 0;
                    }
                }
            } else if (type == ClassDescriptor.tpArrayOfByte) {
                if (order == ASCENT_ORDER) {
                    if (++pos == end) {
                        while (--sp != 0) {
                            db.myPagePool.unfix(pg);
                            pos = posStack[sp - 1];
                            pg = db.getPage(pageStack[sp - 1]);

                            if (++pos <= BtreePage.getnItems(pg)) {
                                posStack[sp - 1] = pos;

                                do {
                                    final int pageId = BtreePage.getKeyStrOid(pg, pos);

                                    db.myPagePool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    end = BtreePage.getnItems(pg);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = 0;
                                } while (++sp < pageStack.length);

                                break;
                            }
                        }
                    } else {
                        posStack[sp - 1] = pos;
                    }

                    if (sp != 0 && till != null && -compareByteArrays(till, pg, pos) >= till.inclusion) {
                        sp = 0;
                    }
                } else { // descent order
                    if (--pos < 0) {
                        while (--sp != 0) {
                            db.myPagePool.unfix(pg);
                            pos = posStack[sp - 1];
                            pg = db.getPage(pageStack[sp - 1]);

                            if (--pos >= 0) {
                                posStack[sp - 1] = pos;

                                do {
                                    final int pageId = BtreePage.getKeyStrOid(pg, pos);

                                    db.myPagePool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = BtreePage.getnItems(pg);
                                } while (++sp < pageStack.length);

                                posStack[sp - 1] = --pos;
                                break;
                            }
                        }
                    } else {
                        posStack[sp - 1] = pos;
                    }
                    if (sp != 0 && from != null && compareByteArrays(from, pg, pos) >= from.inclusion) {
                        sp = 0;
                    }
                }
            } else { // scalar type
                if (order == ASCENT_ORDER) {
                    if (++pos == end) {
                        while (--sp != 0) {
                            db.myPagePool.unfix(pg);
                            pos = posStack[sp - 1];
                            pg = db.getPage(pageStack[sp - 1]);

                            if (++pos <= BtreePage.getnItems(pg)) {
                                posStack[sp - 1] = pos;

                                do {
                                    final int pageId = BtreePage.getReference(pg, BtreePage.maxItems - 1 - pos);

                                    db.myPagePool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    end = BtreePage.getnItems(pg);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = 0;
                                } while (++sp < pageStack.length);

                                break;
                            }
                        }
                    } else {
                        posStack[sp - 1] = pos;
                    }
                    if (sp != 0 && till != null && -BtreePage.compare(till, pg, pos) >= till.inclusion) {
                        sp = 0;
                    }
                } else { // descent order
                    if (--pos < 0) {
                        while (--sp != 0) {
                            db.myPagePool.unfix(pg);
                            pos = posStack[sp - 1];
                            pg = db.getPage(pageStack[sp - 1]);

                            if (--pos >= 0) {
                                posStack[sp - 1] = pos;

                                do {
                                    final int pageId = BtreePage.getReference(pg, BtreePage.maxItems - 1 - pos);

                                    db.myPagePool.unfix(pg);
                                    pg = db.getPage(pageId);
                                    pageStack[sp] = pageId;
                                    posStack[sp] = pos = BtreePage.getnItems(pg);
                                } while (++sp < pageStack.length);

                                posStack[sp - 1] = --pos;
                                break;
                            }
                        }
                    } else {
                        posStack[sp - 1] = pos;
                    }

                    if (sp != 0 && from != null && BtreePage.compare(from, pg, pos) >= from.inclusion) {
                        sp = 0;
                    }
                }
            }

            if (db.myConcurrentIterator && sp != 0) {
                nextKey = getCurrentKey(pg, pos);
            }

            db.myPagePool.unfix(pg);
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

            final StorageImpl db = (StorageImpl) getStorage();
            final int pos = posStack[sp - 1];

            currPos = pos;
            currPage = pageStack[sp - 1];

            final Page pg = db.getPage(currPage);
            final E curr = (E) getCurrent(pg, pos);

            if (db.myConcurrentIterator) {
                currKey = getCurrentKey(pg, pos);
            }

            gotoNextItem(pg, pos);

            return curr;
        }

        @Override
        public int nextOid() {
            if (!hasNext()) {
                return 0;
            }

            final StorageImpl db = (StorageImpl) getStorage();
            final int pos = posStack[sp - 1];

            currPos = pos;
            currPage = pageStack[sp - 1];

            final Page pg = db.getPage(currPage);
            final int oid = getReference(pg, pos);

            if (db.myConcurrentIterator) {
                currKey = getCurrentKey(pg, pos);
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
                        from = nextKey.key;
                    } else {
                        till = nextKey.key;
                    }

                    final int next = nextKey.oid;

                    reset();

                    final StorageImpl db = (StorageImpl) getStorage();

                    while (true) {
                        final int pos = posStack[sp - 1];
                        final Page pg = db.getPage(pageStack[sp - 1]);
                        final int oid = type == ClassDescriptor.tpString || type == ClassDescriptor.tpArrayOfByte
                                ? BtreePage.getKeyStrOid(pg, pos) : BtreePage.getReference(pg, BtreePage.maxItems -
                                        1 - pos);

                        if (oid != next) {
                            gotoNextItem(pg, pos);
                        } else {
                            db.myPagePool.unfix(pg);
                            break;
                        }
                    }
                }
            }

            counter = updateCounter;
        }

        @Override
        public void remove() {
            if (currPage == 0) {
                throw new NoSuchElementException();
            }

            final StorageImpl db = (StorageImpl) getStorage();

            if (!db.myConcurrentIterator) {
                if (counter != updateCounter) {
                    throw new ConcurrentModificationException();
                }

                Page pg = db.getPage(currPage);

                currKey = getCurrentKey(pg, currPos);
                db.myPagePool.unfix(pg);

                if (sp != 0) {
                    final int pos = posStack[sp - 1];

                    pg = db.getPage(pageStack[sp - 1]);
                    nextKey = getCurrentKey(pg, pos);
                    db.myPagePool.unfix(pg);
                }
            }

            Btree.this.removeIfExists(currKey);

            refresh();
            currPage = 0;
        }

        void reset() {
            int i, l, r;

            sp = 0;
            counter = updateCounter;

            if (height == 0) {
                return;
            }

            int pageId = root;
            final StorageImpl db = (StorageImpl) getStorage();

            if (db == null) {
                throw new StorageError(StorageError.DELETED_OBJECT);
            }

            int h = height;

            pageStack = new int[h];
            posStack = new int[h];

            if (type == ClassDescriptor.tpString) {
                if (order == ASCENT_ORDER) {
                    if (from == null) {
                        while (--h >= 0) {
                            posStack[sp] = 0;
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            pageId = BtreePage.getKeyStrOid(pg, 0);
                            end = BtreePage.getnItems(pg);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }
                    } else {
                        while (--h > 0) {
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);

                            while (l < r) {
                                i = l + r >> 1;

                                if (BtreePage.compareStr(from, pg, i) >= from.inclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            posStack[sp] = r;
                            pageId = BtreePage.getKeyStrOid(pg, r);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }

                        pageStack[sp] = pageId;

                        final Page pg = db.getPage(pageId);
                        l = 0;
                        end = r = BtreePage.getnItems(pg);

                        while (l < r) {
                            i = l + r >> 1;

                            if (BtreePage.compareStr(from, pg, i) >= from.inclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == end) {
                            sp += 1;
                            gotoNextItem(pg, r - 1);
                        } else {
                            posStack[sp++] = r;
                            db.myPagePool.unfix(pg);
                        }
                    }

                    if (sp != 0 && till != null) {
                        final Page pg = db.getPage(pageStack[sp - 1]);

                        if (-BtreePage.compareStr(till, pg, posStack[sp - 1]) >= till.inclusion) {
                            sp = 0;
                        }

                        db.myPagePool.unfix(pg);
                    }
                } else { // descent order
                    if (till == null) {
                        while (--h > 0) {
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            posStack[sp] = BtreePage.getnItems(pg);
                            pageId = BtreePage.getKeyStrOid(pg, posStack[sp]);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }

                        pageStack[sp] = pageId;
                        final Page pg = db.getPage(pageId);
                        posStack[sp++] = BtreePage.getnItems(pg) - 1;
                        db.myPagePool.unfix(pg);
                    } else {
                        while (--h > 0) {
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);

                            while (l < r) {
                                i = l + r >> 1;

                                if (BtreePage.compareStr(till, pg, i) >= 1 - till.inclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            posStack[sp] = r;
                            pageId = BtreePage.getKeyStrOid(pg, r);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }

                        pageStack[sp] = pageId;

                        final Page pg = db.getPage(pageId);
                        l = 0;
                        r = BtreePage.getnItems(pg);

                        while (l < r) {
                            i = l + r >> 1;

                            if (BtreePage.compareStr(till, pg, i) >= 1 - till.inclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == 0) {
                            sp += 1;
                            gotoNextItem(pg, r);
                        } else {
                            posStack[sp++] = r - 1;
                            db.myPagePool.unfix(pg);
                        }
                    }

                    if (sp != 0 && from != null) {
                        final Page pg = db.getPage(pageStack[sp - 1]);

                        if (BtreePage.compareStr(from, pg, posStack[sp - 1]) >= from.inclusion) {
                            sp = 0;
                        }

                        db.myPagePool.unfix(pg);
                    }
                }
            } else if (type == ClassDescriptor.tpArrayOfByte) {
                if (order == ASCENT_ORDER) {
                    if (from == null) {
                        while (--h >= 0) {
                            posStack[sp] = 0;
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            pageId = BtreePage.getKeyStrOid(pg, 0);
                            end = BtreePage.getnItems(pg);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }
                    } else {
                        while (--h > 0) {
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);

                            while (l < r) {
                                i = l + r >> 1;

                                if (compareByteArrays(from, pg, i) >= from.inclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            posStack[sp] = r;
                            pageId = BtreePage.getKeyStrOid(pg, r);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }

                        pageStack[sp] = pageId;

                        final Page pg = db.getPage(pageId);
                        l = 0;
                        end = r = BtreePage.getnItems(pg);

                        while (l < r) {
                            i = l + r >> 1;

                            if (compareByteArrays(from, pg, i) >= from.inclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == end) {
                            sp += 1;
                            gotoNextItem(pg, r - 1);
                        } else {
                            posStack[sp++] = r;
                            db.myPagePool.unfix(pg);
                        }
                    }

                    if (sp != 0 && till != null) {
                        final Page pg = db.getPage(pageStack[sp - 1]);

                        if (-compareByteArrays(till, pg, posStack[sp - 1]) >= till.inclusion) {
                            sp = 0;
                        }

                        db.myPagePool.unfix(pg);
                    }
                } else { // descent order
                    if (till == null) {
                        while (--h > 0) {
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            posStack[sp] = BtreePage.getnItems(pg);
                            pageId = BtreePage.getKeyStrOid(pg, posStack[sp]);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }

                        pageStack[sp] = pageId;

                        final Page pg = db.getPage(pageId);
                        posStack[sp++] = BtreePage.getnItems(pg) - 1;
                        db.myPagePool.unfix(pg);
                    } else {
                        while (--h > 0) {
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);

                            while (l < r) {
                                i = l + r >> 1;

                                if (compareByteArrays(till, pg, i) >= 1 - till.inclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            posStack[sp] = r;
                            pageId = BtreePage.getKeyStrOid(pg, r);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }

                        pageStack[sp] = pageId;

                        final Page pg = db.getPage(pageId);
                        l = 0;
                        r = BtreePage.getnItems(pg);

                        while (l < r) {
                            i = l + r >> 1;

                            if (compareByteArrays(till, pg, i) >= 1 - till.inclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == 0) {
                            sp += 1;
                            gotoNextItem(pg, r);
                        } else {
                            posStack[sp++] = r - 1;
                            db.myPagePool.unfix(pg);
                        }
                    }

                    if (sp != 0 && from != null) {
                        final Page pg = db.getPage(pageStack[sp - 1]);

                        if (compareByteArrays(from, pg, posStack[sp - 1]) >= from.inclusion) {
                            sp = 0;
                        }

                        db.myPagePool.unfix(pg);
                    }
                }
            } else { // scalar type
                if (order == ASCENT_ORDER) {
                    if (from == null) {
                        while (--h >= 0) {
                            posStack[sp] = 0;
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            pageId = BtreePage.getReference(pg, BtreePage.maxItems - 1);
                            end = BtreePage.getnItems(pg);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }
                    } else {
                        while (--h > 0) {
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);

                            while (l < r) {
                                i = l + r >> 1;

                                if (BtreePage.compare(from, pg, i) >= from.inclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            posStack[sp] = r;
                            pageId = BtreePage.getReference(pg, BtreePage.maxItems - 1 - r);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }

                        pageStack[sp] = pageId;

                        final Page pg = db.getPage(pageId);
                        l = 0;
                        r = end = BtreePage.getnItems(pg);

                        while (l < r) {
                            i = l + r >> 1;

                            if (BtreePage.compare(from, pg, i) >= from.inclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == end) {
                            sp += 1;
                            gotoNextItem(pg, r - 1);
                        } else {
                            posStack[sp++] = r;
                            db.myPagePool.unfix(pg);
                        }
                    }

                    if (sp != 0 && till != null) {
                        final Page pg = db.getPage(pageStack[sp - 1]);

                        if (-BtreePage.compare(till, pg, posStack[sp - 1]) >= till.inclusion) {
                            sp = 0;
                        }

                        db.myPagePool.unfix(pg);
                    }
                } else { // descent order
                    if (till == null) {
                        while (--h > 0) {
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            posStack[sp] = BtreePage.getnItems(pg);
                            pageId = BtreePage.getReference(pg, BtreePage.maxItems - 1 - posStack[sp]);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }

                        pageStack[sp] = pageId;

                        final Page pg = db.getPage(pageId);
                        posStack[sp++] = BtreePage.getnItems(pg) - 1;
                        db.myPagePool.unfix(pg);
                    } else {
                        while (--h > 0) {
                            pageStack[sp] = pageId;

                            final Page pg = db.getPage(pageId);
                            l = 0;
                            r = BtreePage.getnItems(pg);

                            while (l < r) {
                                i = l + r >> 1;

                                if (BtreePage.compare(till, pg, i) >= 1 - till.inclusion) {
                                    l = i + 1;
                                } else {
                                    r = i;
                                }
                            }

                            Assert.that(r == l);

                            posStack[sp] = r;
                            pageId = BtreePage.getReference(pg, BtreePage.maxItems - 1 - r);
                            db.myPagePool.unfix(pg);
                            sp += 1;
                        }

                        pageStack[sp] = pageId;

                        final Page pg = db.getPage(pageId);
                        l = 0;
                        r = BtreePage.getnItems(pg);

                        while (l < r) {
                            i = l + r >> 1;

                            if (BtreePage.compare(till, pg, i) >= 1 - till.inclusion) {
                                l = i + 1;
                            } else {
                                r = i;
                            }
                        }

                        Assert.that(r == l);

                        if (r == 0) {
                            sp += 1;
                            gotoNextItem(pg, r);
                        } else {
                            posStack[sp++] = r - 1;
                            db.myPagePool.unfix(pg);
                        }
                    }

                    if (sp != 0 && from != null) {
                        final Page pg = db.getPage(pageStack[sp - 1]);

                        if (BtreePage.compare(from, pg, posStack[sp - 1]) >= from.inclusion) {
                            sp = 0;
                        }

                        db.myPagePool.unfix(pg);
                    }
                }
            }
        }

    }

    static final int sizeof = ObjectHeader.sizeof + 4 * 4 + 1;

    static final int op_done = 0;

    static final int op_overflow = 1;

    static final int op_underflow = 2;

    static final int op_not_found = 3;

    static final int op_duplicate = 4;

    static final int op_overwrite = 5;

    static int checkType(final Class c) {
        final int elemType = ClassDescriptor.getTypeCode(c);

        if (elemType > ClassDescriptor.tpObject && elemType != ClassDescriptor.tpEnum &&
                elemType != ClassDescriptor.tpArrayOfByte) {
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, c);
        }

        return elemType;
    }

    static Key getKeyFromObject(final int type, final Object o) {
        if (o == null) {
            return null;
        }

        switch (type) {
            case ClassDescriptor.tpBoolean:
                return new Key(((Boolean) o).booleanValue());
            case ClassDescriptor.tpByte:
                return new Key(((Number) o).byteValue());
            case ClassDescriptor.tpChar:
                return new Key(((Character) o).charValue());
            case ClassDescriptor.tpShort:
                return new Key(((Number) o).shortValue());
            case ClassDescriptor.tpInt:
                return new Key(((Number) o).intValue());
            case ClassDescriptor.tpLong:
                return new Key(((Number) o).longValue());
            case ClassDescriptor.tpFloat:
                return new Key(((Number) o).floatValue());
            case ClassDescriptor.tpDouble:
                return new Key(((Number) o).doubleValue());
            case ClassDescriptor.tpString:
                return new Key((String) o);
            case ClassDescriptor.tpDate:
                return new Key((java.util.Date) o);
            case ClassDescriptor.tpObject:
                return new Key(o);
            case ClassDescriptor.tpValue:
                return new Key((IValue) o);
            case ClassDescriptor.tpEnum:
                return new Key((Enum) o);
            case ClassDescriptor.tpArrayOfByte:
                return new Key((byte[]) o);
            default:
                throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
        }
    }

    static Key getKeyFromObject(final Object o) {
        if (o == null) {
            return null;
        } else if (o instanceof Byte) {
            return new Key(((Byte) o).byteValue());
        } else if (o instanceof Short) {
            return new Key(((Short) o).shortValue());
        } else if (o instanceof Integer) {
            return new Key(((Integer) o).intValue());
        } else if (o instanceof Long) {
            return new Key(((Long) o).longValue());
        } else if (o instanceof Float) {
            return new Key(((Float) o).floatValue());
        } else if (o instanceof Double) {
            return new Key(((Double) o).doubleValue());
        } else if (o instanceof Boolean) {
            return new Key(((Boolean) o).booleanValue());
        } else if (o instanceof Character) {
            return new Key(((Character) o).charValue());
        } else if (o instanceof String) {
            return new Key((String) o);
        } else if (o instanceof java.util.Date) {
            return new Key((java.util.Date) o);
        } else if (o instanceof byte[]) {
            return new Key((byte[]) o);
        } else if (o instanceof Object[]) {
            return new Key((Object[]) o);
        } else if (o instanceof Enum) {
            return new Key((Enum) o);
        } else if (o instanceof IValue) {
            return new Key((IValue) o);
        } else {
            return new Key(o);
        }
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
            case ClassDescriptor.tpEnum:
                return Enum.class;
            case ClassDescriptor.tpString:
                return String.class;
            case ClassDescriptor.tpDate:
                return Date.class;
            case ClassDescriptor.tpObject:
                return Object.class;
            case ClassDescriptor.tpArrayOfByte:
                return byte[].class;
            default:
                return Comparable.class;
        }
    }

    static String unpackStrKey(final Page pg, final int pos) {
        final int len = BtreePage.getKeyStrSize(pg, pos);
        int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, pos);
        final byte[] data = pg.data;
        final char[] sval = new char[len];

        for (int j = 0; j < len; j++) {
            sval[j] = (char) Bytes.unpack2(data, offs);
            offs += 2;
        }

        return new String(sval);
    }

    transient int updateCounter;

    int root;

    int height;

    int type;

    int nElems;

    boolean unique;

    Btree() {
    }

    Btree(final byte[] obj, int offs) {
        height = Bytes.unpack4(obj, offs);
        offs += 4;
        nElems = Bytes.unpack4(obj, offs);
        offs += 4;
        root = Bytes.unpack4(obj, offs);
        offs += 4;
        type = Bytes.unpack4(obj, offs);
        offs += 4;
        unique = obj[offs] != 0;
    }

    Btree(final Class cls, final boolean unique) {
        this.unique = unique;
        type = checkType(cls);
    }

    Btree(final int type, final boolean unique) {
        this.type = type;
        this.unique = unique;
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

            if (key.oval instanceof String) {
                key = new Key(((String) key.oval).toCharArray(), key.inclusion != 0);
            }
        }

        return key;
    }

    @Override
    public void clear() {
        if (root != 0) {
            BtreePage.purge((StorageImpl) getStorage(), root, type, height);

            root = 0;
            nElems = 0;
            height = 0;
            updateCounter += 1;
            modify();
        }
    }

    int compareByteArrays(final byte[] key, final byte[] item, final int offs, final int length) {
        final int n = key.length >= length ? length : key.length;

        for (int i = 0; i < n; i++) {
            final int diff = key[i] - item[i + offs];

            if (diff != 0) {
                return diff;
            }
        }

        return key.length - length;
    }

    final int compareByteArrays(final Key key, final Page pg, final int i) {
        return compareByteArrays((byte[]) key.oval, pg.data, BtreePage.getKeyStrOffs(pg, i) + BtreePage.firstKeyOffs,
                BtreePage.getKeyStrSize(pg, i));
    }

    @Override
    public void deallocate() {
        if (root != 0) {
            BtreePage.purge((StorageImpl) getStorage(), root, type, height);
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
        return new BtreeSelectionEntryIterator(checkKey(getKeyFromObject(type, from)), checkKey(getKeyFromObject(type,
                till)), order);
    }

    public void export(final XMLExporter exporter) throws java.io.IOException {
        if (root != 0) {
            BtreePage.exportPage((StorageImpl) getStorage(), exporter, root, type, height);
        }
    }

    @Override
    public T get(Key key) {
        key = checkKey(key);

        if (root != 0) {
            final ArrayList list = new ArrayList();
            BtreePage.find((StorageImpl) getStorage(), root, key, key, this, height, list);

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
    public Object[] get(final Key from, final Key till) {
        final ArrayList<T> list = getList(from, till);
        return list.toArray();
    }

    @Override
    public T get(final Object key) {
        return get(getKeyFromObject(type, key));
    }

    @Override
    public Object[] get(final Object from, final Object till) {
        return get(getKeyFromObject(type, from), getKeyFromObject(type, till));
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

        if (root != 0) {
            BtreePage.find((StorageImpl) getStorage(), root, checkKey(from), checkKey(till), this, height, list);
        }

        return list;
    }

    @Override
    public ArrayList<T> getList(final Object from, final Object till) {
        return getList(getKeyFromObject(type, from), getKeyFromObject(type, till));
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

    final int insert(Key key, final T obj, final boolean overwrite) {
        final StorageImpl db = (StorageImpl) getStorage();

        if (db == null) {
            throw new StorageError(StorageError.DELETED_OBJECT);
        }

        key = checkKey(key);

        final BtreeKey ins = new BtreeKey(key, db.makePersistent(obj));

        if (root == 0) {
            root = BtreePage.allocate(db, 0, type, ins);
            height = 1;
        } else {
            final int result = BtreePage.insert(db, root, this, ins, height, unique, overwrite);

            if (result == op_overflow) {
                root = BtreePage.allocate(db, root, type, ins);
                height += 1;
            } else if (result == op_duplicate) {
                return -1;
            } else if (result == op_overwrite) {
                return ins.oldOid;
            }
        }

        updateCounter += 1;
        nElems += 1;
        modify();

        return 0;
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
        return new BtreeSelectionIterator<T>(checkKey(getKeyFromObject(type, from)), checkKey(getKeyFromObject(type,
                till)), order);
    }

    public int markTree() {
        if (root != 0) {
            return BtreePage.markPage((StorageImpl) getStorage(), root, type, height);
        }

        return 0;
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

        if (root != 0) {
            BtreePage.prefixSearch((StorageImpl) getStorage(), root, key.toCharArray(), height, list);
        }

        return list;
    }

    @Override
    public boolean put(final Key key, final T obj) {
        return insert(key, obj, false) >= 0;
    }

    @Override
    public boolean put(final Object key, final T obj) {
        return put(getKeyFromObject(type, key), obj);
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

        final BtreeKey rk = new BtreeKey(checkKey(key), 0);
        final StorageImpl db = (StorageImpl) getStorage();

        remove(rk);

        return (T) db.lookupObject(rk.oldOid, null);
    }

    @Override
    public void remove(final Key key, final T obj) {
        remove(new BtreeKey(checkKey(key), getStorage().getOid(obj)));
    }

    @Override
    public void remove(final Object key, final T obj) {
        remove(getKeyFromObject(type, key), obj);
    }

    @Override
    public T remove(final String key) {
        return remove(new Key(key));
    }

    boolean removeIfExists(final BtreeKey rem) {
        final StorageImpl db = (StorageImpl) getStorage();

        if (db == null) {
            throw new StorageError(StorageError.DELETED_OBJECT);
        }

        if (root == 0) {
            return false;
        }

        final int result = BtreePage.remove(db, root, this, rem, height);

        if (result == op_not_found) {
            return false;
        }

        nElems -= 1;

        if (result == op_underflow) {
            final Page pg = db.getPage(root);

            if (BtreePage.getnItems(pg) == 0) {
                int newRoot = 0;

                if (height != 1) {
                    newRoot = type == ClassDescriptor.tpString || type == ClassDescriptor.tpArrayOfByte ? BtreePage
                            .getKeyStrOid(pg, 0) : BtreePage.getReference(pg, BtreePage.maxItems - 1);
                }

                db.freePage(root);
                root = newRoot;
                height -= 1;
            }

            db.myPagePool.unfix(pg);
        } else if (result == op_overflow) {
            root = BtreePage.allocate(db, root, type, rem);
            height += 1;
        }

        updateCounter += 1;
        modify();

        return true;
    }

    boolean removeIfExists(final Key key, final Object obj) {
        return removeIfExists(new BtreeKey(checkKey(key), getStorage().getOid(obj)));
    }

    @Override
    public T removeKey(final Object key) {
        return remove(getKeyFromObject(type, key));
    }

    @Override
    public T set(final Key key, final T obj) {
        final int oid = insert(key, obj, true);
        return (T) (oid != 0 ? ((StorageImpl) getStorage()).lookupObject(oid, null) : null);
    }

    @Override
    public T set(final Object key, final T obj) {
        return set(getKeyFromObject(type, key), obj);
    }

    @Override
    public int size() {
        return nElems;
    }

    @Override
    public Object[] toArray() {
        final Object[] arr = new Object[nElems];

        if (root != 0) {
            BtreePage.traverseForward((StorageImpl) getStorage(), root, type, height, arr, 0);
        }

        return arr;
    }

    @Override
    public <E> E[] toArray(E[] arr) {
        if (arr.length < nElems) {
            arr = (E[]) Array.newInstance(arr.getClass().getComponentType(), nElems);
        }

        if (root != 0) {
            BtreePage.traverseForward((StorageImpl) getStorage(), root, type, height, arr, 0);
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

    Object unpackByteArrayKey(final Page pg, final int pos) {
        final int len = BtreePage.getKeyStrSize(pg, pos);
        final int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, pos);
        final byte[] val = new byte[len];

        System.arraycopy(pg.data, offs, val, 0, len);

        return val;
    }

    protected Object unpackEnum(final int val) {
        // Base B-Tree class has no information about particular enum type
        // so it is not able to correctly unpack enum key
        return val;
    }

    Object unpackKey(final StorageImpl db, final Page pg, final int pos) {
        final byte[] data = pg.data;
        final int offs = BtreePage.firstKeyOffs + pos * ClassDescriptor.sizeof[type];

        switch (type) {
            case ClassDescriptor.tpBoolean:
                return Boolean.valueOf(data[offs] != 0);
            case ClassDescriptor.tpByte:
                return new Byte(data[offs]);
            case ClassDescriptor.tpShort:
                return new Short(Bytes.unpack2(data, offs));
            case ClassDescriptor.tpChar:
                return new Character((char) Bytes.unpack2(data, offs));
            case ClassDescriptor.tpInt:
                return new Integer(Bytes.unpack4(data, offs));
            case ClassDescriptor.tpObject:
                return db.lookupObject(Bytes.unpack4(data, offs), null);
            case ClassDescriptor.tpLong:
                return new Long(Bytes.unpack8(data, offs));
            case ClassDescriptor.tpDate:
                return new Date(Bytes.unpack8(data, offs));
            case ClassDescriptor.tpFloat:
                return new Float(Float.intBitsToFloat(Bytes.unpack4(data, offs)));
            case ClassDescriptor.tpDouble:
                return new Double(Double.longBitsToDouble(Bytes.unpack8(data, offs)));
            case ClassDescriptor.tpEnum:
                return unpackEnum(Bytes.unpack4(data, offs));
            case ClassDescriptor.tpString:
                return unpackStrKey(pg, pos);
            case ClassDescriptor.tpArrayOfByte:
                return unpackByteArrayKey(pg, pos);
            default:
                Assert.failed("Invalid type");
        }

        return null;
    }

}
