
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.GenericIndex;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;
import info.freelibrary.sodbox.TimeSeries;

public class TimeSeriesImpl<T extends TimeSeries.Tick> extends PersistentCollection<T> implements TimeSeries<T> {

    class TimeSeriesIterator extends IterableIterator<T> {

        private Iterator blockIterator;

        private Block currBlock;

        private int pos;

        private long till;

        TimeSeriesIterator(final long from, final long till) {
            pos = -1;
            this.till = till;
            blockIterator = index.iterator(new Key(from - maxBlockTimeInterval), new Key(till),
                    GenericIndex.ASCENT_ORDER);
            while (blockIterator.hasNext()) {
                final Block block = (Block) blockIterator.next();
                final int n = block.used;
                final Tick[] e = block.getTicks();
                int l = 0, r = n;
                while (l < r) {
                    final int i = l + r >> 1;
                    if (from > e[i].getTime()) {
                        l = i + 1;
                    } else {
                        r = i;
                    }
                }
                Assert.that(l == r && (l == n || e[l].getTime() >= from));
                if (l < n) {
                    if (e[l].getTime() <= till) {
                        pos = l;
                        currBlock = block;
                    }
                    return;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return pos >= 0;
        }

        @Override
        public T next() {
            if (pos < 0) {
                throw new NoSuchElementException();
            }
            final T tick = (T) currBlock.getTicks()[pos];
            if (++pos == currBlock.used) {
                if (blockIterator.hasNext()) {
                    currBlock = (Block) blockIterator.next();
                    pos = 0;
                } else {
                    pos = -1;
                    return tick;
                }
            }
            if (currBlock.getTicks()[pos].getTime() > till) {
                pos = -1;
            }
            return tick;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    class TimeSeriesReverseIterator extends IterableIterator<T> {

        private Iterator blockIterator;

        private Block currBlock;

        private int pos;

        private long from;

        TimeSeriesReverseIterator(final long from, final long till) {
            pos = -1;
            this.from = from;
            blockIterator = index.iterator(new Key(from - maxBlockTimeInterval), new Key(till),
                    GenericIndex.DESCENT_ORDER);
            while (blockIterator.hasNext()) {
                final Block block = (Block) blockIterator.next();
                final int n = block.used;
                final Tick[] e = block.getTicks();
                int l = 0, r = n;
                while (l < r) {
                    final int i = l + r >> 1;
                    if (till >= e[i].getTime()) {
                        l = i + 1;
                    } else {
                        r = i;
                    }
                }
                Assert.that(l == r && (l == n || e[l].getTime() > till));
                if (l > 0) {
                    if (e[l - 1].getTime() >= from) {
                        pos = l - 1;
                        currBlock = block;
                    }
                    return;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return pos >= 0;
        }

        @Override
        public T next() {
            if (pos < 0) {
                throw new NoSuchElementException();
            }
            final T tick = (T) currBlock.getTicks()[pos];
            if (--pos < 0) {
                if (blockIterator.hasNext()) {
                    currBlock = (Block) blockIterator.next();
                    pos = currBlock.used - 1;
                } else {
                    pos = -1;
                    return tick;
                }
            }
            if (currBlock.getTicks()[pos].getTime() < from) {
                pos = -1;
            }
            return tick;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private Index index;

    private long maxBlockTimeInterval;

    private String blockClassName;

    private transient Class blockClass;

    TimeSeriesImpl() {
    }

    TimeSeriesImpl(final Storage storage, final Class blockClass, final long maxBlockTimeInterval) {
        this.blockClass = blockClass;
        this.maxBlockTimeInterval = maxBlockTimeInterval;
        blockClassName = ClassDescriptor.getClassName(blockClass);
        index = storage.createIndex(long.class, false);
    }

    @Override
    public boolean add(final T tick) {
        final long time = tick.getTime();
        final Iterator iterator = index.iterator(new Key(time - maxBlockTimeInterval), new Key(time),
                GenericIndex.DESCENT_ORDER);
        if (iterator.hasNext()) {
            insertInBlock((Block) iterator.next(), tick);
        } else {
            addNewBlock(tick);
        }
        return true;
    }

    private void addNewBlock(final Tick t) {
        Block block;
        try {
            block = (Block) blockClass.newInstance();
        } catch (final Exception x) {
            throw new StorageError(StorageError.CONSTRUCTOR_FAILURE, blockClass, x);
        }
        block.timestamp = t.getTime();
        block.used = 1;
        block.getTicks()[0] = t;
        index.put(new Key(block.timestamp), block);
    }

    @Override
    public void clear() {
        final Iterator blockIterator = index.iterator();
        while (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            block.deallocate();
        }
        index.clear();
    }

    @Override
    public long countTicks() {
        long n = 0;
        final Iterator blockIterator = index.iterator();
        while (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            n += block.used;
        }
        return n;
    }

    @Override
    public void deallocate() {
        clear();
        index.deallocate();
        super.deallocate();
    }

    @Override
    public void deallocateMembers() {
        clear();
    }

    @Override
    public ArrayList<T> elements() {
        return new ArrayList<T>(this);
    }

    @Override
    public Date getFirstTime() {
        final Iterator blockIterator = index.iterator();
        if (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            return new Date(block.timestamp);
        }
        return null;
    }

    @Override
    public Date getLastTime() {
        final Iterator blockIterator = index.iterator(null, null, GenericIndex.DESCENT_ORDER);
        if (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            return new Date(block.getTicks()[block.used - 1].getTime());
        }
        return null;
    }

    @Override
    public T getTick(final Date timestamp) {
        final long time = timestamp.getTime();
        final Iterator blockIterator = index.iterator(new Key(time - maxBlockTimeInterval), new Key(time),
                GenericIndex.ASCENT_ORDER);
        while (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            final int n = block.used;
            final Tick[] e = block.getTicks();
            int l = 0, r = n;
            while (l < r) {
                final int i = l + r >> 1;
                if (time > e[i].getTime()) {
                    l = i + 1;
                } else {
                    r = i;
                }
            }
            Assert.that(l == r && (l == n || e[l].getTime() >= time));
            if (l < n && e[l].getTime() == time) {
                return (T) e[l];
            }
        }
        return null;
    }

    @Override
    public boolean has(final Date timestamp) {
        return getTick(timestamp) != null;
    }

    void insertInBlock(final Block block, final Tick tick) {
        final long t = tick.getTime();
        int i;
        final int n = block.used;

        final Tick[] e = block.getTicks();
        int l = 0, r = n;
        while (l < r) {
            i = l + r >> 1;
            if (t >= e[i].getTime()) {
                l = i + 1;
            } else {
                r = i;
            }
        }
        Assert.that(l == r && (l == n || e[l].getTime() >= t));
        if (r == 0) {
            if (e[n - 1].getTime() - t > maxBlockTimeInterval || n == e.length) {
                addNewBlock(tick);
                return;
            }
            if (block.timestamp != t) {
                index.remove(new Key(block.timestamp), block);
                block.timestamp = t;
                index.put(new Key(block.timestamp), block);
            }
        } else if (r == n) {
            if (t - e[0].getTime() > maxBlockTimeInterval || n == e.length) {
                addNewBlock(tick);
                return;
            }
        }
        if (n == e.length) {
            addNewBlock(e[n - 1]);
            for (i = n; --i > r;) {
                e[i] = e[i - 1];
            }
        } else {
            for (i = n; i > r; i--) {
                e[i] = e[i - 1];
            }
            block.used += 1;
        }
        e[r] = tick;
        block.modify();
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(null, null, true);
    }

    @Override
    public IterableIterator<T> iterator(final boolean ascent) {
        return iterator(null, null, ascent);
    }

    @Override
    public IterableIterator<T> iterator(final Date from, final Date till) {
        return iterator(from, till, true);
    }

    @Override
    public IterableIterator<T> iterator(final Date from, final Date till, final boolean ascent) {
        final long low = from == null ? 0 : from.getTime();
        final long high = till == null ? Long.MAX_VALUE : till.getTime();
        return ascent ? (IterableIterator<T>) new TimeSeriesIterator(low, high)
                : (IterableIterator<T>) new TimeSeriesReverseIterator(low, high);
    }

    @Override
    public void onLoad() {
        blockClass = ClassDescriptor.loadClass(getStorage(), blockClassName);
    }

    @Override
    public int remove(final Date from, final Date till) {
        final long low = from == null ? 0 : from.getTime();
        final long high = till == null ? Long.MAX_VALUE : till.getTime();
        int nRemoved = 0;
        final Key fromKey = new Key(low - maxBlockTimeInterval);
        final Key tillKey = new Key(high);
        Iterator blockIterator = index.iterator(fromKey, tillKey, GenericIndex.ASCENT_ORDER);
        while (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            final int n = block.used;
            final Tick[] e = block.getTicks();
            int l = 0, r = n;
            while (l < r) {
                final int i = l + r >> 1;
                if (low > e[i].getTime()) {
                    l = i + 1;
                } else {
                    r = i;
                }
            }
            Assert.that(l == r && (l == n || e[l].getTime() >= low));
            while (r < n && e[r].getTime() <= high) {
                r += 1;
                nRemoved += 1;
            }
            if (l == 0 && r == n) {
                index.remove(new Key(block.timestamp), block);
                blockIterator = index.iterator(fromKey, tillKey, GenericIndex.ASCENT_ORDER);
                block.deallocate();
            } else if (l < n && l != r) {
                if (l == 0) {
                    index.remove(new Key(block.timestamp), block);
                    block.timestamp = e[r].getTime();
                    index.put(new Key(block.timestamp), block);
                    blockIterator = index.iterator(fromKey, tillKey, GenericIndex.ASCENT_ORDER);
                }
                while (r < n) {
                    e[l++] = e[r++];
                }
                block.used = l;
                block.modify();
            }
        }
        return nRemoved;
    }

    @Override
    public int size() {
        return (int) countTicks();
    }

    @Override
    public Object[] toArray() {
        return elements().toArray();
    }

    @Override
    public <E> E[] toArray(final E[] arr) {
        return elements().toArray(arr);
    }
}
