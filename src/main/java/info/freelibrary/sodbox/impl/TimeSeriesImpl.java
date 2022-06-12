
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;
import info.freelibrary.sodbox.TimeSeries;

public class TimeSeriesImpl<T extends TimeSeries.Tick> extends PersistentCollection<T> implements TimeSeries<T> {

    private Index myIndex;

    private long myMaxBlockTimeInterval;

    private String myBlockClassName;

    private transient Class myBlockClass;

    TimeSeriesImpl(final Storage aStorage, final Class aBlockClass, final long aMaxBlockTimeInterval) {
        myBlockClass = aBlockClass;
        myMaxBlockTimeInterval = aMaxBlockTimeInterval;
        myBlockClassName = ClassDescriptor.getClassName(aBlockClass);
        myIndex = aStorage.createIndex(long.class, false);
    }

    TimeSeriesImpl() {
    }

    @Override
    public ArrayList<T> elements() {
        return new ArrayList<>(this);
    }

    @Override
    public Object[] toArray() {
        return elements().toArray();
    }

    @Override
    public <E> E[] toArray(final E[] aArray) {
        return elements().toArray(aArray);
    }

    @Override
    public boolean add(final T aTick) {
        final long time = aTick.getTime();
        final Iterator iterator = myIndex.iterator(new Key(time - myMaxBlockTimeInterval), new Key(time),
                Index.DESCENT_ORDER);

        if (iterator.hasNext()) {
            insertInBlock((Block) iterator.next(), aTick);
        } else {
            addNewBlock(aTick);
        }

        return true;
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(null, null, true);
    }

    @Override
    public IterableIterator<T> iterator(final Date aFrom, final Date aTo) {
        return iterator(aFrom, aTo, true);
    }

    @Override
    public IterableIterator<T> iterator(final boolean aAscent) {
        return iterator(null, null, aAscent);
    }

    @Override
    public IterableIterator<T> iterator(final Date aFrom, final Date aTo, final boolean aAscent) {
        final long low = aFrom == null ? 0 : aFrom.getTime();
        final long high = aTo == null ? Long.MAX_VALUE : aTo.getTime();

        return aAscent ? (IterableIterator<T>) new TimeSeriesIterator(low, high)
                : (IterableIterator<T>) new TimeSeriesReverseIterator(low, high);
    }

    @Override
    public Date getFirstTime() {
        final Iterator blockIterator = myIndex.iterator();

        if (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            return new Date(block.myTimestamp);
        }

        return null;
    }

    @Override
    public Date getLastTime() {
        final Iterator blockIterator = myIndex.iterator(null, null, Index.DESCENT_ORDER);

        if (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            return new Date(block.getTicks()[block.myUsed - 1].getTime());
        }

        return null;
    }

    @Override
    public int size() {
        return (int) countTicks();
    }

    @Override
    public long countTicks() {
        final Iterator blockIterator = myIndex.iterator();

        long count = 0;

        while (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            count += block.myUsed;
        }

        return count;
    }

    @Override
    public T getTick(final Date aTimestamp) {
        final long time = aTimestamp.getTime();
        final Iterator blockIterator = myIndex.iterator(new Key(time - myMaxBlockTimeInterval), new Key(time),
                Index.ASCENT_ORDER);

        while (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            final int count = block.myUsed;
            final Tick[] ticks = block.getTicks();

            int left = 0;
            int right = count;

            while (left < right) {
                final int index = (left + right) >> 1;

                if (time > ticks[index].getTime()) {
                    left = index + 1;
                } else {
                    right = index;
                }
            }

            Assert.that(left == right && (left == count || ticks[left].getTime() >= time));

            if (left < count && ticks[left].getTime() == time) {
                return (T) ticks[left];
            }
        }

        return null;
    }

    @Override
    public boolean has(final Date aTimestamp) {
        return getTick(aTimestamp) != null;
    }

    @Override
    public int remove(final Date aFrom, final Date aTo) {
        final long low = aFrom == null ? 0 : aFrom.getTime();
        final long high = aTo == null ? Long.MAX_VALUE : aTo.getTime();
        final Key fromKey = new Key(low - myMaxBlockTimeInterval);
        final Key tillKey = new Key(high);

        Iterator blockIterator = myIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER);
        int removedCount = 0;

        while (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            final int count = block.myUsed;
            final Tick[] ticks = block.getTicks();

            int left = 0;
            int right = count;

            while (left < right) {
                final int index = (left + right) >> 1;

                if (low > ticks[index].getTime()) {
                    left = index + 1;
                } else {
                    right = index;
                }
            }

            Assert.that(left == right && (left == count || ticks[left].getTime() >= low));

            while (right < count && ticks[right].getTime() <= high) {
                right += 1;
                removedCount += 1;
            }

            if (left == 0 && right == count) {
                myIndex.remove(new Key(block.myTimestamp), block);
                blockIterator = myIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER);
                block.deallocate();
            } else if (left < count && left != right) {
                if (left == 0) {
                    myIndex.remove(new Key(block.myTimestamp), block);
                    block.myTimestamp = ticks[right].getTime();
                    myIndex.put(new Key(block.myTimestamp), block);
                    blockIterator = myIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER);
                }

                while (right < count) {
                    ticks[left++] = ticks[right++];
                }

                block.myUsed = left;
                block.modify();
            }
        }

        return removedCount;
    }

    private void addNewBlock(final Tick aTick) {
        final Block block;

        try {
            block = (Block) myBlockClass.newInstance();
        } catch (final Exception x) {
            throw new StorageError(StorageError.CONSTRUCTOR_FAILURE, myBlockClass, x);
        }

        block.myTimestamp = aTick.getTime();
        block.myUsed = 1;
        block.getTicks()[0] = aTick;
        myIndex.put(new Key(block.myTimestamp), block);
    }

    void insertInBlock(final Block aBlock, final Tick aTick) {
        final long time = aTick.getTime();
        final int count = aBlock.myUsed;
        final Tick[] ticks = aBlock.getTicks();

        int right = count;
        int left = 0;
        int index;

        while (left < right) {
            index = (left + right) >> 1;

            if (time >= ticks[index].getTime()) {
                left = index + 1;
            } else {
                right = index;
            }
        }

        Assert.that(left == right && (left == count || ticks[left].getTime() >= time));

        if (right == 0) {
            if (ticks[count - 1].getTime() - time > myMaxBlockTimeInterval || count == ticks.length) {
                addNewBlock(aTick);
                return;
            }

            if (aBlock.myTimestamp != time) {
                myIndex.remove(new Key(aBlock.myTimestamp), aBlock);
                aBlock.myTimestamp = time;
                myIndex.put(new Key(aBlock.myTimestamp), aBlock);
            }
        } else if (right == count) {
            if (time - ticks[0].getTime() > myMaxBlockTimeInterval || count == ticks.length) {
                addNewBlock(aTick);
                return;
            }
        }

        if (count == ticks.length) {
            addNewBlock(ticks[count - 1]);

            for (index = count; --index > right;) {
                ticks[index] = ticks[index - 1];
            }
        } else {
            for (index = count; index > right; index--) {
                ticks[index] = ticks[index - 1];
            }

            aBlock.myUsed += 1;
        }

        ticks[right] = aTick;
        aBlock.modify();
    }

    @Override
    public void onLoad() {
        myBlockClass = ClassDescriptor.loadClass(getStorage(), myBlockClassName);
    }

    @Override
    public void deallocateMembers() {
        clear();
    }

    @Override
    public void clear() {
        final Iterator blockIterator = myIndex.iterator();
        while (blockIterator.hasNext()) {
            final Block block = (Block) blockIterator.next();
            block.deallocate();
        }
        myIndex.clear();
    }

    @Override
    public void deallocate() {
        clear();
        myIndex.deallocate();
        super.deallocate();
    }

    class TimeSeriesIterator extends IterableIterator<T> {

        private Iterator myBlockIterator;

        private Block myCurrentBlock;

        private int myPosition;

        private long myTo;

        TimeSeriesIterator(final long aFrom, final long aTo) {
            myPosition = -1;
            myTo = aTo;
            myBlockIterator = myIndex.iterator(new Key(aFrom - myMaxBlockTimeInterval), new Key(aTo),
                    Index.ASCENT_ORDER);

            while (myBlockIterator.hasNext()) {
                final Block block = (Block) myBlockIterator.next();
                final int count = block.myUsed;
                final Tick[] ticks = block.getTicks();

                int left = 0;
                int right = count;

                while (left < right) {
                    final int i = (left + right) >> 1;

                    if (aFrom > ticks[i].getTime()) {
                        left = i + 1;
                    } else {
                        right = i;
                    }
                }

                Assert.that(left == right && (left == count || ticks[left].getTime() >= aFrom));

                if (left < count) {
                    if (ticks[left].getTime() <= aTo) {
                        myPosition = left;
                        myCurrentBlock = block;
                    }

                    return;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return myPosition >= 0;
        }

        @Override
        public T next() {
            if (myPosition < 0) {
                throw new NoSuchElementException();
            }

            final T tick = (T) myCurrentBlock.getTicks()[myPosition];

            if (++myPosition == myCurrentBlock.myUsed) {
                if (myBlockIterator.hasNext()) {
                    myCurrentBlock = (Block) myBlockIterator.next();
                    myPosition = 0;
                } else {
                    myPosition = -1;
                    return tick;
                }
            }

            if (myCurrentBlock.getTicks()[myPosition].getTime() > myTo) {
                myPosition = -1;
            }

            return tick;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    class TimeSeriesReverseIterator extends IterableIterator<T> {

        private Iterator myBlockIterator;

        private Block myCurrentBlock;

        private int myPosition;

        private long myFrom;

        TimeSeriesReverseIterator(final long aFrom, final long aTo) {
            myPosition = -1;

            this.myFrom = aFrom;
            myBlockIterator = myIndex.iterator(new Key(aFrom - myMaxBlockTimeInterval), new Key(aTo),
                    Index.DESCENT_ORDER);

            while (myBlockIterator.hasNext()) {
                final Block block = (Block) myBlockIterator.next();
                final int count = block.myUsed;
                final Tick[] ticks = block.getTicks();

                int left = 0;
                int right = count;

                while (left < right) {
                    final int index = (left + right) >> 1;

                    if (aTo >= ticks[index].getTime()) {
                        left = index + 1;
                    } else {
                        right = index;
                    }
                }

                Assert.that(left == right && (left == count || ticks[left].getTime() > aTo));

                if (left > 0) {
                    if (ticks[left - 1].getTime() >= aFrom) {
                        myPosition = left - 1;
                        myCurrentBlock = block;
                    }

                    return;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return myPosition >= 0;
        }

        @Override
        public T next() {
            if (myPosition < 0) {
                throw new NoSuchElementException();
            }

            final T tick = (T) myCurrentBlock.getTicks()[myPosition];

            if (--myPosition < 0) {
                if (myBlockIterator.hasNext()) {
                    myCurrentBlock = (Block) myBlockIterator.next();
                    myPosition = myCurrentBlock.myUsed - 1;
                } else {
                    myPosition = -1;
                    return tick;
                }
            }

            if (myCurrentBlock.getTicks()[myPosition].getTime() < myFrom) {
                myPosition = -1;
            }

            return tick;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
