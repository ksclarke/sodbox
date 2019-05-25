
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.CustomAllocator;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.Storage;

public class DefaultAllocator extends Persistent implements CustomAllocator {

    /**
     * Create a default allocator.
     *
     * @param aStorage A storage
     */
    public DefaultAllocator(final Storage aStorage) {
        super(aStorage);
    }

    protected DefaultAllocator() {
    }

    @Override
    public long allocate(final long aSize) {
        return ((StorageImpl) getStorage()).allocate(aSize, 0);
    }

    @Override
    public long getSegmentBase() {
        return 0;
    }

    @Override
    public long getSegmentSize() {
        return 1L << StorageImpl.DB_LARGE_DATABASE_OFFSET_BITS;
    }

    @Override
    public long reallocate(final long aPosition, final long aOldSize, final long aNewSize) {
        final StorageImpl db = (StorageImpl) getStorage();

        long position = aPosition;

        if ((aNewSize + StorageImpl.DB_ALLOCATION_QUANTUM - 1 & ~(StorageImpl.DB_ALLOCATION_QUANTUM -
                1)) > (aOldSize + StorageImpl.DB_ALLOCATION_QUANTUM - 1 & ~(StorageImpl.DB_ALLOCATION_QUANTUM - 1))) {
            final long newPos = db.allocate(aNewSize, 0);

            db.cloneBitmap(position, aOldSize);
            db.free(position, aOldSize);
            position = newPos;
        }

        return position;
    }

    @Override
    public void free(final long aPosition, final long aSize) {
        ((StorageImpl) getStorage()).cloneBitmap(aPosition, aSize);
    }

    @Override
    public void commit() {
    }

}
