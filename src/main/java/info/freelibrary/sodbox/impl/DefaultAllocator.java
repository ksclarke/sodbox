
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.CustomAllocator;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.Storage;

public class DefaultAllocator extends Persistent implements CustomAllocator {

    protected DefaultAllocator() {
    }

    /**
     * Create a new default allocator from the supplied Storage.
     *
     * @param aStorage A storage from which the default allocator should be created
     */
    public DefaultAllocator(final Storage aStorage) {
        super(aStorage);
    }

    @Override
    public long allocate(final long aSize) {
        return ((StorageImpl) getStorage()).allocate(aSize, 0);
    }

    @Override
    public void commit() {
    }

    @Override
    public void free(final long aPosition, final long aSize) {
        ((StorageImpl) getStorage()).cloneBitmap(aPosition, aSize);
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

        if ((((aNewSize + StorageImpl.DB_ALLOCATION_QUANTUM) - 1) & ~(StorageImpl.DB_ALLOCATION_QUANTUM -
                1)) > (((aOldSize + StorageImpl.DB_ALLOCATION_QUANTUM) - 1) & ~(StorageImpl.DB_ALLOCATION_QUANTUM -
                        1))) {
            final long newPosition = db.allocate(aNewSize, 0);

            db.cloneBitmap(aPosition, aOldSize);
            db.free(aPosition, aOldSize);

            return newPosition;
        }

        return aPosition;
    }
}
