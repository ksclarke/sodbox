
package info.freelibrary.sodbox.impl;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;

public class SoftHashTable extends WeakHashTable {

    /**
     * Creates a <soft>SoftHashTable</code>.
     *
     * @param aStorageImpl
     * @param aInitialCapacity
     */
    public SoftHashTable(final StorageImpl aStorageImpl, final int aInitialCapacity) {
        super(aStorageImpl, aInitialCapacity);
    }

    @Override
    protected Reference createReference(final Object aObj) {
        return new SoftReference(aObj);
    }

}
