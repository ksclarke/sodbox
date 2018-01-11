
package info.freelibrary.sodbox.impl;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;

public class SoftHashTable extends WeakHashTable {

    public SoftHashTable(final StorageImpl db, final int initialCapacity) {
        super(db, initialCapacity);
    }

    @Override
    protected Reference createReference(final Object obj) {
        return new SoftReference(obj);
    }
}
