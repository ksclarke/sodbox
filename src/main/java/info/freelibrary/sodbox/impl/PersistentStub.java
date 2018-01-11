
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.IPersistent;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

public class PersistentStub implements IPersistent {

    transient Storage storage;

    transient int oid;

    public PersistentStub(final Storage storage, final int oid) {
        this.storage = storage;
        this.oid = oid;
    }

    @Override
    public void assignOid(final Storage storage, final int oid, final boolean raw) {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        final PersistentStub p = (PersistentStub) super.clone();
        p.oid = 0;
        return p;
    }

    @Override
    public void deallocate() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public boolean equals(final Object o) {
        return getStorage().getOid(o) == oid;
    }

    @Override
    public final int getOid() {
        return oid;
    }

    @Override
    public final Storage getStorage() {
        return storage;
    }

    @Override
    public int hashCode() {
        return oid;
    }

    @Override
    public void invalidate() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public final boolean isDeleted() {
        return false;
    }

    @Override
    public final boolean isModified() {
        return false;
    }

    @Override
    public final boolean isPersistent() {
        return true;
    }

    @Override
    public final boolean isRaw() {
        return true;
    }

    @Override
    public void load() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void loadAndModify() {
        load();
        modify();
    }

    @Override
    public void makePersistent(final Storage storage) {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void modify() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void onLoad() {
    }

    @Override
    public void onStore() {
    }

    @Override
    public void readExternal(final java.io.ObjectInput s) throws java.io.IOException, ClassNotFoundException {
        oid = s.readInt();
    }

    @Override
    public boolean recursiveLoading() {
        return true;
    }

    @Override
    public void store() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void unassignOid() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void writeExternal(final java.io.ObjectOutput s) throws java.io.IOException {
        s.writeInt(oid);
    }
}
