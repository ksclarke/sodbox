
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import info.freelibrary.sodbox.IPersistent;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

public class PersistentStub implements IPersistent {

    transient Storage myStorage;

    transient int myOID;

    /**
     * Creates a persistent stub.
     *
     * @param aStorage A storage
     * @param aOID An object ID
     */
    public PersistentStub(final Storage aStorage, final int aOID) {
        myStorage = aStorage;
        myOID = aOID;
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
    public final boolean isRaw() {
        return true;
    }

    @Override
    public final boolean isModified() {
        return false;
    }

    @Override
    public final boolean isDeleted() {
        return false;
    }

    @Override
    public final boolean isPersistent() {
        return true;
    }

    @Override
    public void makePersistent(final Storage aStorage) {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void store() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void modify() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public final int getOid() {
        return myOID;
    }

    @Override
    public void deallocate() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public boolean recursiveLoading() {
        return true;
    }

    @Override
    public final Storage getStorage() {
        return myStorage;
    }

    @Override
    public boolean equals(final Object aObject) {
        return getStorage().getOid(aObject) == myOID;
    }

    @Override
    public int hashCode() {
        return myOID;
    }

    @Override
    public void onLoad() {
    }

    @Override
    public void onStore() {
    }

    @Override
    public void invalidate() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void unassignOid() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void assignOid(final Storage aStorage, final int aOID, final boolean aRaw) {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        final PersistentStub stub = (PersistentStub) super.clone();

        stub.myOID = 0;

        return stub;
    }

    @Override
    public void readExternal(final ObjectInput aObjectInput) throws IOException, ClassNotFoundException {
        myOID = aObjectInput.readInt();
    }

    @Override
    public void writeExternal(final ObjectOutput aObjectOutput) throws IOException {
        aObjectOutput.writeInt(myOID);
    }

}
