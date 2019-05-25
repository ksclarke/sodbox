
package info.freelibrary.sodbox;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import info.freelibrary.sodbox.impl.StorageImpl;

/**
 * Base class for all persistent capable objects. Unlike Persistent class it doesn't define finalize method. It means
 * that object derived from this class rather than Persistent, can be used either with infinite object cache
 * (DEFAULT_PAGE_POOL_SIZE) either with per-thread serializable transactions (in the last case all modified objects
 * are pinned in memory until the end of transaction). Presence of finalize method adds additional memory overhead
 * (especially for small classes) and slow down garbage collections. So using this class instead of Persistent can
 * improve performance of your application.
 */
public class PinnedPersistent implements IPersistent, ICloneable {

    public static final int RAW = 1;

    public static final int DIRTY = 2;

    public static final int DELETED = 4;

    transient Storage myStorage;

    transient int myOID;

    transient int myState;

    /**
     * Creates a new pinned persistent.
     */
    public PinnedPersistent() {
    }

    /**
     * Creates a new pinned persistent with the supplied storage.
     *
     * @param aStorage
     */
    public PinnedPersistent(final Storage aStorage) {
        myStorage = aStorage;
    }

    @Override
    public synchronized void load() {
        if (myOID != 0 && (myState & RAW) != 0) {
            myStorage.loadObject(this);
        }
    }

    @Override
    public synchronized void loadAndModify() {
        load();
        modify();
    }

    @Override
    public final boolean isRaw() {
        return (myState & RAW) != 0;
    }

    @Override
    public final boolean isModified() {
        return (myState & DIRTY) != 0;
    }

    @Override
    public final boolean isDeleted() {
        return (myState & DELETED) != 0;
    }

    @Override
    public final boolean isPersistent() {
        return myOID != 0;
    }

    @Override
    public void makePersistent(final Storage aStorage) {
        if (myOID == 0) {
            aStorage.makePersistent(this);
        }
    }

    @Override
    public void store() {
        if ((myState & RAW) != 0) {
            throw new StorageError(StorageError.ACCESS_TO_STUB);
        }

        if (myStorage != null) {
            myStorage.storeObject(this);
            myState &= ~DIRTY;
        }
    }

    @Override
    public void modify() {
        if ((myState & DIRTY) == 0 && myOID != 0) {
            if ((myState & RAW) != 0) {
                throw new StorageError(StorageError.ACCESS_TO_STUB);
            }
            Assert.that((myState & DELETED) == 0);
            myStorage.modifyObject(this);
            myState |= DIRTY;
        }
    }

    @Override
    public final int getOid() {
        return myOID;
    }

    @Override
    public void deallocate() {
        if (myOID != 0) {
            myStorage.deallocateObject(this);
        }
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
        if (aObject == this) {
            return true;
        }

        if (myOID == 0) {
            return super.equals(aObject);
        }

        return aObject instanceof IPersistent && ((IPersistent) aObject).getOid() == myOID;
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
        myState &= ~DIRTY;
        myState |= RAW;
    }

    @Override
    public void unassignOid() {
        myOID = 0;
        myState = DELETED;
        myStorage = null;
    }

    @Override
    public void assignOid(final Storage aStorage, final int aOID, final boolean aRaw) {
        myOID = aOID;
        myStorage = aStorage;

        if (aRaw) {
            myState |= RAW;
        } else {
            myState &= ~RAW;
        }
    }

    protected void clearState() {
        myState = 0;
        myOID = 0;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        final Persistent p = (Persistent) super.clone();

        p.myOID = 0;
        p.myState = 0;

        return p;
    }

    @Override
    public void readExternal(final ObjectInput aInput) throws IOException, ClassNotFoundException {
        myOID = aInput.readInt();
    }

    @Override
    public void writeExternal(final ObjectOutput aOutput) throws IOException {
        if (aOutput instanceof StorageImpl.PersistentObjectOutputStream) {
            makePersistent(((StorageImpl.PersistentObjectOutputStream) aOutput).getStorage());
        }

        aOutput.writeInt(myOID);
    }

}
