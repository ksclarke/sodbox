
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

    static public final int RAW = 1;

    static public final int DIRTY = 2;

    static public final int DELETED = 4;

    transient Storage myStorage;

    transient int myOID;

    transient int myState;

    public PinnedPersistent() {
    }

    public PinnedPersistent(final Storage storage) {
        myStorage = storage;
    }

    @Override
    public void assignOid(final Storage storage, final int oid, final boolean raw) {
        myOID = oid;
        myStorage = storage;

        if (raw) {
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
    public void deallocate() {
        if (myOID != 0) {
            myStorage.deallocateObject(this);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }

        if (myOID == 0) {
            return super.equals(o);
        }

        return o instanceof IPersistent && ((IPersistent) o).getOid() == myOID;
    }

    @Override
    public final int getOid() {
        return myOID;
    }

    @Override
    public final Storage getStorage() {
        return myStorage;
    }

    @Override
    public int hashCode() {
        return myOID;
    }

    @Override
    public void invalidate() {
        myState &= ~DIRTY;
        myState |= RAW;
    }

    @Override
    public final boolean isDeleted() {
        return (myState & DELETED) != 0;
    }

    @Override
    public final boolean isModified() {
        return (myState & DIRTY) != 0;
    }

    @Override
    public final boolean isPersistent() {
        return myOID != 0;
    }

    @Override
    public final boolean isRaw() {
        return (myState & RAW) != 0;
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
    public void makePersistent(final Storage storage) {
        if (myOID == 0) {
            storage.makePersistent(this);
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
    public void onLoad() {
    }

    @Override
    public void onStore() {
    }

    @Override
    public void readExternal(final ObjectInput s) throws IOException, ClassNotFoundException {
        myOID = s.readInt();
    }

    @Override
    public boolean recursiveLoading() {
        return true;
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
    public void unassignOid() {
        myOID = 0;
        myState = DELETED;
        myStorage = null;
    }

    @Override
    public void writeExternal(final ObjectOutput s) throws IOException {
        if (s instanceof StorageImpl.PersistentObjectOutputStream) {
            makePersistent(((StorageImpl.PersistentObjectOutputStream) s).getStorage());
        }

        s.writeInt(myOID);
    }

}
