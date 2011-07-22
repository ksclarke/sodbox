package info.freelibrary.sodbox;

import info.freelibrary.sodbox.impl.StorageImpl;

/**
 * Base class for all persistent capable objects.
 * Unlike Persistent class it doesn't define finalize method.
 * It means that object derived from this class rather than Persistent,
 * can be used either with infinite object cache (DEFAULT_PAGE_POOL_SIZE) either
 * with per-thread serializable transactions (in the last case all modified objects 
 * are pinned in memory until the end of transaction). 
 * Presence of finalize method adds additional memory overhead (especially for small classes)
 * and slow down garbage collections. So using this class instead of Persistent can improve
 * performance of your application.
 */
public class PinnedPersistent implements IPersistent, ICloneable 
{ 
    public synchronized void load() {
        if (myOID != 0 && (myState & RAW) != 0) { 
        	myStorage.loadObject(this);
        }
    }

    public synchronized void loadAndModify() {
        load();
        modify();
    }

    public final boolean isRaw() { 
        return (myState & RAW) != 0;
    } 
    
    public final boolean isModified() { 
        return (myState & DIRTY) != 0;
    } 
    
    public final boolean isDeleted() { 
        return (myState & DELETED) != 0;
    } 
    
    public final boolean isPersistent() { 
        return myOID != 0;
    }
    
    public void makePersistent(Storage storage) { 
        if (myOID == 0) { 
            storage.makePersistent(this);
        }
    }

    public void store() {
        if ((myState & RAW) != 0) { 
            throw new StorageError(StorageError.ACCESS_TO_STUB);
        }
        if (myStorage != null) { 
        	myStorage.storeObject(this);
        	myState &= ~DIRTY;
        }
    }
  
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

    public PinnedPersistent() {}

    public PinnedPersistent(Storage storage) { 
    	myStorage = storage;
    }

    public final int getOid() {
        return myOID;
    }

    public void deallocate() { 
        if (myOID != 0) { 
            myStorage.deallocateObject(this);
        }
    }

    public boolean recursiveLoading() {
        return true;
    }
    
    public final Storage getStorage() {
        return myStorage;
    }
    
    public boolean equals(Object o) { 
        if (o == this) {
            return true;
        } 
        if (myOID == 0) { 
            return super.equals(o);
        }
        return o instanceof IPersistent && ((IPersistent)o).getOid() == myOID;
    }

    public int hashCode() {
        return myOID;
    }

    public void onLoad() {
    }

    public void onStore() {
    }

    public void invalidate() { 
    	myState &= ~DIRTY;
        myState |= RAW;
    }

    transient Storage myStorage;
    transient int     myOID;
    transient int     myState;

    static public final int RAW   = 1;
    static public final int DIRTY = 2;
    static public final int DELETED = 4;

    public void unassignOid() {
    	myOID = 0;
    	myState = DELETED;
        myStorage = null;
    }

    public void assignOid(Storage storage, int oid, boolean raw) { 
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

    public Object clone() throws CloneNotSupportedException { 
        Persistent p = (Persistent)super.clone();
        p.myOID = 0;
        p.myState = 0;
        return p;
    }

    public void readExternal(java.io.ObjectInput s) throws java.io.IOException, ClassNotFoundException
    {
    	myOID = s.readInt();
    }

    public void writeExternal(java.io.ObjectOutput s) throws java.io.IOException
    {
        if (s instanceof StorageImpl.PersistentObjectOutputStream) { 
            makePersistent(((StorageImpl.PersistentObjectOutputStream)s).getStorage());
        }
        s.writeInt(myOID);
    }
}





