package info.freelibrary.sodbox;

import info.freelibrary.sodbox.impl.StorageImpl;

/**
 * Base class for all persistent capable objects
 */
public class Persistent implements IPersistent, ICloneable {

	transient Storage myStorage;
	transient int myOid;
	transient int myState;

	static public final int RAW = 1;
	static public final int DIRTY = 2;
	static public final int DELETED = 4;
	
	public synchronized void load() {
		if (myOid != 0 && (myState & RAW) != 0) {
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
		return myOid != 0;
	}

	public void makePersistent(Storage storage) {
		if (myOid == 0) {
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
		if ((myState & DIRTY) == 0 && myOid != 0) {
			if ((myState & RAW) != 0) {
				throw new StorageError(StorageError.ACCESS_TO_STUB);
			}

			Assert.that((myState & DELETED) == 0);

			myStorage.modifyObject(this);
			myState |= DIRTY;
		}
	}

	public Persistent() {
	}

	public Persistent(Storage storage) {
		myStorage = storage;
	}

	public final int getOid() {
		return myOid;
	}

	public void deallocate() {
		if (myOid != 0) {
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

		if (myOid == 0) {
			return super.equals(o);
		}

		return o instanceof IPersistent && ((IPersistent) o).getOid() == myOid;
	}

	public int hashCode() {
		return myOid;
	}

	public void onLoad() {
	}

	public void onStore() {
	}

	public void invalidate() {
		myState &= ~DIRTY;
		myState |= RAW;
	}

	protected void finalize() {
		if ((myState & DIRTY) != 0 && myOid != 0) {
			myStorage.storeFinalizedObject(this);
		}

		myState = DELETED;
	}

	public void unassignOid() {
		myOid = 0;
		myState = DELETED;
		myStorage = null;
	}

	public void assignOid(Storage aStorage, int aOid, boolean raw) {
		myOid = aOid;
		myStorage = aStorage;

		if (raw) {
			myState |= RAW;
		}
		else {
			myState &= ~RAW;
		}
	}

	protected void clearState() {
		myState = 0;
		myOid = 0;
	}

	public Object clone() throws CloneNotSupportedException {
		Persistent p = (Persistent) super.clone();

		p.myOid = 0;
		p.myState = 0;

		return p;
	}

	public void readExternal(java.io.ObjectInput s) throws java.io.IOException,
			ClassNotFoundException {
		myOid = s.readInt();
	}

	public void writeExternal(java.io.ObjectOutput s)
			throws java.io.IOException {
		if (s instanceof StorageImpl.PersistentObjectOutputStream) {
			makePersistent(((StorageImpl.PersistentObjectOutputStream) s)
					.getStorage());
		}

		s.writeInt(myOid);
	}
}
