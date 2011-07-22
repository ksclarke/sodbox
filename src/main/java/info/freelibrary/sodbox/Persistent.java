package info.freelibrary.sodbox;

import info.freelibrary.sodbox.PinnedPersistent;
import info.freelibrary.sodbox.Storage;

/**
 * Base class for all persistent capable objects
 */
public class Persistent extends PinnedPersistent
{ 
    public Persistent() {}

    public Persistent(Storage storage) { 
        super(storage);
    }

    protected void finalize() { 
        if ((myState & DIRTY) != 0 && myOID != 0) { 
        	myStorage.storeFinalizedObject(this);
        }
        myState = DELETED;
    }
}