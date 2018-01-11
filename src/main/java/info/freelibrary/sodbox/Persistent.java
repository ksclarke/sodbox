
package info.freelibrary.sodbox;

/**
 * Base class for all persistent capable objects.
 */
public class Persistent extends PinnedPersistent {

    public Persistent() {
    }

    public Persistent(final Storage storage) {
        super(storage);
    }

    @Override
    protected void finalize() {
        if ((myState & DIRTY) != 0 && myOID != 0) {
            myStorage.storeFinalizedObject(this);
        }
        myState = DELETED;
    }

}
