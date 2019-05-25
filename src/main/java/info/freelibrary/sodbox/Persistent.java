
package info.freelibrary.sodbox;

/**
 * Base class for all persistent capable objects.
 */
public class Persistent extends PinnedPersistent {

    /**
     * Creates new Persistent.
     */
    public Persistent() {
    }

    /**
     * Creates a new Persistent using the supplied Storage.
     *
     * @param aStorage A storage
     */
    public Persistent(final Storage aStorage) {
        super(aStorage);
    }

    @Override
    protected void finalize() throws Throwable {
        if ((myState & DIRTY) != 0 && myOID != 0) {
            myStorage.storeFinalizedObject(this);
        }

        myState = DELETED;
        super.finalize();
    }

}
