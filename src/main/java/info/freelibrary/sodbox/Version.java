
package info.freelibrary.sodbox;

import java.util.Date;

/**
 * Base class for version of versioned object. All versions are kept in version history.
 */
public class Version extends PersistentResource {

    VersionHistory<Version> myHistory;

    private Link<Version> mySuccessors;

    private Link<Version> myPredecessors;

    private String[] myLabels;

    private Date myDate;

    private String myID;

    /**
     * Default constructor. Not directly accessible.
     */
    @SuppressWarnings("unused")
    private Version() {
    }

    /**
     * Constructor of root version. All other versions should be created using <code>Version.newVersion</code> or
     * <code>VersionHistory.checkout</code> methods.
     */
    protected Version(final Storage aStorage) {
        super(aStorage);

        mySuccessors = aStorage.<Version>createLink(1);
        myPredecessors = aStorage.<Version>createLink(1);
        myLabels = new String[0];
        myDate = new Date();
        myID = "1";
    }

    /**
     * Get version history containing this versioned object.
     */
    public synchronized VersionHistory<Version> getVersionHistory() {
        return myHistory;
    }

    /**
     * Get predecessors of this version.
     *
     * @return array of predecessor versions
     */
    public synchronized Version[] getPredecessors() {
        return myPredecessors.toArray(new Version[myPredecessors.size()]);
    }

    /**
     * Get successors of this version.
     *
     * @return array of predecessor versions
     */
    public synchronized Version[] getSuccessors() {
        return mySuccessors.toArray(new Version[mySuccessors.size()]);
    }

    /**
     * Check if version is checked-in.
     *
     * @return <code>true</code> if version belongs to version history
     */
    public boolean isCheckedIn() {
        return myID != null;
    }

    /**
     * Check if version is checked-out.
     *
     * @return <code>true</code> if version is just created and not checked-in yet (and so belongs to version history)
     */
    public boolean isCheckedOut() {
        return myID == null;
    }

    /**
     * Create new version which will be direct successor of this version. This version has to be checked-in in order
     * to be placed in version history.
     */
    public Version newVersion() {
        try {
            final Version newVersion = (Version) clone();

            newVersion.myPredecessors = getStorage().<Version>createLink(1);
            newVersion.myPredecessors.add(this);
            newVersion.mySuccessors = getStorage().<Version>createLink(1);
            newVersion.myLabels = new String[0];
            newVersion.myID = null;
            newVersion.myOID = 0;
            newVersion.myState = 0;

            return newVersion;
        } catch (final CloneNotSupportedException x) {
            // Could not happen since we cloned ourself
            throw new AssertionFailed("Clone not supported");
        }
    }

    /**
     * Check-in new version. This method inserts in version history version created by <code>Version.newVersion</code>
     * or <code>VersionHistory.checkout</code> method.
     */
    public void checkin() {
        synchronized (myHistory) {
            Assert.that(isCheckedOut());

            for (int i = 0; i < myPredecessors.size(); i++) {
                final Version predecessor = myPredecessors.get(i);

                synchronized (predecessor) {
                    if (i == 0) {
                        myID = predecessor.constructId();
                    }
                    predecessor.mySuccessors.add(this);
                }
            }

            myDate = new Date();
            myHistory.myVersions.add(this);
            myHistory.myCurrent = this;
            modify();
        }
    }

    /**
     * Make specified version predecessor of this version. This method can be used to perform merge of two versions
     * (merging of version data should be done by application itself).
     *
     * @param aPredecessor version to merged with
     */
    public void addPredecessor(final Version aPredecessor) {
        synchronized (aPredecessor) {
            synchronized (this) {
                myPredecessors.add(aPredecessor);

                if (isCheckedIn()) {
                    aPredecessor.mySuccessors.add(this);
                }
            }
        }
    }

    /**
     * Get date of version creation.
     *
     * @return date when this version was created
     */
    public Date getDate() {
        return myDate;
    }

    /**
     * Get labels associated with this version.
     *
     * @return array of labels assigned to this version
     */
    public synchronized String[] getLabels() {
        return myLabels;
    }

    /**
     * Add new label to this version.
     *
     * @param aLabel label to be associated with this version
     */
    public synchronized void addLabel(final String aLabel) {
        final String[] newLabels = new String[myLabels.length + 1];

        System.arraycopy(myLabels, 0, newLabels, 0, myLabels.length);
        newLabels[newLabels.length - 1] = aLabel;
        myLabels = newLabels;

        modify();
    }

    /**
     * Check if version has specified label.
     *
     * @param aLabel version label
     */
    public synchronized boolean hasLabel(final String aLabel) {
        for (int i = 0; i < myLabels.length; i++) {
            if (myLabels[i].equals(aLabel)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Get identifier of the version.
     *
     * @return version identifier automatically assigned by system
     */
    public String getId() {
        return myID;
    }

    private String constructId() {
        final int suffixPos = myID.lastIndexOf('.');
        final int suffix = Integer.parseInt(myID.substring(suffixPos + 1));
        String nextId = suffixPos < 0 ? Integer.toString(suffix + 1) : myID.substring(0, suffixPos) + Integer.toString(
                suffix + 1);

        if (mySuccessors.size() != 0) {
            nextId += '.' + mySuccessors.size() + ".1";
        }

        return nextId;
    }

}
