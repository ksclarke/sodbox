
package info.freelibrary.sodbox;

import java.util.Date;
import java.util.Iterator;

/**
 * Collection of version of versioned object. Versioned object should be access through version history object.
 * Instead of storing direct reference to Version in some component of some other persistent object, it is necessary
 * to store reference to it's <code>VersionHistory</code>.
 */
public class VersionHistory<V extends Version> extends PersistentResource {

    Link<V> myVersions;

    V myCurrent;

    /**
     * Create new version history.
     *
     * @param aRootVersion root version
     */
    @SuppressWarnings("unchecked")
    public VersionHistory(final V aRootVersion) {
        myVersions = aRootVersion.getStorage().<V>createLink(1);
        myVersions.add(aRootVersion);
        myCurrent = aRootVersion;
        myCurrent.myHistory = (VersionHistory<Version>) this;
    }

    /**
     * Get current version in version history. Current version can be explicitly set by setVersion or result of last
     * checkOut is used as current version.
     */
    public synchronized V getCurrent() {
        return myCurrent;
    }

    /**
     * Set new current version in version history.
     *
     * @param aVersion new current version in version history (it must belong to version history)
     */
    public synchronized void setCurrent(final V aVersion) {
        myCurrent = aVersion;
        modify();
    }

    /**
     * Checkout current version: create successor of the current version. This version has to be checked-in in order
     * to be placed in version history.
     *
     * @return checked-out version
     */
    @SuppressWarnings("unchecked")
    public synchronized V checkout() {
        Assert.that(myCurrent.isCheckedIn());
        return (V) myCurrent.newVersion();
    }

    /**
     * Get root version.
     *
     * @return root version in this version history
     */
    public synchronized V getRoot() {
        return myVersions.get(0);
    }

    /**
     * Get latest version before specified date.
     *
     * @param aTimestamp deadline, if <code>null</code> then the latest version in version history will be returned
     * @return version with the largest timestamp less than or equal to specified <code>timestamp</code>
     */
    public synchronized V getLatestBefore(final Date aTimestamp) {
        if (aTimestamp == null) {
            return myVersions.get(myVersions.size() - 1);
        }

        int l = 0;
        final int n = myVersions.size();
        int r = n;
        final long t = aTimestamp.getTime() + 1;

        while (l < r) {
            final int m = l + r >> 1;

            if (myVersions.get(m).getDate().getTime() < t) {
                l = m + 1;
            } else {
                r = m;
            }
        }

        return r > 0 ? myVersions.get(r - 1) : null;
    }

    /**
     * Get earliest version after specified date.
     *
     * @param aTimestamp deadline, if <code>null</code> then root version will be returned
     * @return version with the smallest timestamp greater than or equal to specified <code>timestamp</code>
     */
    public synchronized V getEarliestAfter(final Date aTimestamp) {
        if (aTimestamp == null) {
            return myVersions.get(0);
        }

        int l = 0;
        final int n = myVersions.size();
        int r = n;
        final long t = aTimestamp.getTime();

        while (l < r) {
            final int m = l + r >> 1;

            if (myVersions.get(m).getDate().getTime() < t) {
                l = m + 1;
            } else {
                r = m;
            }
        }

        return r < n ? myVersions.get(r) : null;
    }

    /**
     * Get version with specified label. If there are more than one version marked with this label, then the latest
     * one will be returned.
     *
     * @param aLabel version label
     * @return latest version with specified label
     */
    public synchronized V getVersionByLabel(final String aLabel) {
        for (int i = myVersions.size(); --i >= 0;) {
            final V v = myVersions.get(i);

            if (v.hasLabel(aLabel)) {
                return v;
            }
        }

        return null;
    }

    /**
     * Get version with specified ID.
     *
     * @param aID version ID
     * @return version with specified ID
     */
    public synchronized V getVersionById(final String aID) {
        for (int i = myVersions.size(); --i >= 0;) {
            final V v = myVersions.get(i);

            if (v.getId().equals(aID)) {
                return v;
            }
        }

        return null;
    }

    /**
     * Get all versions in version history.
     *
     * @return array of versions sorted by date
     */
    public synchronized Version[] getAllVersions() {
        return myVersions.toArray(new Version[myVersions.size()]);
    }

    /**
     * Get iterator through all version in version history Iteration is started from the root version and performed in
     * direction of increasing version timestamp This iterator supports remove() method.
     *
     * @return version iterator
     */
    public synchronized Iterator<V> iterator() {
        return myVersions.iterator();
    }

}
