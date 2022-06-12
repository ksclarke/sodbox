
package info.freelibrary.sodbox.fulltext;

import info.freelibrary.sodbox.Storage;

/**
 * Class representing full text search result hit (document + rank)
 */
public class FullTextSearchHit implements Comparable {

    /**
     * Rank of the document for this query
     */
    public final float myRank;

    /**
     * Object identifier of document
     */
    public final int myOID;

    /**
     * Database storage
     */
    public final Storage myStorage;

    /**
     * Constructor of the full text search result hit
     */
    public FullTextSearchHit(final Storage aStorage, final int aOID, final float aRank) {
        myStorage = aStorage;
        myOID = aOID;
        myRank = aRank;
    }

    /**
     * Get document matching full text query
     */
    public Object getDocument() {
        return myStorage.getObjectByOID(myOID);
    }

    @Override
    public int compareTo(final Object aObject) {
        final float oRank = ((FullTextSearchHit) aObject).myRank;
        return myRank > oRank ? -1 : myRank < oRank ? 1 : 0;
    }

}
