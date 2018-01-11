
package info.freelibrary.sodbox.fulltext;

import info.freelibrary.sodbox.Storage;

/**
 * Class representing full text search result hit (document + rank)
 */
public class FullTextSearchHit implements Comparable {

    /**
     * Rank of the document for this query
     */
    public final float rank;

    /**
     * Object identifier of document
     */
    public final int oid;

    public final Storage storage;

    /**
     * Constructor of the full text search result hit
     */
    public FullTextSearchHit(final Storage storage, final int oid, final float rank) {
        this.storage = storage;
        this.oid = oid;
        this.rank = rank;
    }

    @Override
    public int compareTo(final Object o) {
        final float oRank = ((FullTextSearchHit) o).rank;
        return rank > oRank ? -1 : rank < oRank ? 1 : 0;
    }

    /**
     * Get document matching full text query
     */
    public Object getDocument() {
        return storage.getObjectByOID(oid);
    }
}