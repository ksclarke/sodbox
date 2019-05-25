
package info.freelibrary.sodbox.fulltext;

import java.util.Arrays;

/**
 * Full text search result
 */
public class FullTextSearchResult {

    /**
     * Estimation of total number of documents in the index matching this query. Full text search query result is
     * usually limited by number of returned documents and query execution time. So there are can be more documents in
     * the index matching this query than actually returned. This field provides estimation for total number of
     * documents matching the query.
     */
    public int myEstimation;

    /**
     * Full text search result hits
     */
    public FullTextSearchHit[] myHits;

    /**
     * A full-text search result.
     *
     * @param aHits A hits
     * @param aEstimation A estimation
     */
    public FullTextSearchResult(final FullTextSearchHit[] aHits, final int aEstimation) {
        myHits = aHits;
        myEstimation = aEstimation;
    }

    /**
     * Merge results of two searches
     *
     * @param aResult Full text search result to merge with this result
     * @return Result set containing documents present in both result sets
     */
    public FullTextSearchResult merge(final FullTextSearchResult aResult) {
        if (myHits.length == 0 || aResult.myHits.length == 0) {
            return new FullTextSearchResult(new FullTextSearchHit[0], 0);
        }

        final FullTextSearchHit[] joinHits = new FullTextSearchHit[myHits.length + aResult.myHits.length];

        System.arraycopy(myHits, 0, joinHits, 0, myHits.length);
        System.arraycopy(aResult.myHits, 0, joinHits, myHits.length, aResult.myHits.length);

        Arrays.sort(joinHits, (o1, o2) -> o1.myOID - o2.myOID);

        int n = 0;

        for (int i = 1; i < joinHits.length; i++) {
            if (joinHits[i].myOID == joinHits[i - 1].myOID) {
                joinHits[n++] = new FullTextSearchHit(joinHits[i].myStorage, joinHits[i].myOID, joinHits[i -
                        1].myRank + joinHits[i].myRank);
                i += 1;
            }
        }

        final FullTextSearchHit[] mergeHits = new FullTextSearchHit[n];

        System.arraycopy(joinHits, 0, mergeHits, 0, n);
        Arrays.sort(joinHits);

        return new FullTextSearchResult(joinHits, Math.min(myEstimation * n / myHits.length, aResult.myEstimation *
                n / aResult.myHits.length));
    }

}
