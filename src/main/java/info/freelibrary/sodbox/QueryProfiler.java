
package info.freelibrary.sodbox;

import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;

/**
 * Class used to profile query execution. It should be registered as storage listener.
 */
public class QueryProfiler extends StorageListener {

    protected HashMap<String, QueryInfo> myProfile = new HashMap<>();

    /**
     * Execute a query.
     *
     * @param aQuery A query
     * @param aElapsedTime An elapsed time
     * @param aSequentialSearch Whether it's a sequential search or not
     */
    public synchronized void queryExecution(final String aQuery, final long aElapsedTime,
            final boolean aSequentialSearch) {
        QueryInfo info = myProfile.get(aQuery);

        if (info == null) {
            info = new QueryInfo();
            info.myQuery = aQuery;
            myProfile.put(aQuery, info);
        }

        if (info.myMaxTime < aElapsedTime) {
            info.myMaxTime = aElapsedTime;
        }

        info.myTotalTime += aElapsedTime;
        info.myCount += 1;
        info.mySequentialSearch |= aSequentialSearch;
    }

    /**
     * Dump queries execution profile to standard output
     */
    public void dump() {
        dump(System.out);
    }

    /**
     * Dump queries execution profile to the specified destination.
     *
     * @param appendable destination where profile should be dumped (it can be StringBuilder, PrintStream...)
     */
    public void dump(final Appendable appendable) {
        final QueryInfo[] results = getProfile();
        final Formatter f = new Formatter(appendable);

        f.format("S     Total      Count Maximum Average Percent Query\n");
        long total = 0;

        for (final QueryInfo info : results) {
            total += info.myTotalTime;
        }

        final String format = "%c%10d %10d %7d %7d %6d%% %s\n";

        for (final QueryInfo info : results) {
            f.format(format, info.mySequentialSearch ? '!' : ' ', info.myTotalTime, info.myCount, info.myMaxTime,
                    info.myCount != 0 ? info.myTotalTime / info.myCount : 0L, total != 0 ? info.myTotalTime * 100 /
                            total : 0L, info.myQuery);
        }

        f.flush();
        f.close();
    }

    /**
     * Get array with QueryInfo elements sorted by (totalTime,count)
     */
    public QueryInfo[] getProfile() {
        final QueryInfo[] a = new QueryInfo[myProfile.size()];

        myProfile.values().toArray(a);
        Arrays.sort(a);

        return a;
    }

    public static class QueryInfo implements Comparable<QueryInfo> {

        public String myQuery;

        public long myTotalTime;

        public long myMaxTime;

        public long myCount;

        public boolean mySequentialSearch;

        @Override
        public int compareTo(final QueryInfo aQueryInfo) {
            return myTotalTime > aQueryInfo.myTotalTime ? -1 : myTotalTime < aQueryInfo.myTotalTime ? 1
                    : myCount > aQueryInfo.myCount ? -1 : myCount < aQueryInfo.myCount ? 1 : 0;
        }
    }

}
