
package info.freelibrary.sodbox;

import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;

/**
 * Class used to profile query execution. It should be registered as storage listener.
 */
public class QueryProfiler extends StorageListener {

    public static class QueryInfo implements Comparable<QueryInfo> {

        public String query;

        public long totalTime;

        public long maxTime;

        public long count;

        public boolean sequentialSearch;

        @Override
        public int compareTo(final QueryInfo info) {
            return totalTime > info.totalTime ? -1 : totalTime < info.totalTime ? 1 : count > info.count ? -1
                    : count < info.count ? 1 : 0;
        }
    }

    protected HashMap<String, QueryInfo> profile = new HashMap<String, QueryInfo>();

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
            total += info.totalTime;
        }

        final String format = "%c%10d %10d %7d %7d %6d%% %s\n";

        for (final QueryInfo info : results) {
            f.format(format, info.sequentialSearch ? '!' : ' ', info.totalTime, info.count, info.maxTime,
                    info.count != 0 ? info.totalTime / info.count : 0L, total != 0 ? info.totalTime * 100 / total
                            : 0L, info.query);
        }

        f.flush();
    }

    /**
     * Get array with QueryInfo elements sorted by (totalTime,count)
     */
    public QueryInfo[] getProfile() {
        final QueryInfo[] a = new QueryInfo[profile.size()];

        profile.values().toArray(a);
        Arrays.sort(a);

        return a;
    }

    public synchronized void queryExecution(final String query, final long elapsedTime,
            final boolean sequentialSearch) {
        QueryInfo info = profile.get(query);

        if (info == null) {
            info = new QueryInfo();
            info.query = query;
            profile.put(query, info);
        }

        if (info.maxTime < elapsedTime) {
            info.maxTime = elapsedTime;
        }

        info.totalTime += elapsedTime;
        info.count += 1;
        info.sequentialSearch |= sequentialSearch;
    }

}
