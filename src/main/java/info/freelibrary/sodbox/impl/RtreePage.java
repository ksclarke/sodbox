
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.Rectangle;
import info.freelibrary.sodbox.Storage;

public class RtreePage extends Persistent {

    static final int card = (Page.pageSize - ObjectHeader.sizeof - 4 * 3) / (4 * 4 + 4);

    static final int minFill = card / 2;

    int n;

    Rectangle[] b;

    Link branch;

    RtreePage() {
    }

    RtreePage(final Storage storage, final Object obj, final Rectangle r) {
        branch = storage.createLink(card);
        branch.setSize(card);
        b = new Rectangle[card];
        setBranch(0, new Rectangle(r), obj);
        n = 1;
        for (int i = 1; i < card; i++) {
            b[i] = new Rectangle();
        }
    }

    RtreePage(final Storage storage, final RtreePage root, final RtreePage p) {
        branch = storage.createLink(card);
        branch.setSize(card);
        b = new Rectangle[card];
        n = 2;
        setBranch(0, root.cover(), root);
        setBranch(1, p.cover(), p);
        for (int i = 2; i < card; i++) {
            b[i] = new Rectangle();
        }
    }

    final RtreePage addBranch(final Storage storage, final Rectangle r, final Object obj) {
        if (n < card) {
            setBranch(n++, r, obj);
            return null;
        } else {
            return splitPage(storage, r, obj);
        }
    }

    final Rectangle cover() {
        final Rectangle r = new Rectangle(b[0]);
        for (int i = 1; i < n; i++) {
            r.join(b[i]);
        }
        return r;
    }

    void find(final Rectangle r, final ArrayList result, int level) {
        if (--level != 0) { /* this is an internal node in the tree */
            for (int i = 0; i < n; i++) {
                if (r.intersects(b[i])) {
                    ((RtreePage) branch.get(i)).find(r, result, level);
                }
            }
        } else { /* this is a leaf node */
            for (int i = 0; i < n; i++) {
                if (r.intersects(b[i])) {
                    result.add(branch.get(i));
                }
            }
        }
    }

    RtreePage insert(final Storage storage, final Rectangle r, final Object obj, int level) {
        modify();
        if (--level != 0) {
            // not leaf page
            int i, mini = 0;
            long minIncr = Long.MAX_VALUE;
            long minArea = Long.MAX_VALUE;
            for (i = 0; i < n; i++) {
                final long area = b[i].area();
                final long incr = Rectangle.joinArea(b[i], r) - area;
                if (incr < minIncr) {
                    minIncr = incr;
                    minArea = area;
                    mini = i;
                } else if (incr == minIncr && area < minArea) {
                    minArea = area;
                    mini = i;
                }
            }
            final RtreePage p = (RtreePage) branch.get(mini);
            final RtreePage q = p.insert(storage, r, obj, level);
            if (q == null) {
                // child was not split
                b[mini].join(r);
                return null;
            } else {
                // child was split
                setBranch(mini, p.cover(), p);
                return addBranch(storage, q.cover(), q);
            }
        } else {
            return addBranch(storage, new Rectangle(r), obj);
        }
    }

    void purge(int level) {
        if (--level != 0) { /* this is an internal node in the tree */
            for (int i = 0; i < n; i++) {
                ((RtreePage) branch.get(i)).purge(level);
            }
        }
        deallocate();
    }

    int remove(final Rectangle r, final Object obj, int level, final ArrayList reinsertList) {
        if (--level != 0) {
            for (int i = 0; i < n; i++) {
                if (r.intersects(b[i])) {
                    final RtreePage pg = (RtreePage) branch.get(i);
                    int reinsertLevel = pg.remove(r, obj, level, reinsertList);
                    if (reinsertLevel >= 0) {
                        if (pg.n >= minFill) {
                            setBranch(i, pg.cover(), pg);
                            modify();
                        } else {
                            // not enough entries in child
                            reinsertList.add(pg);
                            reinsertLevel = level - 1;
                            removeBranch(i);
                        }
                        return reinsertLevel;
                    }
                }
            }
        } else {
            for (int i = 0; i < n; i++) {
                if (branch.containsElement(i, obj)) {
                    removeBranch(i);
                    return 0;
                }
            }
        }
        return -1;
    }

    final void removeBranch(final int i) {
        n -= 1;
        System.arraycopy(b, i + 1, b, i, n - i);
        branch.remove(i);
        branch.setSize(card);
        modify();
    }

    final void setBranch(final int i, final Rectangle r, final Object obj) {
        b[i] = r;
        branch.setObject(i, obj);
    }

    final RtreePage splitPage(final Storage storage, final Rectangle r, final Object obj) {
        int i, j, seed0 = 0, seed1 = 0;
        final long[] rectArea = new long[card + 1];
        long waste;
        long worstWaste = Long.MIN_VALUE;
        //
        // As the seeds for the two groups, find two rectangles which waste
        // the most area if covered by a single rectangle.
        //
        rectArea[0] = r.area();
        for (i = 0; i < card; i++) {
            rectArea[i + 1] = b[i].area();
        }
        Rectangle bp = r;
        for (i = 0; i < card; i++) {
            for (j = i + 1; j <= card; j++) {
                waste = Rectangle.joinArea(bp, b[j - 1]) - rectArea[i] - rectArea[j];
                if (waste > worstWaste) {
                    worstWaste = waste;
                    seed0 = i;
                    seed1 = j;
                }
            }
            bp = b[i];
        }
        final byte[] taken = new byte[card];
        Rectangle group0, group1;
        long groupArea0, groupArea1;
        int groupCard0, groupCard1;
        RtreePage pg;

        taken[seed1 - 1] = 2;
        group1 = new Rectangle(b[seed1 - 1]);

        if (seed0 == 0) {
            group0 = new Rectangle(r);
            pg = new RtreePage(storage, obj, r);
        } else {
            group0 = new Rectangle(b[seed0 - 1]);
            pg = new RtreePage(storage, branch.getRaw(seed0 - 1), group0);
            setBranch(seed0 - 1, r, obj);
        }
        groupCard0 = groupCard1 = 1;
        groupArea0 = rectArea[seed0];
        groupArea1 = rectArea[seed1];
        //
        // Split remaining rectangles between two groups.
        // The one chosen is the one with the greatest difference in area
        // expansion depending on which group - the rect most strongly
        // attracted to one group and repelled from the other.
        //
        while (groupCard0 + groupCard1 < card + 1 && groupCard0 < card + 1 - minFill && groupCard1 < card + 1 -
                minFill) {
            int betterGroup = -1, chosen = -1;
            long biggestDiff = -1;
            for (i = 0; i < card; i++) {
                if (taken[i] == 0) {
                    final long diff = Rectangle.joinArea(group0, b[i]) - groupArea0 - (Rectangle.joinArea(group1,
                            b[i]) - groupArea1);
                    if (diff > biggestDiff || -diff > biggestDiff) {
                        chosen = i;
                        if (diff < 0) {
                            betterGroup = 0;
                            biggestDiff = -diff;
                        } else {
                            betterGroup = 1;
                            biggestDiff = diff;
                        }
                    }
                }
            }
            Assert.that(chosen >= 0);
            if (betterGroup == 0) {
                group0.join(b[chosen]);
                groupArea0 = group0.area();
                taken[chosen] = 1;
                pg.setBranch(groupCard0++, b[chosen], branch.getRaw(chosen));
            } else {
                groupCard1 += 1;
                group1.join(b[chosen]);
                groupArea1 = group1.area();
                taken[chosen] = 2;
            }
        }
        //
        // If one group gets too full, then remaining rectangle are
        // split between two groups in such way to balance cards of two groups.
        //
        if (groupCard0 + groupCard1 < card + 1) {
            for (i = 0; i < card; i++) {
                if (taken[i] == 0) {
                    if (groupCard0 >= groupCard1) {
                        taken[i] = 2;
                        groupCard1 += 1;
                    } else {
                        taken[i] = 1;
                        pg.setBranch(groupCard0++, b[i], branch.getRaw(i));
                    }
                }
            }
        }
        pg.n = groupCard0;
        n = groupCard1;
        for (i = 0, j = 0; i < groupCard1; j++) {
            if (taken[j] == 2) {
                setBranch(i++, b[j], branch.getRaw(j));
            }
        }
        return pg;
    }
}
