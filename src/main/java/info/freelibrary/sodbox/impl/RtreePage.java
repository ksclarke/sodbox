
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.Rectangle;
import info.freelibrary.sodbox.Storage;

public class RtreePage extends Persistent {

    static final int CARD = (Page.PAGE_SIZE - ObjectHeader.SIZE_OF - 4 * 3) / (4 * 4 + 4);

    static final int MIN_FILL = CARD / 2;

    int myIndex;

    Rectangle[] myRectangle;

    Link myBranch;

    RtreePage(final Storage aStorage, final Object aObj, final Rectangle aRect) {
        myBranch = aStorage.createLink(CARD);
        myBranch.setSize(CARD);
        myRectangle = new Rectangle[CARD];

        setBranch(0, new Rectangle(aRect), aObj);
        myIndex = 1;

        for (int i = 1; i < CARD; i++) {
            myRectangle[i] = new Rectangle();
        }
    }

    RtreePage(final Storage aStorage, final RtreePage aRoot, final RtreePage aPage) {
        myBranch = aStorage.createLink(CARD);
        myBranch.setSize(CARD);
        myRectangle = new Rectangle[CARD];
        myIndex = 2;

        setBranch(0, aRoot.cover(), aRoot);
        setBranch(1, aPage.cover(), aPage);

        for (int i = 2; i < CARD; i++) {
            myRectangle[i] = new Rectangle();
        }
    }

    RtreePage() {
    }

    RtreePage insert(final Storage aStorage, final Rectangle aRect, final Object aObj, final int aLevel) {
        int level = aLevel;

        modify();

        if (--level != 0) {
            // not leaf page
            int i;
            int mini = 0;
            long minIncr = Long.MAX_VALUE;
            long minArea = Long.MAX_VALUE;

            for (i = 0; i < myIndex; i++) {
                final long area = myRectangle[i].area();
                final long incr = Rectangle.joinArea(myRectangle[i], aRect) - area;

                if (incr < minIncr) {
                    minIncr = incr;
                    minArea = area;
                    mini = i;
                } else if (incr == minIncr && area < minArea) {
                    minArea = area;
                    mini = i;
                }
            }

            final RtreePage p = (RtreePage) myBranch.get(mini);
            final RtreePage q = p.insert(aStorage, aRect, aObj, level);

            if (q == null) {
                // child was not split
                myRectangle[mini].join(aRect);
                return null;
            } else {
                // child was split
                setBranch(mini, p.cover(), p);
                return addBranch(aStorage, q.cover(), q);
            }
        } else {
            return addBranch(aStorage, new Rectangle(aRect), aObj);
        }
    }

    @SuppressWarnings("unchecked")
    int remove(final Rectangle aRect, final Object aObj, final int aLevel, final ArrayList aReinsertList) {
        int level = aLevel;

        if (--level != 0) {
            for (int i = 0; i < myIndex; i++) {
                if (aRect.intersects(myRectangle[i])) {
                    final RtreePage page = (RtreePage) myBranch.get(i);
                    int reinsertLevel = page.remove(aRect, aObj, level, aReinsertList);

                    if (reinsertLevel >= 0) {
                        if (page.myIndex >= MIN_FILL) {
                            setBranch(i, page.cover(), page);
                            modify();
                        } else {
                            // not enough entries in child
                            aReinsertList.add(page);
                            reinsertLevel = level - 1;
                            removeBranch(i);
                        }

                        return reinsertLevel;
                    }
                }
            }
        } else {
            for (int i = 0; i < myIndex; i++) {
                if (myBranch.containsElement(i, aObj)) {
                    removeBranch(i);
                    return 0;
                }
            }
        }

        return -1;
    }

    @SuppressWarnings("unchecked")
    void find(final Rectangle aRect, final ArrayList aResult, final int aLevel) {
        int level = aLevel;

        if (--level != 0) { /* this is an internal node in the tree */
            for (int index = 0; index < myIndex; index++) {
                if (aRect.intersects(myRectangle[index])) {
                    ((RtreePage) myBranch.get(index)).find(aRect, aResult, level);
                }
            }
        } else { /* this is a leaf node */
            for (int index = 0; index < myIndex; index++) {
                if (aRect.intersects(myRectangle[index])) {
                    aResult.add(myBranch.get(index));
                }
            }
        }
    }

    void purge(final int aLevel) {
        int level = aLevel;

        if (--level != 0) { /* this is an internal node in the tree */
            for (int i = 0; i < myIndex; i++) {
                ((RtreePage) myBranch.get(i)).purge(level);
            }
        }

        deallocate();
    }

    @SuppressWarnings("unchecked")
    final void setBranch(final int aIndex, final Rectangle aRect, final Object aObj) {
        myRectangle[aIndex] = aRect;
        myBranch.setObject(aIndex, aObj);
    }

    final void removeBranch(final int aIndex) {
        myIndex -= 1;

        System.arraycopy(myRectangle, aIndex + 1, myRectangle, aIndex, myIndex - aIndex);

        myBranch.remove(aIndex);
        myBranch.setSize(CARD);

        modify();
    }

    final RtreePage addBranch(final Storage aStorage, final Rectangle aRect, final Object aObj) {
        if (myIndex < CARD) {
            setBranch(myIndex++, aRect, aObj);
            return null;
        } else {
            return splitPage(aStorage, aRect, aObj);
        }
    }

    final RtreePage splitPage(final Storage aStorage, final Rectangle aRect, final Object aObj) {
        final long[] rectArea = new long[CARD + 1];

        long worstWaste = Long.MIN_VALUE;
        long waste;
        int seed0 = 0;
        int seed1 = 0;
        int i;
        int j;

        // As seeds for the two groups, find two rectangles which waste the most area if covered by single rectangle
        rectArea[0] = aRect.area();

        for (i = 0; i < CARD; i++) {
            rectArea[i + 1] = myRectangle[i].area();
        }

        Rectangle rect = aRect;

        for (i = 0; i < CARD; i++) {
            for (j = i + 1; j <= CARD; j++) {
                waste = Rectangle.joinArea(rect, myRectangle[j - 1]) - rectArea[i] - rectArea[j];

                if (waste > worstWaste) {
                    worstWaste = waste;
                    seed0 = i;
                    seed1 = j;
                }
            }

            rect = myRectangle[i];
        }

        final byte[] taken = new byte[CARD];
        final Rectangle group0;
        final Rectangle group1;
        final RtreePage page;

        long groupArea0;
        long groupArea1;
        int groupCard0;
        int groupCard1;

        taken[seed1 - 1] = 2;
        group1 = new Rectangle(myRectangle[seed1 - 1]);

        if (seed0 == 0) {
            group0 = new Rectangle(aRect);
            page = new RtreePage(aStorage, aObj, aRect);
        } else {
            group0 = new Rectangle(myRectangle[seed0 - 1]);
            page = new RtreePage(aStorage, myBranch.getRaw(seed0 - 1), group0);

            setBranch(seed0 - 1, aRect, aObj);
        }

        groupCard0 = groupCard1 = 1;
        groupArea0 = rectArea[seed0];
        groupArea1 = rectArea[seed1];

        /*
         * Split remaining rectangles between two groups. The one chosen is the one with the greatest difference in
         * area expansion depending on which group - the rect most strongly attracted to one group and repelled from
         * the other.
         */
        while (groupCard0 + groupCard1 < CARD + 1 && groupCard0 < CARD + 1 - MIN_FILL && groupCard1 < CARD + 1 -
                MIN_FILL) {
            int betterGroup = -1;
            int chosen = -1;
            long biggestDiff = -1;

            for (i = 0; i < CARD; i++) {
                if (taken[i] == 0) {
                    final long diff = Rectangle.joinArea(group0, myRectangle[i]) - groupArea0 - (Rectangle.joinArea(
                            group1, myRectangle[i]) - groupArea1);
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
                group0.join(myRectangle[chosen]);
                groupArea0 = group0.area();
                taken[chosen] = 1;
                page.setBranch(groupCard0++, myRectangle[chosen], myBranch.getRaw(chosen));
            } else {
                groupCard1 += 1;
                group1.join(myRectangle[chosen]);
                groupArea1 = group1.area();
                taken[chosen] = 2;
            }
        }

        /*
         * If one group gets too full, then remaining rectangle are split between two groups in such way to balance
         * cards of two groups.
         */
        if (groupCard0 + groupCard1 < CARD + 1) {
            for (i = 0; i < CARD; i++) {
                if (taken[i] == 0) {
                    if (groupCard0 >= groupCard1) {
                        taken[i] = 2;
                        groupCard1 += 1;
                    } else {
                        taken[i] = 1;
                        page.setBranch(groupCard0++, myRectangle[i], myBranch.getRaw(i));
                    }
                }
            }
        }

        page.myIndex = groupCard0;
        myIndex = groupCard1;

        for (i = 0, j = 0; i < groupCard1; j++) {
            if (taken[j] == 2) {
                setBranch(i++, myRectangle[j], myBranch.getRaw(j));
            }
        }

        return page;
    }

    final Rectangle cover() {
        final Rectangle rect = new Rectangle(myRectangle[0]);

        for (int i = 1; i < myIndex; i++) {
            rect.join(myRectangle[i]);
        }

        return rect;
    }
}
