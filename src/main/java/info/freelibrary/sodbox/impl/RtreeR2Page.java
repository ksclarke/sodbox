
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.RectangleR2;
import info.freelibrary.sodbox.Storage;

public class RtreeR2Page extends Persistent {

    static final int CARD = (Page.PAGE_SIZE - ObjectHeader.SIZE_OF - 4 * 3) / (8 * 4 + 4);

    static final int MIN_FILL = CARD / 2;

    int myCount;

    RectangleR2[] myRectR2;

    Link myBranch;

    RtreeR2Page(final Storage aStorage, final Object aObj, final RectangleR2 aRectR2) {
        myBranch = aStorage.createLink(CARD);
        myBranch.setSize(CARD);
        myRectR2 = new RectangleR2[CARD];

        setBranch(0, new RectangleR2(aRectR2), aObj);

        myCount = 1;

        for (int index = 1; index < CARD; index++) {
            myRectR2[index] = new RectangleR2();
        }
    }

    RtreeR2Page(final Storage aStorage, final RtreeR2Page aRoot, final RtreeR2Page aRtreeR2Page) {
        myBranch = aStorage.createLink(CARD);
        myBranch.setSize(CARD);
        myRectR2 = new RectangleR2[CARD];
        myCount = 2;

        setBranch(0, aRoot.cover(), aRoot);
        setBranch(1, aRtreeR2Page.cover(), aRtreeR2Page);

        for (int index = 2; index < CARD; index++) {
            myRectR2[index] = new RectangleR2();
        }
    }

    RtreeR2Page() {
    }

    RtreeR2Page insert(final Storage aStorage, final RectangleR2 aRectR2, final Object aObj, final int aLevel) {
        int level = aLevel;

        modify();

        if (--level != 0) {
            // not leaf page
            double minIncrement = Double.MAX_VALUE;
            double minArea = Double.MAX_VALUE;
            int mini = 0;
            int index;

            for (index = 0; index < myCount; index++) {
                final double area = myRectR2[index].area();
                final double increment = RectangleR2.joinArea(myRectR2[index], aRectR2) - area;

                if (increment < minIncrement) {
                    minIncrement = increment;
                    minArea = area;
                    mini = index;
                } else if (increment == minIncrement && area < minArea) {
                    minArea = area;
                    mini = index;
                }
            }

            final RtreeR2Page page1 = (RtreeR2Page) myBranch.get(mini);
            final RtreeR2Page page2 = page1.insert(aStorage, aRectR2, aObj, level);

            if (page2 == null) {
                // child was not split
                myRectR2[mini].join(aRectR2);
                return null;
            } else {
                // child was split
                setBranch(mini, page1.cover(), page1);
                return addBranch(aStorage, page2.cover(), page2);
            }
        } else {
            return addBranch(aStorage, new RectangleR2(aRectR2), aObj);
        }
    }

    int remove(final RectangleR2 aRectR2, final Object aObj, final int aLevel, final ArrayList aReinsertList) {
        int level = aLevel;

        if (--level != 0) {
            for (int index = 0; index < myCount; index++) {
                if (aRectR2.intersects(myRectR2[index])) {
                    final RtreeR2Page pg = (RtreeR2Page) myBranch.get(index);

                    int reinsertLevel = pg.remove(aRectR2, aObj, level, aReinsertList);

                    if (reinsertLevel >= 0) {
                        if (pg.myCount >= MIN_FILL) {
                            setBranch(index, pg.cover(), pg);
                            modify();
                        } else {
                            // not enough entries in child
                            aReinsertList.add(pg);
                            reinsertLevel = level - 1;
                            removeBranch(index);
                        }

                        return reinsertLevel;
                    }
                }
            }
        } else {
            for (int index = 0; index < myCount; index++) {
                if (myBranch.containsElement(index, aObj)) {
                    removeBranch(index);
                    return 0;
                }
            }
        }

        return -1;
    }

    void find(final RectangleR2 aRectR2, final ArrayList aResult, final int aLevel) {
        int level = aLevel;

        if (--level != 0) { /* this is an internal node in the tree */
            for (int index = 0; index < myCount; index++) {
                if (aRectR2.intersects(myRectR2[index])) {
                    ((RtreeR2Page) myBranch.get(index)).find(aRectR2, aResult, level);
                }
            }
        } else { /* this is a leaf node */
            for (int index = 0; index < myCount; index++) {
                if (aRectR2.intersects(myRectR2[index])) {
                    aResult.add(myBranch.get(index));
                }
            }
        }
    }

    void purge(final int aLevel) {
        int level = aLevel;

        if (--level != 0) { /* this is an internal node in the tree */
            for (int index = 0; index < myCount; index++) {
                ((RtreeR2Page) myBranch.get(index)).purge(level);
            }
        }

        deallocate();
    }

    final void setBranch(final int aIndex, final RectangleR2 aRectR2, final Object aObj) {
        myRectR2[aIndex] = aRectR2;
        myBranch.setObject(aIndex, aObj);
    }

    final void removeBranch(final int aIndex) {
        myCount -= 1;

        System.arraycopy(myRectR2, aIndex + 1, myRectR2, aIndex, myCount - aIndex);

        myBranch.remove(aIndex);
        myBranch.setSize(CARD);

        modify();
    }

    final RtreeR2Page addBranch(final Storage aStorage, final RectangleR2 aRectR2, final Object aObj) {
        if (myCount < CARD) {
            setBranch(myCount++, aRectR2, aObj);
            return null;
        } else {
            return splitPage(aStorage, aRectR2, aObj);
        }
    }

    final RtreeR2Page splitPage(final Storage aStorage, final RectangleR2 aRectR2, final Object aObj) {
        int seed0 = 0;
        int seed1 = 0;
        int index;
        int jndex;

        final double[] rectArea = new double[CARD + 1];

        double waste;
        double worstWaste = Double.NEGATIVE_INFINITY;

        //
        // As the seeds for the two groups, find two rectangles which waste
        // the most area if covered by a single rectangle.
        //
        rectArea[0] = aRectR2.area();

        for (index = 0; index < CARD; index++) {
            rectArea[index + 1] = myRectR2[index].area();
        }

        RectangleR2 rectR2 = aRectR2;

        for (index = 0; index < CARD; index++) {
            for (jndex = index + 1; jndex <= CARD; jndex++) {
                waste = RectangleR2.joinArea(rectR2, myRectR2[jndex - 1]) - rectArea[index] - rectArea[jndex];

                if (waste > worstWaste) {
                    worstWaste = waste;
                    seed0 = index;
                    seed1 = jndex;
                }
            }

            rectR2 = myRectR2[index];
        }

        final byte[] taken = new byte[CARD];

        final RectangleR2 group0;
        final RectangleR2 group1;
        final RtreeR2Page page;
        double groupArea0;
        double groupArea1;
        int groupCard0;
        int groupCard1;

        taken[seed1 - 1] = 2;
        group1 = new RectangleR2(myRectR2[seed1 - 1]);

        if (seed0 == 0) {
            group0 = new RectangleR2(aRectR2);
            page = new RtreeR2Page(aStorage, aObj, aRectR2);
        } else {
            group0 = new RectangleR2(myRectR2[seed0 - 1]);
            page = new RtreeR2Page(aStorage, myBranch.getRaw(seed0 - 1), group0);
            setBranch(seed0 - 1, aRectR2, aObj);
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
        while (groupCard0 + groupCard1 < CARD + 1 && groupCard0 < CARD + 1 - MIN_FILL && groupCard1 < CARD + 1 -
                MIN_FILL) {
            int betterGroup = -1;
            int chosen = -1;
            double biggestDiff = -1;

            for (index = 0; index < CARD; index++) {
                if (taken[index] == 0) {
                    final double diff = (RectangleR2.joinArea(group0, myRectR2[index]) - groupArea0) - (RectangleR2
                            .joinArea(group1, myRectR2[index]) - groupArea1);
                    if (diff > biggestDiff || -diff > biggestDiff) {
                        chosen = index;

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
                group0.join(myRectR2[chosen]);
                groupArea0 = group0.area();
                taken[chosen] = 1;
                page.setBranch(groupCard0++, myRectR2[chosen], myBranch.getRaw(chosen));
            } else {
                groupCard1 += 1;
                group1.join(myRectR2[chosen]);
                groupArea1 = group1.area();
                taken[chosen] = 2;
            }
        }

        //
        // If one group gets too full, then remaining rectangle are
        // split between two groups in such way to balance cards of two groups.
        //
        if (groupCard0 + groupCard1 < CARD + 1) {
            for (index = 0; index < CARD; index++) {
                if (taken[index] == 0) {
                    if (groupCard0 >= groupCard1) {
                        taken[index] = 2;
                        groupCard1 += 1;
                    } else {
                        taken[index] = 1;
                        page.setBranch(groupCard0++, myRectR2[index], myBranch.getRaw(index));
                    }
                }
            }
        }

        page.myCount = groupCard0;
        myCount = groupCard1;

        for (index = 0, jndex = 0; index < groupCard1; jndex++) {
            if (taken[jndex] == 2) {
                setBranch(index++, myRectR2[jndex], myBranch.getRaw(jndex));
            }
        }

        return page;
    }

    final RectangleR2 cover() {
        final RectangleR2 rectR2 = new RectangleR2(myRectR2[0]);

        for (int index = 1; index < myCount; index++) {
            rectR2.join(myRectR2[index]);
        }

        return rectR2;
    }
}
