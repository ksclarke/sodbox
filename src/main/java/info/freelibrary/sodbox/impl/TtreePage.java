
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;

import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentComparator;
import info.freelibrary.sodbox.Storage;

public class TtreePage extends Persistent {

    static final int MAX_ITEMS = (Page.PAGE_SIZE - ObjectHeader.SIZE_OF - 4 * 5) / 4;

    static final int MIN_ITEMS = MAX_ITEMS - 2; // minimal number of items in internal node

    static final int OK = 0;

    static final int NOT_UNIQUE = 1;

    static final int NOT_FOUND = 2;

    static final int OVERFLOW = 3;

    static final int UNDERFLOW = 4;

    TtreePage myLeft;

    TtreePage myRight;

    int myBalance;

    int myItemCount;

    Object[] myItems;

    TtreePage() {
    }

    TtreePage(final Storage aStorage, final Object aObj) {
        super(aStorage);

        myItemCount = 1;
        myItems = new Object[MAX_ITEMS];
        myItems[0] = aObj;
    }

    @Override
    public boolean recursiveLoading() {
        return false;
    }

    final Object loadItem(final int aIndex) {
        final Object obj = myItems[aIndex];

        getStorage().load(obj);

        return obj;
    }

    final boolean find(final PersistentComparator aComparator, final Object aMinValue, final int aMinInclusive,
            final Object aMaxValue, final int aMaxInclusive, final ArrayList aSelection) {
        final int count = myItemCount;

        int left;
        int right;
        int m;

        load();

        if (aMinValue != null) {
            if (-aComparator.compareMemberWithKey(loadItem(0), aMinValue) >= aMinInclusive) {
                if (-aComparator.compareMemberWithKey(loadItem(count - 1), aMinValue) >= aMinInclusive) {
                    if (myRight != null) {
                        return myRight.find(aComparator, aMinValue, aMinInclusive, aMaxValue, aMaxInclusive,
                                aSelection);
                    }

                    return true;
                }

                for (left = 0, right = count; left < right;) {
                    m = left + right >> 1;

                    if (-aComparator.compareMemberWithKey(loadItem(m), aMinValue) >= aMinInclusive) {
                        left = m + 1;
                    } else {
                        right = m;
                    }
                }

                while (right < count) {
                    if (aMaxValue != null && aComparator.compareMemberWithKey(loadItem(right),
                            aMaxValue) >= aMaxInclusive) {
                        return false;
                    }

                    aSelection.add(loadItem(right));
                    right += 1;
                }

                if (myRight != null) {
                    return myRight.find(aComparator, aMinValue, aMinInclusive, aMaxValue, aMaxInclusive, aSelection);
                }

                return true;
            }
        }

        if (myLeft != null) {
            if (!myLeft.find(aComparator, aMinValue, aMinInclusive, aMaxValue, aMaxInclusive, aSelection)) {
                return false;
            }
        }

        for (left = 0; left < count; left++) {
            if (aMaxValue != null && aComparator.compareMemberWithKey(loadItem(left), aMaxValue) >= aMaxInclusive) {
                return false;
            }

            aSelection.add(loadItem(left));
        }

        if (myRight != null) {
            return myRight.find(aComparator, aMinValue, aMinInclusive, aMaxValue, aMaxInclusive, aSelection);
        }

        return true;
    }

    final boolean contains(final PersistentComparator aComparator, final Object aKey) {
        final int count = myItemCount;

        int left;
        int right;
        int m;

        load();

        if (aComparator.compareMembers(loadItem(0), aKey) < 0) {
            if (aComparator.compareMembers(loadItem(count - 1), aKey) < 0) {
                if (myRight != null) {
                    return myRight.contains(aComparator, aKey);
                }

                return false;
            }

            for (left = 0, right = count; left < right;) {
                m = left + right >> 1;

                if (aComparator.compareMembers(loadItem(m), aKey) < 0) {
                    left = m + 1;
                } else {
                    right = m;
                }
            }

            while (right < count) {
                if (aKey.equals(loadItem(right))) {
                    return true;
                }

                if (aComparator.compareMembers(myItems[right], aKey) > 0) {
                    return false;
                }

                right += 1;
            }

            if (myRight != null) {
                return myRight.contains(aComparator, aKey);
            }

            return false;
        }

        if (myLeft != null) {
            if (myLeft.contains(aComparator, aKey)) {
                return true;
            }
        }

        for (left = 0; left < count; left++) {
            if (aKey.equals(loadItem(left))) {
                return true;
            }

            if (aComparator.compareMembers(myItems[left], aKey) > 0) {
                return false;
            }
        }

        if (myRight != null) {
            return myRight.contains(aComparator, aKey);
        }

        return false;
    }

    final boolean containsKey(final PersistentComparator aComparator, final Object aKey) {
        final int count = myItemCount;

        int left;
        int right;
        int m;

        load();

        if (aComparator.compareMemberWithKey(loadItem(0), aKey) < 0) {
            if (aComparator.compareMemberWithKey(loadItem(count - 1), aKey) < 0) {
                if (myRight != null) {
                    return myRight.containsKey(aComparator, aKey);
                }

                return false;
            }

            for (left = 0, right = count; left < right;) {
                m = left + right >> 1;

                if (aComparator.compareMemberWithKey(loadItem(m), aKey) < 0) {
                    left = m + 1;
                } else {
                    right = m;
                }
            }

            while (right < count) {
                final int diff = aComparator.compareMemberWithKey(loadItem(right), aKey);

                if (diff == 0) {
                    return true;
                } else if (diff > 0) {
                    return false;
                }

                right += 1;
            }

            if (myRight != null) {
                return myRight.containsKey(aComparator, aKey);
            }

            return false;
        }

        if (myLeft != null) {
            if (myLeft.containsKey(aComparator, aKey)) {
                return true;
            }
        }

        for (left = 0; left < count; left++) {
            final int diff = aComparator.compareMemberWithKey(loadItem(left), aKey);

            if (diff == 0) {
                return true;
            } else if (diff > 0) {
                return false;
            }
        }

        if (myRight != null) {
            return myRight.containsKey(aComparator, aKey);
        }

        return false;
    }

    final boolean containsObject(final PersistentComparator aComparator, final Object aObj) {
        final int count = myItemCount;

        int left;
        int right;
        int m;

        load();

        if (aComparator.compareMembers(loadItem(0), aObj) < 0) {
            if (aComparator.compareMembers(loadItem(count - 1), aObj) < 0) {
                if (myRight != null) {
                    return myRight.containsObject(aComparator, aObj);
                }

                return false;
            }

            for (left = 0, right = count; left < right;) {
                m = left + right >> 1;

                if (aComparator.compareMembers(loadItem(m), aObj) < 0) {
                    left = m + 1;
                } else {
                    right = m;
                }
            }

            while (right < count) {
                if (aObj == myItems[right]) {
                    return true;
                }

                if (aComparator.compareMembers(myItems[right], aObj) > 0) {
                    return false;
                }

                right += 1;
            }

            if (myRight != null) {
                return myRight.containsObject(aComparator, aObj);
            }

            return false;
        }

        if (myLeft != null) {
            if (myLeft.containsObject(aComparator, aObj)) {
                return true;
            }
        }

        for (left = 0; left < count; left++) {
            if (aObj == myItems[left]) {
                return true;
            }

            if (aComparator.compareMembers(myItems[left], aObj) > 0) {
                return false;
            }
        }

        if (myRight != null) {
            return myRight.containsObject(aComparator, aObj);
        }

        return false;
    }

    final int insert(final PersistentComparator aComparator, final Object aObj, final boolean aUniqueInsert,
            final PageReference aPageRef) {
        load();

        final int count = myItemCount;

        TtreePage page;
        int diff = aComparator.compareMembers(aObj, loadItem(0));

        if (diff <= 0) {
            if (aUniqueInsert && diff == 0) {
                return NOT_UNIQUE;
            }

            if ((myLeft == null || diff == 0) && count != MAX_ITEMS) {
                modify();

                System.arraycopy(myItems, 0, myItems, 1, count);

                myItems[0] = aObj;
                myItemCount += 1;

                return OK;
            }

            if (myLeft == null) {
                modify();
                myLeft = new TtreePage(getStorage(), aObj);
            } else {
                page = aPageRef.myPage;
                aPageRef.myPage = myLeft;

                final int result = myLeft.insert(aComparator, aObj, aUniqueInsert, aPageRef);

                if (result == NOT_UNIQUE) {
                    return NOT_UNIQUE;
                }

                modify();

                myLeft = aPageRef.myPage;
                aPageRef.myPage = page;

                if (result == OK) {
                    return OK;
                }
            }

            if (myBalance > 0) {
                myBalance = 0;
                return OK;
            } else if (myBalance == 0) {
                myBalance = -1;
                return OVERFLOW;
            } else {
                final TtreePage left = myLeft;

                left.load();
                left.modify();

                if (left.myBalance < 0) { // single LL turn
                    myLeft = left.myRight;
                    left.myRight = this;
                    myBalance = 0;
                    left.myBalance = 0;
                    aPageRef.myPage = left;
                } else { // double LR turn
                    final TtreePage right = left.myRight;

                    right.load();
                    right.modify();
                    left.myRight = right.myLeft;
                    right.myLeft = left;
                    myLeft = right.myRight;
                    right.myRight = this;
                    myBalance = right.myBalance < 0 ? 1 : 0;
                    left.myBalance = right.myBalance > 0 ? -1 : 0;
                    right.myBalance = 0;
                    aPageRef.myPage = right;
                }

                return OK;
            }
        }

        diff = aComparator.compareMembers(aObj, loadItem(count - 1));

        if (diff >= 0) {
            if (aUniqueInsert && diff == 0) {
                return NOT_UNIQUE;
            }

            if ((myRight == null || diff == 0) && count != MAX_ITEMS) {
                modify();

                myItems[count] = aObj;
                myItemCount += 1;

                return OK;
            }

            if (myRight == null) {
                modify();
                myRight = new TtreePage(getStorage(), aObj);
            } else {
                page = aPageRef.myPage;
                aPageRef.myPage = myRight;

                final int result = myRight.insert(aComparator, aObj, aUniqueInsert, aPageRef);

                if (result == NOT_UNIQUE) {
                    return NOT_UNIQUE;
                }

                modify();

                myRight = aPageRef.myPage;
                aPageRef.myPage = page;

                if (result == OK) {
                    return OK;
                }
            }

            if (myBalance < 0) {
                myBalance = 0;
                return OK;
            } else if (myBalance == 0) {
                myBalance = 1;
                return OVERFLOW;
            } else {
                final TtreePage right = myRight;

                right.load();
                right.modify();

                if (right.myBalance > 0) { // single RR turn
                    myRight = right.myLeft;
                    right.myLeft = this;
                    myBalance = 0;
                    right.myBalance = 0;
                    aPageRef.myPage = right;
                } else { // double RL turn
                    final TtreePage left = right.myLeft;

                    left.load();
                    left.modify();
                    right.myLeft = left.myRight;
                    left.myRight = right;
                    myRight = left.myLeft;
                    left.myLeft = this;
                    myBalance = left.myBalance > 0 ? -1 : 0;
                    right.myBalance = left.myBalance < 0 ? 1 : 0;
                    left.myBalance = 0;
                    aPageRef.myPage = left;
                }

                return OK;
            }
        }

        int left = 1;
        int right = count - 1;

        while (left < right) {
            final int i = left + right >> 1;

            diff = aComparator.compareMembers(aObj, loadItem(i));

            if (diff > 0) {
                left = i + 1;
            } else {
                right = i;

                if (diff == 0) {
                    if (aUniqueInsert) {
                        return NOT_UNIQUE;
                    }

                    break;
                }
            }
        }

        // Insert before item[r]
        modify();

        if (count != MAX_ITEMS) {
            System.arraycopy(myItems, right, myItems, right + 1, count - right);

            myItems[right] = aObj;
            myItemCount += 1;

            return OK;
        } else {
            final Object reinsertItem;

            if (myBalance >= 0) {
                reinsertItem = loadItem(0);
                System.arraycopy(myItems, 1, myItems, 0, right - 1);
                myItems[right - 1] = aObj;
            } else {
                reinsertItem = loadItem(count - 1);
                System.arraycopy(myItems, right, myItems, right + 1, count - right - 1);
                myItems[right] = aObj;
            }

            return insert(aComparator, reinsertItem, aUniqueInsert, aPageRef);
        }
    }

    final int balanceLeftBranch(final PageReference aPageRef) {
        if (myBalance < 0) {
            myBalance = 0;
            return UNDERFLOW;
        } else if (myBalance == 0) {
            myBalance = 1;
            return OK;
        } else {
            final TtreePage right = myRight;

            right.load();
            right.modify();

            if (right.myBalance >= 0) { // single RR turn
                myRight = right.myLeft;
                right.myLeft = this;

                if (right.myBalance == 0) {
                    myBalance = 1;
                    right.myBalance = -1;
                    aPageRef.myPage = right;
                    return OK;
                } else {
                    myBalance = 0;
                    right.myBalance = 0;
                    aPageRef.myPage = right;
                    return UNDERFLOW;
                }
            } else { // double RL turn
                final TtreePage left = right.myLeft;

                left.load();
                left.modify();
                right.myLeft = left.myRight;
                left.myRight = right;
                myRight = left.myLeft;
                left.myLeft = this;
                myBalance = left.myBalance > 0 ? -1 : 0;
                right.myBalance = left.myBalance < 0 ? 1 : 0;
                left.myBalance = 0;
                aPageRef.myPage = left;

                return UNDERFLOW;
            }
        }
    }

    final int balanceRightBranch(final PageReference aPageRef) {
        if (myBalance > 0) {
            myBalance = 0;
            return UNDERFLOW;
        } else if (myBalance == 0) {
            myBalance = -1;
            return OK;
        } else {
            final TtreePage left = myLeft;

            left.load();
            left.modify();

            if (left.myBalance <= 0) { // single LL turn
                myLeft = left.myRight;
                left.myRight = this;

                if (left.myBalance == 0) {
                    myBalance = -1;
                    left.myBalance = 1;
                    aPageRef.myPage = left;
                    return OK;
                } else {
                    myBalance = 0;
                    left.myBalance = 0;
                    aPageRef.myPage = left;
                    return UNDERFLOW;
                }
            } else { // double LR turn
                final TtreePage right = left.myRight;

                right.load();
                right.modify();
                left.myRight = right.myLeft;
                right.myLeft = left;
                myLeft = right.myRight;
                right.myRight = this;
                myBalance = right.myBalance < 0 ? 1 : 0;
                left.myBalance = right.myBalance > 0 ? -1 : 0;
                right.myBalance = 0;
                aPageRef.myPage = right;

                return UNDERFLOW;
            }
        }
    }

    final int remove(final PersistentComparator aComparator, final Object aObj, final PageReference aPageRef) {
        load();

        final int count = myItemCount;

        int diff = aComparator.compareMembers(aObj, loadItem(0));
        TtreePage page;

        if (diff <= 0) {
            if (myLeft != null) {
                modify();

                page = aPageRef.myPage;
                aPageRef.myPage = myLeft;

                final int height = myLeft.remove(aComparator, aObj, aPageRef);

                myLeft = aPageRef.myPage;
                aPageRef.myPage = page;

                if (height == UNDERFLOW) {
                    return balanceLeftBranch(aPageRef);
                } else if (height == OK) {
                    return OK;
                }
            }
        }

        diff = aComparator.compareMembers(aObj, loadItem(count - 1));

        if (diff <= 0) {
            for (int i = 0; i < count; i++) {
                if (myItems[i] == aObj) {
                    if (count == 1) {
                        if (myRight == null) {
                            deallocate();
                            aPageRef.myPage = myLeft;
                            return UNDERFLOW;
                        } else if (myLeft == null) {
                            deallocate();
                            aPageRef.myPage = myRight;
                            return UNDERFLOW;
                        }
                    }

                    modify();

                    if (count <= MIN_ITEMS) {
                        if (myLeft != null && myBalance <= 0) {
                            TtreePage prev = myLeft;

                            prev.load();

                            while (prev.myRight != null) {
                                prev = prev.myRight;
                                prev.load();
                            }

                            System.arraycopy(myItems, 0, myItems, 1, i);

                            myItems[0] = prev.myItems[prev.myItemCount - 1];
                            page = aPageRef.myPage;
                            aPageRef.myPage = myLeft;

                            int height = myLeft.remove(aComparator, loadItem(0), aPageRef);

                            myLeft = aPageRef.myPage;
                            aPageRef.myPage = page;

                            if (height == UNDERFLOW) {
                                height = balanceLeftBranch(aPageRef);
                            }

                            return height;
                        } else if (myRight != null) {
                            TtreePage next = myRight;

                            next.load();

                            while (next.myLeft != null) {
                                next = next.myLeft;
                                next.load();
                            }

                            System.arraycopy(myItems, i + 1, myItems, i, count - i - 1);

                            myItems[count - 1] = next.myItems[0];
                            page = aPageRef.myPage;
                            aPageRef.myPage = myRight;

                            int height = myRight.remove(aComparator, loadItem(count - 1), aPageRef);

                            myRight = aPageRef.myPage;
                            aPageRef.myPage = page;

                            if (height == UNDERFLOW) {
                                height = balanceRightBranch(aPageRef);
                            }

                            return height;
                        }
                    }

                    System.arraycopy(myItems, i + 1, myItems, i, count - i - 1);

                    myItems[count - 1] = null;
                    myItemCount -= 1;

                    return OK;
                }
            }
        }

        if (myRight != null) {
            modify();

            page = aPageRef.myPage;
            aPageRef.myPage = myRight;

            final int height = myRight.remove(aComparator, aObj, aPageRef);

            myRight = aPageRef.myPage;
            aPageRef.myPage = page;

            if (height == UNDERFLOW) {
                return balanceRightBranch(aPageRef);
            } else {
                return height;
            }
        }

        return NOT_FOUND;
    }

    final int toArray(final Object[] aArray, final int aIndex) {
        int arrayIndex = aIndex;

        load();

        if (myLeft != null) {
            arrayIndex = myLeft.toArray(aArray, arrayIndex);
        }

        for (int index = 0, count = myItemCount; index < count; index++) {
            aArray[arrayIndex++] = loadItem(index);
        }

        if (myRight != null) {
            arrayIndex = myRight.toArray(aArray, arrayIndex);
        }

        return arrayIndex;
    }

    final void prune() {
        load();

        if (myLeft != null) {
            myLeft.prune();
        }

        if (myRight != null) {
            myRight.prune();
        }

        deallocate();
    }

    static class PageReference {

        TtreePage myPage;

        PageReference(final TtreePage aPage) {
            myPage = aPage;
        }
    }

}
