
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Stack;

import info.freelibrary.sodbox.IterableIterator;
import info.freelibrary.sodbox.MultidimensionalComparator;
import info.freelibrary.sodbox.MultidimensionalIndex;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentCollection;
import info.freelibrary.sodbox.PersistentIterator;
import info.freelibrary.sodbox.Storage;

public class KDTree<T> extends PersistentCollection<T> implements MultidimensionalIndex<T> {

    static final int OK = 0;

    static final int NOT_FOUND = 1;

    static final int TRUNCATE = 2;

    KDTreeNode myRoot;

    int myMemberCount;

    int myHeight;

    MultidimensionalComparator<T> myComparator;

    @SuppressWarnings("unused")
    private KDTree() {
    }

    KDTree(final Storage aStorage, final MultidimensionalComparator<T> aComparator) {
        super(aStorage);

        myComparator = aComparator;
    }

    KDTree(final Storage aStorage, final Class aClass, final String[] aFieldNames,
            final boolean aTreateZeroAsUndefinedValue) {
        super(aStorage);

        myComparator = new ReflectionMultidimensionalComparator<>(aStorage, aClass, aFieldNames,
                aTreateZeroAsUndefinedValue);
    }

    @Override
    public MultidimensionalComparator<T> getComparator() {
        return myComparator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void optimize() {
        final Iterator<T> itr = iterator();
        final int memberCount = myMemberCount;
        final Object[] members = new Object[memberCount];

        for (int index = 0; index < memberCount; index++) {
            members[index] = itr.next();
        }

        final Random random = new Random();

        for (int index = 0; index < memberCount; index++) {
            final int j = random.nextInt(memberCount);
            final Object tmp = members[j];

            members[j] = members[index];
            members[index] = tmp;
        }

        clear();

        for (int index = 0; index < memberCount; index++) {
            add((T) members[index]);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean add(final T aObject) {
        modify();

        if (myRoot == null) {
            myRoot = new KDTreeNode<>(getStorage(), aObject);
            myHeight = 1;
        } else {
            final int level = myRoot.insert(aObject, myComparator, 0);

            if (level >= myHeight) {
                myHeight = level + 1;
            }
        }

        myMemberCount += 1;

        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(final Object aObject) {
        if (myRoot == null) {
            return false;
        }

        final int result = myRoot.remove(aObject, myComparator, 0);

        if (result == NOT_FOUND) {
            return false;
        }

        modify();

        if (result == TRUNCATE) {
            myRoot = null;
        }

        myMemberCount -= 1;

        return true;
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(null, null);
    }

    @Override
    public IterableIterator<T> iterator(final T aPattern) {
        return iterator(aPattern, aPattern);
    }

    @Override
    public IterableIterator<T> iterator(final T aLow, final T aHigh) {
        return new KDTreeIterator(aLow, aHigh);
    }

    @Override
    public ArrayList<T> queryByExample(final T aPattern) {
        return queryByExample(aPattern, aPattern);
    }

    @Override
    public ArrayList<T> queryByExample(final T aLow, final T aHigh) {
        final Iterator<T> iterator = iterator(aLow, aHigh);
        final ArrayList<T> list = new ArrayList<>();

        while (iterator.hasNext()) {
            list.add(iterator.next());
        }

        return list;
    }

    @Override
    public Object[] toArray() {
        return queryByExample(null, null).toArray();
    }

    @Override
    public <E> E[] toArray(final E[] arr) {
        return queryByExample(null, null).toArray(arr);
    }

    @Override
    public int size() {
        return myMemberCount;
    }

    @Override
    public int getHeight() {
        return myHeight;
    }

    @Override
    public void clear() {
        if (myRoot != null) {
            myRoot.deallocate();
            modify();
            myRoot = null;
            myMemberCount = 0;
            myHeight = 0;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(final Object aMember) {
        final Iterator<T> iterator = iterator((T) aMember);

        while (iterator.hasNext()) {
            if (iterator.next() == aMember) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void deallocate() {
        if (myRoot != null) {
            myRoot.deallocate();
        }

        super.deallocate();
    }

    int compareAllComponents(final T aPattern, final T aObject) {
        final int n = myComparator.getNumberOfDimensions();

        int result = MultidimensionalComparator.EQ;

        for (int i = 0; i < n; i++) {
            final int diff = myComparator.compare(aPattern, aObject, i);

            if (diff == MultidimensionalComparator.RIGHT_UNDEFINED) {
                return diff;
            } else if (diff == MultidimensionalComparator.LT) {
                if (result == MultidimensionalComparator.GT) {
                    return MultidimensionalComparator.NE;
                } else {
                    result = MultidimensionalComparator.LT;
                }
            } else if (diff == MultidimensionalComparator.GT) {
                if (result == MultidimensionalComparator.LT) {
                    return MultidimensionalComparator.NE;
                } else {
                    result = MultidimensionalComparator.GT;
                }
            }
        }

        return result;
    }

    public class KDTreeIterator extends IterableIterator<T> implements PersistentIterator {

        Stack<KDTreeNode<T>> myStack;

        int myDimensionCount;

        T myHigh;

        T myLow;

        KDTreeNode<T> myCurrentNode;

        KDTreeNode<T> myNextNode;

        int myCurrentLevel;

        @SuppressWarnings("unchecked")
        KDTreeIterator(final T aLow, final T aHigh) {
            myLow = aLow;
            myHigh = aHigh;
            myDimensionCount = myComparator.getNumberOfDimensions();
            myStack = new Stack<>();
            getMin(myRoot);
        }

        /**
         * Gets tree level.
         *
         * @return A level
         */
        public int getLevel() {
            return myCurrentLevel;
        }

        @SuppressWarnings("unchecked")
        private boolean getMin(final KDTreeNode<T> aNode) {
            KDTreeNode node = aNode;

            if (node != null) {
                while (true) {
                    node.load();
                    myStack.push(node);

                    final int diff = myLow == null ? MultidimensionalComparator.LEFT_UNDEFINED : myComparator.compare(
                            myLow, (T) node.myObject, (myStack.size() - 1) % myDimensionCount);

                    if (diff != MultidimensionalComparator.GT && node.myLeftNode != null) {
                        node = node.myLeftNode;
                    } else {
                        return true;
                    }
                }
            }

            return false;
        }

        @SuppressWarnings({ "unchecked", "checkstyle:BooleanExpressionComplexity" })
        @Override
        public boolean hasNext() {
            if (myNextNode != null) {
                return true;
            }

            while (!myStack.empty()) {
                final KDTreeNode<T> node = myStack.pop();

                if (node != null) {
                    if (!node.isDeleted) {
                        int result;

                        if ((myLow == null || (result = compareAllComponents(myLow,
                                node.myObject)) == MultidimensionalComparator.LT ||
                                result == MultidimensionalComparator.EQ) && (myHigh == null || (result =
                                        compareAllComponents(myHigh,
                                                node.myObject)) == MultidimensionalComparator.GT ||
                                        result == MultidimensionalComparator.EQ)) {
                            myNextNode = node;
                            myCurrentLevel = myStack.size();
                        }
                    }

                    if (node.myRightNode != null && (myHigh == null || myComparator.compare(myHigh, node.myObject,
                            myStack.size() % myDimensionCount) != MultidimensionalComparator.LT)) {
                        myStack.push(null);

                        if (!getMin(node.myRightNode)) {
                            myStack.pop();
                        }
                    }

                    if (myNextNode != null) {
                        return true;
                    }
                }
            }

            return false;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            myCurrentNode = myNextNode;
            myNextNode = null;

            return myCurrentNode.myObject;
        }

        @Override
        public int nextOID() {
            if (!hasNext()) {
                return 0;
            }

            myCurrentNode = myNextNode;
            myNextNode = null;

            return getStorage().getOid(myCurrentNode.myObject);
        }

        @Override
        public void remove() {
            if (myCurrentNode == null) {
                throw new IllegalStateException();
            }

            myCurrentNode.modify();
            myCurrentNode.myObject = myComparator.cloneField(myCurrentNode.myObject, myCurrentLevel %
                    myDimensionCount);
            myCurrentNode.isDeleted = true;
            myCurrentNode = null;
        }
    }

    static class KDTreeNode<T> extends Persistent {

        KDTreeNode myLeftNode;

        KDTreeNode myRightNode;

        T myObject;

        boolean isDeleted;

        KDTreeNode(final Storage aStorage, final T aObject) {
            super(aStorage);

            myObject = aObject;
        }

        @SuppressWarnings("unused")
        private KDTreeNode() {
        }

        @Override
        public void load() {
            super.load();

            getStorage().load(myObject);
        }

        @Override
        public boolean recursiveLoading() {
            return false;
        }

        @SuppressWarnings("unchecked")
        int insert(final T aInsert, final MultidimensionalComparator<T> aComparator, final int aLevel) {
            load();

            final int diff = aComparator.compare(aInsert, myObject, aLevel % aComparator.getNumberOfDimensions());

            if (diff == MultidimensionalComparator.EQ && isDeleted) {
                getStorage().deallocate(myObject);
                modify();
                myObject = aInsert;
                isDeleted = false;

                return aLevel;
            } else if (diff != MultidimensionalComparator.GT) {
                if (myLeftNode == null) {
                    modify();
                    myLeftNode = new KDTreeNode<>(getStorage(), aInsert);

                    return aLevel + 1;
                } else {
                    return myLeftNode.insert(aInsert, aComparator, aLevel + 1);
                }
            } else {
                if (myRightNode == null) {
                    modify();
                    myRightNode = new KDTreeNode<>(getStorage(), aInsert);

                    return aLevel + 1;
                } else {
                    return myRightNode.insert(aInsert, aComparator, aLevel + 1);
                }
            }
        }

        @SuppressWarnings("unchecked")
        int remove(final T aRemove, final MultidimensionalComparator<T> aComparator, final int aLevel) {
            load();

            if (myObject == aRemove) {
                if (myLeftNode == null && myRightNode == null) {
                    deallocate();

                    return TRUNCATE;
                } else {
                    modify();
                    myObject = aComparator.cloneField(myObject, aLevel % aComparator.getNumberOfDimensions());
                    isDeleted = true;

                    return OK;
                }
            }

            final int diff = aComparator.compare(aRemove, myObject, aLevel % aComparator.getNumberOfDimensions());

            if (diff != MultidimensionalComparator.GT && myLeftNode != null) {
                final int result = myLeftNode.remove(aRemove, aComparator, aLevel + 1);

                if (result == TRUNCATE) {
                    modify();
                    myLeftNode = null;

                    return OK;
                } else if (result == OK) {
                    return OK;
                }
            }

            if (diff != MultidimensionalComparator.LT && myRightNode != null) {
                final int result = myRightNode.remove(aRemove, aComparator, aLevel + 1);

                if (result == TRUNCATE) {
                    modify();
                    myRightNode = null;

                    return OK;
                } else if (result == OK) {
                    return OK;
                }
            }

            return NOT_FOUND;
        }

        @Override
        public void deallocate() {
            load();

            if (isDeleted) {
                getStorage().deallocate(myObject);
            }

            if (myLeftNode != null) {
                myLeftNode.deallocate();
            }

            if (myRightNode != null) {
                myRightNode.deallocate();
            }

            super.deallocate();
        }
    }
}
