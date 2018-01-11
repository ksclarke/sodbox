
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

    public class KDTreeIterator extends IterableIterator<T> implements PersistentIterator {

        Stack<KDTreeNode<T>> stack;

        int nDims;

        T high;

        T low;

        KDTreeNode<T> curr;

        KDTreeNode<T> next;

        int currLevel;

        KDTreeIterator(final T low, final T high) {
            this.low = low;
            this.high = high;
            nDims = comparator.getNumberOfDimensions();
            stack = new Stack<KDTreeNode<T>>();
            getMin(root);
        }

        public int getLevel() {
            return currLevel;
        }

        private boolean getMin(KDTreeNode<T> node) {
            if (node != null) {
                while (true) {
                    node.load();
                    stack.push(node);
                    final int diff = low == null ? MultidimensionalComparator.LEFT_UNDEFINED : comparator.compare(low,
                            node.obj, (stack.size() - 1) % nDims);
                    if (diff != MultidimensionalComparator.GT && node.left != null) {
                        node = node.left;
                    } else {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            while (!stack.empty()) {
                final KDTreeNode<T> node = stack.pop();
                if (node != null) {
                    if (!node.deleted) {
                        int result;
                        if ((low == null || (result = compareAllComponents(low,
                                node.obj)) == MultidimensionalComparator.LT ||
                                result == MultidimensionalComparator.EQ) && (high == null || (result =
                                        compareAllComponents(high, node.obj)) == MultidimensionalComparator.GT ||
                                        result == MultidimensionalComparator.EQ)) {
                            next = node;
                            currLevel = stack.size();
                        }
                    }
                    if (node.right != null && (high == null || comparator.compare(high, node.obj, stack.size() %
                            nDims) != MultidimensionalComparator.LT)) {
                        stack.push(null);
                        if (!getMin(node.right)) {
                            stack.pop();
                        }
                    }
                    if (next != null) {
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
            curr = next;
            next = null;
            return curr.obj;
        }

        @Override
        public int nextOid() {
            if (!hasNext()) {
                return 0;
            }
            curr = next;
            next = null;
            return getStorage().getOid(curr.obj);
        }

        @Override
        public void remove() {
            if (curr == null) {
                throw new IllegalStateException();
            }
            curr.modify();
            curr.obj = comparator.cloneField(curr.obj, currLevel % nDims);
            curr.deleted = true;
            curr = null;
        }
    }

    static class KDTreeNode<T> extends Persistent {

        KDTreeNode left;

        KDTreeNode right;

        T obj;

        boolean deleted;

        KDTreeNode(final Storage db, final T obj) {
            super(db);
            this.obj = obj;
        }

        @Override
        public void deallocate() {
            load();
            if (deleted) {
                getStorage().deallocate(obj);
            }
            if (left != null) {
                left.deallocate();
            }
            if (right != null) {
                right.deallocate();
            }
            super.deallocate();
        }

        int insert(final T ins, final MultidimensionalComparator<T> comparator, final int level) {
            load();
            final int diff = comparator.compare(ins, obj, level % comparator.getNumberOfDimensions());
            if (diff == MultidimensionalComparator.EQ && deleted) {
                getStorage().deallocate(obj);
                modify();
                obj = ins;
                deleted = false;
                return level;
            } else if (diff != MultidimensionalComparator.GT) {
                if (left == null) {
                    modify();
                    left = new KDTreeNode<T>(getStorage(), ins);
                    return level + 1;
                } else {
                    return left.insert(ins, comparator, level + 1);
                }
            } else {
                if (right == null) {
                    modify();
                    right = new KDTreeNode<T>(getStorage(), ins);
                    return level + 1;
                } else {
                    return right.insert(ins, comparator, level + 1);
                }
            }
        }

        @Override
        public void load() {
            super.load();
            getStorage().load(obj);
        }

        @Override
        public boolean recursiveLoading() {
            return false;
        }

        int remove(final T rem, final MultidimensionalComparator<T> comparator, final int level) {
            load();
            if (obj == rem) {
                if (left == null && right == null) {
                    deallocate();
                    return TRUNCATE;
                } else {
                    modify();
                    obj = comparator.cloneField(obj, level % comparator.getNumberOfDimensions());
                    deleted = true;
                    return OK;
                }
            }
            final int diff = comparator.compare(rem, obj, level % comparator.getNumberOfDimensions());
            if (diff != MultidimensionalComparator.GT && left != null) {
                final int result = left.remove(rem, comparator, level + 1);
                if (result == TRUNCATE) {
                    modify();
                    left = null;
                    return OK;
                } else if (result == OK) {
                    return OK;
                }
            }
            if (diff != MultidimensionalComparator.LT && right != null) {
                final int result = right.remove(rem, comparator, level + 1);
                if (result == TRUNCATE) {
                    modify();
                    right = null;
                    return OK;
                } else if (result == OK) {
                    return OK;
                }
            }
            return NOT_FOUND;
        }
    }

    static final int OK = 0;

    static final int NOT_FOUND = 1;

    static final int TRUNCATE = 2;

    KDTreeNode root;

    int nMembers;

    int height;

    MultidimensionalComparator<T> comparator;

    KDTree(final Storage storage, final Class cls, final String[] fieldNames,
            final boolean treateZeroAsUndefinedValue) {
        super(storage);
        this.comparator = new ReflectionMultidimensionalComparator<T>(storage, cls, fieldNames,
                treateZeroAsUndefinedValue);
    }

    KDTree(final Storage storage, final MultidimensionalComparator<T> comparator) {
        super(storage);
        this.comparator = comparator;
    }

    @Override
    public boolean add(final T obj) {
        modify();
        if (root == null) {
            root = new KDTreeNode<T>(getStorage(), obj);
            height = 1;
        } else {
            final int level = root.insert(obj, comparator, 0);
            if (level >= height) {
                height = level + 1;
            }
        }
        nMembers += 1;
        return true;
    }

    @Override
    public void clear() {
        if (root != null) {
            root.deallocate();
            modify();
            root = null;
            nMembers = 0;
            height = 0;
        }
    }

    int compareAllComponents(final T pattern, final T obj) {
        final int n = comparator.getNumberOfDimensions();
        int result = MultidimensionalComparator.EQ;
        for (int i = 0; i < n; i++) {
            final int diff = comparator.compare(pattern, obj, i);
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

    @Override
    public boolean contains(final Object member) {
        final Iterator<T> i = iterator((T) member);
        while (i.hasNext()) {
            if (i.next() == member) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void deallocate() {
        if (root != null) {
            root.deallocate();
        }
        super.deallocate();
    }

    @Override
    public MultidimensionalComparator<T> getComparator() {
        return comparator;
    }

    @Override
    public int getHeight() {
        return height;
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(null, null);
    }

    @Override
    public IterableIterator<T> iterator(final T pattern) {
        return iterator(pattern, pattern);
    }

    @Override
    public IterableIterator<T> iterator(final T low, final T high) {
        return new KDTreeIterator(low, high);
    }

    @Override
    public void optimize() {
        final Iterator<T> itr = iterator();
        final int n = nMembers;
        final Object[] members = new Object[n];
        for (int i = 0; i < n; i++) {
            members[i] = itr.next();
        }
        final Random rnd = new Random();
        for (int i = 0; i < n; i++) {
            final int j = rnd.nextInt(n);
            final Object tmp = members[j];
            members[j] = members[i];
            members[i] = tmp;
        }
        clear();
        for (int i = 0; i < n; i++) {
            add((T) members[i]);
        }
    }

    @Override
    public ArrayList<T> queryByExample(final T pattern) {
        return queryByExample(pattern, pattern);
    }

    @Override
    public ArrayList<T> queryByExample(final T low, final T high) {
        final Iterator<T> i = iterator(low, high);
        final ArrayList<T> list = new ArrayList<T>();
        while (i.hasNext()) {
            list.add(i.next());
        }
        return list;
    }

    @Override
    public boolean remove(final Object obj) {
        if (root == null) {
            return false;
        }
        final int result = root.remove(obj, comparator, 0);
        if (result == NOT_FOUND) {
            return false;
        }
        modify();
        if (result == TRUNCATE) {
            root = null;
        }
        nMembers -= 1;
        return true;
    }

    @Override
    public int size() {
        return nMembers;
    }

    @Override
    public Object[] toArray() {
        return queryByExample(null, null).toArray();
    }

    @Override
    public <E> E[] toArray(final E[] arr) {
        return queryByExample(null, null).toArray(arr);
    }
}
