
package info.freelibrary.sodbox;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Interface of multidimensional index. The main idea of this index is to provide efficient search of object using
 * multiple search criteria, for example "select from StockOptions where Symbol between 'AAB' and 'ABC' and Price &gt;
 * 100 and Volume between 1000 and 10000" . Each component of the object is represented as separate dimension in the
 * index.
 */
public interface MultidimensionalIndex<T> extends IPersistent, IResource {

    /**
     * Get comparator used in this index.
     *
     * @return comparator used to compare objects in this index
     */
    MultidimensionalComparator<T> getComparator();

    /**
     * Get iterator for traversing all objects in the index. Objects are iterated in the ascent key order. This
     * iterator supports remove() method.
     *
     * @return index iterator
     */
    Iterator<T> iterator();

    /**
     * Get iterator through objects matching specified pattern object. All fields which are part of multidimensional
     * index and which values in pattern object is not null are used as filter for index members.
     *
     * @param aPattern object which is used as search pattern, non null values of components of this object forms
     *        search condition
     * @return iterator through index members which field values are equal to correspondent non-null fields of pattern
     *         object
     */
    IterableIterator<T> iterator(T aPattern);

    /**
     * Get iterator through objects which field values belongs to the range specified by correspondent fields of low
     * and high objects.
     *
     * @param aLow pattern object specifying inclusive low boundary for field values. If there is no low boundary for
     *        some particular field it should be set to null. For scalar types (like int) you can use instead minimal
     *        possible value, like Integer.MIN_VALUE. If low is null, then low boundary is not specified for all
     *        fields.
     * @param aHigh pattern object specifying inclusive high boundary for field values. If there is no high boundary
     *        for some particular field it should be set to null. For scalar types (like int) you can use instead
     *        maximal possible value, like Integer.MAX_VALUE. If high is null, then high boundary is not specified for
     *        all fields.
     * @return iterator through index members which field values are belongs to the range specified by correspondent
     *         fields of low and high objects
     */
    IterableIterator<T> iterator(T aLow, T aHigh);

    /**
     * Get array of index members matching specified pattern object. All fields which are part of multidimensional
     * index and which values in pattern object is not null are used as filter for index members.
     *
     * @param aPattern object which is used as search pattern, non null values of components of this object forms
     *        search condition
     * @return array of index members which field values are equal to correspondent non-null fields of pattern object
     */
    ArrayList<T> queryByExample(T aPattern);

    /**
     * Get array of index members which field values belongs to the range specified by correspondent fields of low and
     * high objects.
     *
     * @param aLow pattern object specifying inclusive low boundary for field values. If there is no low boundary for
     *        some particular field it should be set to null. For scalar types (like int) you can use instead minimal
     *        possible value, like Integer.MIN_VALUE. If low is null, then low boundary is not specified for all
     *        fields.
     * @param aHigh pattern object specifying inclusive high boundary for field values. If there is no high boundary
     *        for some particular field it should be set to null. For scalar types (like int) you can use instead
     *        maximal possible value, like Integer.MAX_VALUE. If high is null, then high boundary is not specified for
     *        all fields.
     * @return array of index members which field values are belongs to the range specified by correspondent fields of
     *         low and high objects
     */
    ArrayList<T> queryByExample(T aLow, T aHigh);

    /**
     * Optimize index to make search more efficient. This operation cause complete reconstruction of the index and so
     * may take a long time. Also please notice that this method doesn't build the ideally balanced tree - it just
     * reinserts elements in the tree in random order.
     */
    void optimize();

    /**
     * Get height of the tree. Height of the tree can be used by application to determine when tree structure is no
     * optimal and tree should be reconstructed using optimize method.
     *
     * @return height of the tree
     */
    int getHeight();

}
