
package info.freelibrary.sodbox;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * Interface of object spatial index. Spatial index is used to allow fast selection of spatial objects belonging to
 * the specified rectangle. Spatial index is implemented using Guttman R-Tree with quadratic split algorithm.
 */
public interface SpatialIndexR2<T> extends IPersistent, IResource {

    /**
     * Find all objects located in the selected rectangle.
     *
     * @param aRectangle selected rectangle
     * @return array of objects which enveloping rectangle intersects with specified rectangle
     */
    Object[] get(RectangleR2 aRectangle);

    /**
     * Find all objects located in the selected rectangle.
     *
     * @param aRectangle selected rectangle
     * @return array list of objects which enveloping rectangle intersects with specified rectangle
     */
    ArrayList<T> getList(RectangleR2 aRectangle);

    /**
     * Put new object in the index.
     *
     * @param aRectangle enveloping rectangle for the object
     * @param aObject object associated with this rectangle. Object can be not yet persistent, in this case its forced
     *        to become persistent by assigning OID to it.
     */
    void put(RectangleR2 aRectangle, T aObject);

    /**
     * Remove object with specified enveloping rectangle from the tree.
     *
     * @param aRectangle enveloping rectangle for the object
     * @param aObject object removed from the index
     * @throws StorageError KEY_NOT_FOUND exception if there is no such key in the index
     */
    void remove(RectangleR2 aRectangle, T aObject);

    /**
     * Get wrapping rectangle.
     *
     * @return minimal rectangle containing all rectangles in the index, <code>null</code> if index is empty
     */
    RectangleR2 getWrappingRectangle();

    /**
     * Get iterator through all members of the index This iterator doesn't support remove() method. It is not possible
     * to modify spatial index during iteration.
     *
     * @return iterator through all objects in the index
     */
    Iterator<T> iterator();

    /**
     * Get entry iterator through all members of the index This iterator doesn't support remove() method. It is not
     * possible to modify spatial index during iteration.
     *
     * @return entry iterator which key specifies rectangle and value - correspondent object
     */
    IterableIterator<Map.Entry<RectangleR2, T>> entryIterator();

    /**
     * Get objects which rectangle intersects with specified rectangle This iterator doesn't support remove() method.
     * It is not possible to modify spatial index during iteration.
     *
     * @param aRectangle selected rectangle
     * @return iterator for objects which enveloping rectangle overlaps with specified rectangle
     */
    IterableIterator<T> iterator(RectangleR2 aRectangle);

    /**
     * Get entry iterator through objects which rectangle intersects with specified rectangle This iterator doesn't
     * support remove() method. It is not possible to modify spatial index during iteration.
     *
     * @param aRectangle selected rectangle
     * @return entry iterator for objects which enveloping rectangle overlaps with specified rectangle
     */
    IterableIterator<Map.Entry<RectangleR2, T>> entryIterator(RectangleR2 aRectangle);

    /**
     * Get iterator through all neighbors of the specified point in the order of increasing distance from the
     * specified point to the wrapper rectangle of the object.
     *
     * @param aX x coordinate of the point
     * @param aY y coordinate of the point
     * @return iterator through all objects in the index in the order of increasing distance from the specified point
     */
    IterableIterator<T> neighborIterator(double aX, double aY);

}
