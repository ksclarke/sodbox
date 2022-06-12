
package info.freelibrary.sodbox;

/**
 * Base class for multidimensional persistent comparator used in multidimensional index.
 */
public abstract class MultidimensionalComparator<T> extends Persistent {

    public static final int LEFT_UNDEFINED = -2;

    public static final int LT = -1;

    public static final int EQ = 0;

    public static final int GT = 1;

    public static final int RIGHT_UNDEFINED = 2;

    public static final int NE = 3;

    protected MultidimensionalComparator(final Storage aStorage) {
        super(aStorage);
    }

    protected MultidimensionalComparator() {
    }

    /**
     * Compare i-th component of two objects.
     *
     * @param a1stObject first object
     * @param a2ndObject second object
     * @param aIndex component index
     * @return LEFT_UNDEFINED if value of i-th component of the first object is null and value of i-th component of
     *         the second object is not null, RIGHT_UNDEFINED if value of i-th component of the first object is not
     *         null and value of i-th component of the second object is null, EQ if both values are null, otherwise
     *         LT, EQ or GT depending on result of their comparison
     */
    public abstract int compare(T a1stObject, T a2ndObject, int aIndex);

    /**
     * Get number of dimensions.
     *
     * @return number of dimensions
     */
    public abstract int getNumberOfDimensions();

    /**
     * Create clone of the specified object containing a copy of the specified field.
     *
     * @param aObject original object
     * @param aIndex component index
     * @return clone of the object
     */
    public abstract T cloneField(T aObject, int aIndex);

}
