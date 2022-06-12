
package info.freelibrary.sodbox;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Class use to project selected objects using relation field. For all selected objects (specified by array or
 * iterator), value of specified field (of IPersistent, array of IPersistent, Link or Relation type) is inspected and
 * all referenced object for projection (duplicate values are eliminated).
 */
public class Projection<From, To> extends HashSet<To> {

    private static final long serialVersionUID = -745192931423112566L;

    private Field myField;

    /**
     * Constructor of projection specified by class and field name of projected objects.
     *
     * @param aType base class for selected objects
     * @param aFieldName field name used to perform projection
     */
    public Projection(final Class aType, final String aFieldName) {
        setProjectionField(aType, aFieldName);
    }

    /**
     * Default constructor of projection. This constructor should be used only when you are going to derive your class
     * from Projection and redefine map method in it or specify type and fieldName later using setProjectionField
     * method.
     */
    public Projection() {
    }

    /**
     * Specify class of the projected objects and projection field name.
     *
     * @param aType base class for selected objects
     * @param aFieldName field name used to perform projection
     */
    public void setProjectionField(final Class aType, final String aFieldName) {
        try {
            myField = aType.getDeclaredField(aFieldName);
            myField.setAccessible(true);
        } catch (final Exception x) {
            throw new StorageError(StorageError.KEY_NOT_FOUND, x);
        }
    }

    /**
     * Project specified selection.
     *
     * @param aSelection array with selected object
     */
    public void project(final From[] aSelection) {
        for (int i = 0; i < aSelection.length; i++) {
            map(aSelection[i]);
        }
    }

    /**
     * Project specified object.
     *
     * @param aObject selected object
     */
    public void project(final From aObject) {
        map(aObject);
    }

    /**
     * Project specified selection.
     *
     * @param aSelection iterator specifying selected objects
     */
    public void project(final Iterator<From> aSelection) {
        while (aSelection.hasNext()) {
            map(aSelection.next());
        }
    }

    /**
     * Project specified selection.
     *
     * @param aCollection selection iterator specifying selected objects
     */
    public void project(final Collection<From> aCollection) {
        for (final From o : aCollection) {
            map(o);
        }
    }

    /**
     * Join this projection with another projection. Result of this join is set of objects present in both
     * projections.
     */
    public void join(final Projection<From, To> aProjection) {
        retainAll(aProjection);
    }

    /**
     * Reset projection - clear result of preceding project and join operations.
     */
    public void reset() {
        clear();
    }

    /**
     * Add object to the set.
     *
     * @param aObject object to be added
     */
    @Override
    public boolean add(final To aObject) {
        if (aObject != null) {
            return super.add(aObject);
        }

        return false;
    }

    /**
     * Get related objects for the object aObject. It is possible to redefine this method in derived classes to
     * provide application specific mapping.
     *
     * @param aObject A object from the selection
     */
    @SuppressWarnings("unchecked")
    protected void map(final From aObject) {
        if (myField == null) {
            add((To) aObject);
        } else {
            try {
                final Object o = myField.get(aObject);

                if (o instanceof Link) {
                    final Object[] arr = ((Link) o).toArray();

                    for (int i = 0; i < arr.length; i++) {
                        add((To) arr[i]);
                    }
                } else if (o instanceof Object[]) {
                    final Object[] arr = (Object[]) o;

                    for (int i = 0; i < arr.length; i++) {
                        add((To) arr[i]);
                    }
                } else {
                    add((To) o);
                }
            } catch (final Exception x) {
                throw new StorageError(StorageError.ACCESS_VIOLATION, x);
            }
        }
    }

}
