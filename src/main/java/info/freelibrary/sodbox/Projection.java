
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

    private Field field;

    /**
     * Default constructor of projection. This constructor should be used only when you are going to derive your class
     * from Projection and redefine map method in it or specify type and fieldName later using setProjectionField
     * method.
     */
    public Projection() {
    }

    /**
     * Constructor of projection specified by class and field name of projected objects.
     *
     * @param type base class for selected objects
     * @param fieldName field name used to perform projection
     */
    public Projection(final Class type, final String fieldName) {
        setProjectionField(type, fieldName);
    }

    /**
     * Add object to the set.
     *
     * @param obj object to be added
     */
    @Override
    public boolean add(final To obj) {
        if (obj != null) {
            return super.add(obj);
        }

        return false;
    }

    /**
     * Join this projection with another projection. Result of this join is set of objects present in both
     * projections.
     */
    public void join(final Projection<From, To> prj) {
        retainAll(prj);
    }

    /**
     * Get related objects for the object aObject. It is possible to redefine this method in derived classes to
     * provide application specific mapping.
     *
     * @param aObject A object from the selection
     */
    @SuppressWarnings("unchecked")
    protected void map(final From aObject) {
        if (field == null) {
            add((To) aObject);
        } else {
            try {
                final Object o = field.get(aObject);

                if (o instanceof Link) {
                    final Object[] arr = ((Link) o).toArray();

                    for (final Object element : arr) {
                        add((To) element);
                    }
                } else if (o instanceof Object[]) {
                    final Object[] arr = (Object[]) o;

                    for (final Object element : arr) {
                        add((To) element);
                    }
                } else {
                    add((To) o);
                }
            } catch (final Exception x) {
                throw new StorageError(StorageError.ACCESS_VIOLATION, x);
            }
        }
    }

    /**
     * Project specified selection.
     *
     * @param c selection iterator specifying selected objects
     */
    public void project(final Collection<From> c) {
        for (final From o : c) {
            map(o);
        }
    }

    /**
     * Project specified object.
     *
     * @param obj selected object
     */
    public void project(final From obj) {
        map(obj);
    }

    /**
     * Project specified selection.
     *
     * @param selection array with selected object
     */
    public void project(final From[] selection) {
        for (final From element : selection) {
            map(element);
        }
    }

    /**
     * Project specified selection.
     *
     * @param selection iterator specifying selected objects
     */
    public void project(final Iterator<From> selection) {
        while (selection.hasNext()) {
            map(selection.next());
        }
    }

    /**
     * Reset projection - clear result of preceding project and join operations.
     */
    public void reset() {
        clear();
    }

    /**
     * Specify class of the projected objects and projection field name.
     *
     * @param type base class for selected objects
     * @param fieldName field name used to perform projection
     */
    public void setProjectionField(final Class type, final String fieldName) {
        try {
            field = type.getDeclaredField(fieldName);
            field.setAccessible(true);
        } catch (final Exception x) {
            throw new StorageError(StorageError.KEY_NOT_FOUND, x);
        }
    }

}
