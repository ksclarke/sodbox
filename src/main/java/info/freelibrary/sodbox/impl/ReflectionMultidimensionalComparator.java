
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Field;

import info.freelibrary.sodbox.MultidimensionalComparator;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

/**
 * Implementation of multidimensional reflection comparator using reflection
 */
public class ReflectionMultidimensionalComparator<T> extends MultidimensionalComparator<T> {

    private String myClassName;

    private String[] myFieldNames;

    private boolean hasUndefinedZeroValue;

    private transient Class myClass;

    private transient Field[] myFields;

    private transient ClassDescriptor myClassDescriptor;

    /**
     * Creates a multidimensional comparator.
     *
     * @param aStorage A storage
     * @param aClass A class
     * @param aFieldNames The class' field names
     * @param aZeroHasUndefinedValue If a zero has an undefined value
     */
    public ReflectionMultidimensionalComparator(final Storage aStorage, final Class aClass,
            final String[] aFieldNames, final boolean aZeroHasUndefinedValue) {
        super(aStorage);

        myClass = aClass;
        myFieldNames = aFieldNames;
        hasUndefinedZeroValue = aZeroHasUndefinedValue;
        myClassName = ClassDescriptor.getClassName(aClass);

        locateFields();
    }

    ReflectionMultidimensionalComparator() {
    }

    @Override
    public void onLoad() {
        myClass = ClassDescriptor.loadClass(getStorage(), myClassName);
        locateFields();
    }

    private void locateFields() {
        if (myFieldNames == null) {
            myFields = myClass.getDeclaredFields();
        } else {
            myFields = new Field[myFieldNames.length];

            for (int index = 0; index < myFields.length; index++) {
                myFields[index] = ClassDescriptor.locateField(myClass, myFieldNames[index]);

                if (myFields[index] == null) {
                    throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, myClassName + "." +
                            myFieldNames[index]);
                }
            }
        }
    }

    private static boolean isZero(final Object aValue) {
        return aValue instanceof Double || aValue instanceof Float ? ((Number) aValue).doubleValue() == 0.0
                : aValue instanceof Number ? ((Number) aValue).longValue() == 0 : false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compare(final T aFirstMember, final T aSecondMember, final int aIndex) {
        try {
            final Comparable aFirstComparable = (Comparable) myFields[aIndex].get(aFirstMember);
            final Comparable aSecondComparable = (Comparable) myFields[aIndex].get(aSecondMember);

            if (aFirstComparable == null && aSecondComparable == null) {
                return EQ;
            } else if (aFirstComparable == null || hasUndefinedZeroValue && isZero(aFirstComparable)) {
                return LEFT_UNDEFINED;
            } else if (aSecondComparable == null || hasUndefinedZeroValue && isZero(aSecondComparable)) {
                return RIGHT_UNDEFINED;
            } else {
                final int diff = aFirstComparable.compareTo(aSecondComparable);

                return diff < 0 ? LT : diff == 0 ? EQ : GT;
            }
        } catch (final IllegalAccessException details) {
            throw new IllegalAccessError();
        }
    }

    @Override
    public int getNumberOfDimensions() {
        return myFields.length;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T cloneField(final T aObject, final int aIndex) {
        if (myClassDescriptor == null) {
            myClassDescriptor = ((StorageImpl) getStorage()).findClassDescriptor(myClass);
        }

        final T clone = (T) myClassDescriptor.newInstance();

        try {
            myFields[aIndex].set(clone, myFields[aIndex].get(aObject));
            return clone;
        } catch (final IllegalAccessException details) {
            throw new IllegalAccessError();
        }
    }

}
