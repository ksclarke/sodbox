
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Field;

import info.freelibrary.sodbox.MultidimensionalComparator;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

/**
 * Implementation of multidimensional reflection comparator using reflection
 */
public class ReflectionMultidimensionalComparator<T> extends MultidimensionalComparator<T> {

    private static boolean isZero(final Object val) {
        return val instanceof Double || val instanceof Float ? ((Number) val).doubleValue() == 0.0
                : val instanceof Number ? ((Number) val).longValue() == 0 : false;
    }

    private String className;

    private String[] fieldNames;

    private boolean treateZeroAsUndefinedValue;

    transient private Class cls;

    transient private Field[] fields;

    transient private ClassDescriptor desc;

    ReflectionMultidimensionalComparator() {
    }

    public ReflectionMultidimensionalComparator(final Storage storage, final Class cls, final String[] fieldNames,
            final boolean treateZeroAsUndefinedValue) {
        super(storage);
        this.cls = cls;
        this.fieldNames = fieldNames;
        this.treateZeroAsUndefinedValue = treateZeroAsUndefinedValue;
        className = ClassDescriptor.getClassName(cls);
        locateFields();
    }

    @Override
    public T cloneField(final T obj, final int i) {
        if (desc == null) {
            desc = ((StorageImpl) getStorage()).findClassDescriptor(cls);
        }
        final T clone = (T) desc.newInstance();
        try {
            fields[i].set(clone, fields[i].get(obj));
            return clone;
        } catch (final IllegalAccessException x) {
            throw new IllegalAccessError();
        }
    }

    @Override
    public int compare(final T m1, final T m2, final int i) {
        try {
            final Comparable c1 = (Comparable) fields[i].get(m1);
            final Comparable c2 = (Comparable) fields[i].get(m2);
            if (c1 == null && c2 == null) {
                return EQ;
            } else if (c1 == null || treateZeroAsUndefinedValue && isZero(c1)) {
                return LEFT_UNDEFINED;
            } else if (c2 == null || treateZeroAsUndefinedValue && isZero(c2)) {
                return RIGHT_UNDEFINED;
            } else {
                final int diff = c1.compareTo(c2);
                return diff < 0 ? LT : diff == 0 ? EQ : GT;
            }
        } catch (final IllegalAccessException x) {
            throw new IllegalAccessError();
        }
    }

    @Override
    public int getNumberOfDimensions() {
        return fields.length;
    }

    private final void locateFields() {
        if (fieldNames == null) {
            fields = cls.getDeclaredFields();
        } else {
            fields = new Field[fieldNames.length];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = ClassDescriptor.locateField(cls, fieldNames[i]);
                if (fields[i] == null) {
                    throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldNames[i]);
                }
            }
        }
    }

    @Override
    public void onLoad() {
        cls = ClassDescriptor.loadClass(getStorage(), className);
        locateFields();
    }
}
