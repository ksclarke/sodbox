
package info.freelibrary.sodbox;

import info.freelibrary.sodbox.impl.ClassDescriptor;

/**
 * Class for specifying key value (needed to access object by key using index).
 */
public class Key {

    public final int type;

    public final int ival;

    public final long lval;

    public final double dval;

    public final Object oval;

    public final int inclusion;

    /**
     * Constructor of boolean key (boundary is inclusive).
     */
    public Key(final boolean v) {
        this(v, true);
    }

    /**
     * Constructor of boolean key.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final boolean v, final boolean inclusive) {
        this(ClassDescriptor.tpBoolean, v ? 1 : 0, 0.0, null, inclusive);
    }

    /**
     * Constructor of byte key (boundary is inclusive).
     */
    public Key(final byte v) {
        this(v, true);
    }

    /**
     * Constructor of byte key.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final byte v, final boolean inclusive) {
        this(ClassDescriptor.tpByte, v, 0.0, null, inclusive);
    }

    /**
     * Constructor of array of byte key (boundary is inclusive).
     */
    public Key(final byte[] v) {
        this(v, true);
    }

    /**
     * Constructor of byte array key.
     *
     * @param v byte array value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final byte[] v, final boolean inclusive) {
        this(ClassDescriptor.tpArrayOfByte, 0, 0.0, v, inclusive);
    }

    /**
     * Constructor of char key (boundary is inclusive).
     */
    public Key(final char v) {
        this(v, true);
    }

    /**
     * Constructor of char key.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final char v, final boolean inclusive) {
        this(ClassDescriptor.tpChar, v, 0.0, null, inclusive);
    }

    /**
     * Constructor of array of char key (boundary is inclusive).
     */
    public Key(final char[] v) {
        this(v, true);
    }

    /**
     * Constructor of array of char key.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final char[] v, final boolean inclusive) {
        this(ClassDescriptor.tpString, 0, 0.0, v, inclusive);
    }

    /**
     * Constructor of double key (boundary is inclusive).
     */
    public Key(final double v) {
        this(v, true);
    }

    /**
     * Constructor of double key.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final double v, final boolean inclusive) {
        this(ClassDescriptor.tpDouble, 0, v, null, inclusive);
    }

    /**
     * Constructor of key of Enum type (boundary is inclusive).
     *
     * @param v array of key of Enum type
     */
    public Key(final Enum<?> v) {
        this(v, true);
    }

    /**
     * Constructor of key of enum type.
     *
     * @param v enum value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final Enum<?> v, final boolean inclusive) {
        this(ClassDescriptor.tpEnum, v.ordinal(), 0.0, v, inclusive);
    }

    /**
     * Constructor of float key (boundary is inclusive).
     */
    public Key(final float v) {
        this(v, true);
    }

    /**
     * Constructor of float key.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final float v, final boolean inclusive) {
        this(ClassDescriptor.tpFloat, 0, v, null, inclusive);
    }

    /**
     * Constructor of int key (boundary is inclusive).
     */
    public Key(final int v) {
        this(v, true);
    }

    /**
     * Constructor of int key.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final int v, final boolean inclusive) {
        this(ClassDescriptor.tpInt, v, 0.0, null, inclusive);
    }

    private Key(final int type, final long lval, final double dval, final Object oval, final boolean inclusive) {
        this.type = type;
        ival = (int) lval;
        this.lval = lval;
        this.dval = dval;
        this.oval = oval;
        inclusion = inclusive ? 1 : 0;
    }

    /**
     * Constructor of key with persistent object reference (boundary is inclusive).
     */
    public Key(final IPersistent v) {
        this(v, true);
    }

    /**
     * Constructor of key with persistent object reference.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final IPersistent v, final boolean inclusive) {
        this(ClassDescriptor.tpObject, v == null ? 0 : v.getOid(), 0.0, v, inclusive);
    }

    /**
     * Constructor of key of user defined type (boundary is inclusive).
     *
     * @param v user defined value
     */
    public Key(final IValue v) {
        this(v, true);
    }

    /**
     * Constructor of key of used defined type.
     *
     * @param v user defined value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final IValue v, final boolean inclusive) {
        this(ClassDescriptor.tpValue, 0, 0.0, v, inclusive);
    }

    /**
     * Constructor of date key (boundary is inclusive).
     */
    public Key(final java.util.Date v) {
        this(v, true);
    }

    /**
     * Constructor of date key.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final java.util.Date v, final boolean inclusive) {
        this(ClassDescriptor.tpDate, v == null ? -1 : v.getTime(), 0.0, null, inclusive);
    }

    /**
     * Constructor of long key (boundary is inclusive).
     */
    public Key(final long v) {
        this(v, true);
    }

    /**
     * Constructor of long key.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final long v, final boolean inclusive) {
        this(ClassDescriptor.tpLong, v, 0.0, null, inclusive);
    }

    /**
     * Constructor of key with persistent object reference (boundary is inclusive).
     */
    public Key(final Object v) {
        this(v, true);
    }

    /**
     * Constructor of key with persistent object reference.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final Object v, final boolean inclusive) {
        this(ClassDescriptor.tpObject, 0, 0.0, v, inclusive);
    }

    /**
     * Constructor of key with persistent object reference.
     *
     * @param v key value
     * @param oid object identifier
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final Object v, final int oid, final boolean inclusive) {
        this(ClassDescriptor.tpObject, oid, 0.0, v, inclusive);
    }

    /**
     * Constructor of compound key with two values (boundary is inclusive).
     *
     * @param v1 first value of compound key
     * @param v2 second value of compound key
     */
    public Key(final Object v1, final Object v2) {
        this(new Object[] { v1, v2 }, true);
    }

    /**
     * Constructor of compound key with two values.
     *
     * @param v1 first value of compound key
     * @param v2 second value of compound key
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final Object v1, final Object v2, final boolean inclusive) {
        this(new Object[] { v1, v2 }, inclusive);
    }

    /**
     * Constructor of compound key (boundary is inclusive).
     *
     * @param v array of compound key values
     */
    public Key(final Object[] v) {
        this(v, true);
    }

    /**
     * Constructor of compound key.
     *
     * @param v array of key values
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final Object[] v, final boolean inclusive) {
        this(ClassDescriptor.tpArrayOfObject, 0, 0.0, v, inclusive);
    }

    /**
     * Constructor of short key (boundary is inclusive).
     */
    public Key(final short v) {
        this(v, true);
    }

    /**
     * Constructor of short key.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final short v, final boolean inclusive) {
        this(ClassDescriptor.tpShort, v, 0.0, null, inclusive);
    }

    /**
     * Constructor of string key (boundary is inclusive).
     */
    public Key(final String v) {
        this(v, true);
    }

    /**
     * Constructor of string key.
     *
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(final String v, final boolean inclusive) {
        this(ClassDescriptor.tpString, 0, 0.0, v, inclusive);
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof Key) {
            final Key key = (Key) o;
            return key.type == type && key.ival == ival && key.lval == lval && key.dval == dval & key.oval == oval;
        }

        return false;
    }

}
