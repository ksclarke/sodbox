
package info.freelibrary.sodbox;

import java.util.Date;

import info.freelibrary.sodbox.impl.ClassDescriptor;

/**
 * Class for specifying key value (needed to access object by key using index).
 */
public class Key {

    public final int myType;

    public final int myIntValue;

    public final long myLongValue;

    public final double myDoubleValue;

    public final Object myObjectValue;

    public final int myInclusion;

    /**
     * Constructor of boolean key (boundary is inclusive).
     */
    public Key(final boolean aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of byte key (boundary is inclusive).
     */
    public Key(final byte aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of char key (boundary is inclusive).
     */
    public Key(final char aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of short key (boundary is inclusive).
     */
    public Key(final short aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of int key (boundary is inclusive).
     */
    public Key(final int aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of long key (boundary is inclusive).
     */
    public Key(final long aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of float key (boundary is inclusive).
     */
    public Key(final float aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of double key (boundary is inclusive).
     */
    public Key(final double aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of date key (boundary is inclusive).
     */
    public Key(final Date aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of string key (boundary is inclusive).
     */
    public Key(final String aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of array of char key (boundary is inclusive).
     */
    public Key(final char[] aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of array of byte key (boundary is inclusive).
     */
    public Key(final byte[] aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of key of user defined type (boundary is inclusive).
     *
     * @param aValue user defined value
     */
    public Key(final IValue aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of key of Enum type (boundary is inclusive).
     *
     * @param aValue array of key of Enum type
     */
    public Key(final Enum<?> aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of compound key (boundary is inclusive).
     *
     * @param aValue array of compound key values
     */
    public Key(final Object[] aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of compound key with two values (boundary is inclusive).
     *
     * @param a1stValue first value of compound key
     * @param a2ndValue second value of compound key
     */
    public Key(final Object a1stValue, final Object a2ndValue) {
        this(new Object[] { a1stValue, a2ndValue }, true);
    }

    /**
     * Constructor of key with persistent object reference (boundary is inclusive).
     */
    public Key(final Object aValue) {
        this(aValue, true);
    }

    /**
     * Constructor of key with persistent object reference (boundary is inclusive).
     */
    public Key(final IPersistent aValue) {
        this(aValue, true);
    }

    private Key(final int aType, final long aLongvalue, final double aDoubleValue, final Object aObjValue,
            final boolean aInclusive) {
        myType = aType;
        myIntValue = (int) aLongvalue;
        myLongValue = aLongvalue;
        myDoubleValue = aDoubleValue;
        myObjectValue = aObjValue;
        myInclusion = aInclusive ? 1 : 0;
    }

    /**
     * Constructor of boolean key.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final boolean aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_BOOLEAN, aValue ? 1 : 0, 0.0, null, aInclusive);
    }

    /**
     * Constructor of byte key.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final byte aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_BYTE, aValue, 0.0, null, aInclusive);
    }

    /**
     * Constructor of char key.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final char aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_CHAR, aValue, 0.0, null, aInclusive);
    }

    /**
     * Constructor of short key.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final short aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_SHORT, aValue, 0.0, null, aInclusive);
    }

    /**
     * Constructor of int key.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final int aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_INT, aValue, 0.0, null, aInclusive);
    }

    /**
     * Constructor of long key.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final long aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_LONG, aValue, 0.0, null, aInclusive);
    }

    /**
     * Constructor of float key.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final float aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_FLOAT, 0, aValue, null, aInclusive);
    }

    /**
     * Constructor of double key.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final double aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_DOUBLE, 0, aValue, null, aInclusive);
    }

    /**
     * Constructor of date key.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final Date aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_DATE, aValue == null ? -1 : aValue.getTime(), 0.0, null, aInclusive);
    }

    /**
     * Constructor of string key.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final String aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_STRING, 0, 0.0, aValue, aInclusive);
    }

    /**
     * Constructor of array of char key.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final char[] aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_STRING, 0, 0.0, aValue, aInclusive);
    }

    /**
     * Constructor of key with persistent object reference.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final IPersistent aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_OBJECT, aValue == null ? 0 : aValue.getOid(), 0.0, aValue, aInclusive);
    }

    /**
     * Constructor of key with persistent object reference.
     *
     * @param aValue key value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final Object aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_OBJECT, 0, 0.0, aValue, aInclusive);
    }

    /**
     * Constructor of key with persistent object reference.
     *
     * @param aValue key value
     * @param aOID object identifier
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final Object aValue, final int aOID, final boolean aInclusive) {
        this(ClassDescriptor.TP_OBJECT, aOID, 0.0, aValue, aInclusive);
    }

    /**
     * Constructor of compound key.
     *
     * @param aValue array of key values
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final Object[] aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_ARRAY_OF_OBJECTS, 0, 0.0, aValue, aInclusive);
    }

    /**
     * Constructor of key of used defined type.
     *
     * @param aValue user defined value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final IValue aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_VALUE, 0, 0.0, aValue, aInclusive);
    }

    /**
     * Constructor of key of enum type.
     *
     * @param aValue enum value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final Enum<?> aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_ENUM, aValue.ordinal(), 0.0, aValue, aInclusive);
    }

    /**
     * Constructor of compound key with two values.
     *
     * @param a1stValue first value of compound key
     * @param a2ndValue second value of compound key
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final Object a1stValue, final Object a2ndValue, final boolean aInclusive) {
        this(new Object[] { a1stValue, a2ndValue }, aInclusive);
    }

    /**
     * Constructor of byte array key.
     *
     * @param aValue byte array value
     * @param aInclusive whether boundary is inclusive or exclusive
     */
    public Key(final byte[] aValue, final boolean aInclusive) {
        this(ClassDescriptor.TP_ARRAY_OF_BYTES, 0, 0.0, aValue, aInclusive);
    }

    @Override
    public int hashCode() {
        final long temp = Double.doubleToLongBits(myDoubleValue);
        final int prime = 31;
        int result = 1;

        result = prime * result + (int) (temp ^ temp >>> 32);
        result = prime * result + myIntValue;
        result = prime * result + (int) (myLongValue ^ myLongValue >>> 32);
        result = prime * result + (myObjectValue == null ? 0 : myObjectValue.hashCode());
        result = prime * result + myType;

        return result;
    }

    @Override
    public boolean equals(final Object aObject) {
        if (aObject instanceof Key) {
            final Key key = (Key) aObject;
            return key.myType == myType && key.myIntValue == myIntValue && key.myLongValue == myLongValue &&
                    key.myDoubleValue == myDoubleValue & key.myObjectValue == myObjectValue;
        }

        return false;
    }

}
