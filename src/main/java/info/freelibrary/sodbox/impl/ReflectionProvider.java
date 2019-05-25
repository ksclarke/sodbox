
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

public interface ReflectionProvider {

    /**
     * Gets the default constructor.
     *
     * @param aClass A class for which to get the constructor
     * @return A constructor
     * @throws Exception If there is a problem getting the default constructor
     */
    Constructor getDefaultConstructor(Class aClass) throws Exception;

    /**
     * Sets an integer.
     *
     * @param aField A field to set
     * @param aObject An object with the field to set
     * @param aValue An integer value
     * @throws Exception If there is a problem setting the integer
     */
    void setInt(Field aField, Object aObject, int aValue) throws Exception;

    /**
     * Sets a long.
     *
     * @param aField A field to set
     * @param aObject An object with the field to set
     * @param aValue A long value
     * @throws Exception If there is a problem setting the long
     */
    void setLong(Field aField, Object aObject, long aValue) throws Exception;

    /**
     * Sets a short.
     *
     * @param aField A field to set
     * @param aObject An object with the field to set
     * @param aValue A short value
     * @throws Exception If there is a problem setting the short
     */
    void setShort(Field aField, Object aObject, short aValue) throws Exception;

    /**
     * Sets a character.
     *
     * @param aField A field to set
     * @param aObject An object with the field to set
     * @param aValue A character value
     * @throws Exception If there is a problem setting the character
     */
    void setChar(Field aField, Object aObject, char aValue) throws Exception;

    /**
     * Sets a byte.
     *
     * @param aField A field to set
     * @param aObject An object with the field to set
     * @param aValue A byte value
     * @throws Exception If there is a problem setting the byte
     */
    void setByte(Field aField, Object aObject, byte aValue) throws Exception;

    /**
     * Sets a float.
     *
     * @param aField A field to set
     * @param aObject An object with the field to set
     * @param aValue A float value
     * @throws Exception If there is a problem setting the float
     */
    void setFloat(Field aField, Object aObject, float aValue) throws Exception;

    /**
     * Sets a double.
     *
     * @param aField A field to set
     * @param aObject An object with the field to set
     * @param aValue A double value
     * @throws Exception If there is a problem setting the double
     */
    void setDouble(Field aField, Object aObject, double aValue) throws Exception;

    /**
     * Sets a boolean.
     *
     * @param aField A field to set
     * @param aObject An object with the field to set
     * @param aValue A double value
     * @throws Exception If there is a problem setting the boolean
     */
    void setBoolean(Field aField, Object aObject, boolean aValue) throws Exception;

    /**
     * Sets a value.
     *
     * @param aField A field to set
     * @param aObject An object with the field to set
     * @param aValue A value
     * @throws Exception If there is a problem setting the value
     */
    void set(Field aField, Object aObject, Object aValue) throws Exception;

}
