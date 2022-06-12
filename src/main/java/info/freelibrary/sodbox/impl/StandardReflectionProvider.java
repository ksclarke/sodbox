
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import info.freelibrary.sodbox.Constants;
import info.freelibrary.sodbox.MessageCodes;
import info.freelibrary.util.Logger;
import info.freelibrary.util.LoggerFactory;

public class StandardReflectionProvider implements ReflectionProvider {

    static final Class[] DEFAULT_CONSTRUCTOR_PROFILE = new Class[0];

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardReflectionProvider.class,
            Constants.MESSAGES);

    @Override
    public Constructor getDefaultConstructor(final Class aClass) throws Exception {
        final Constructor constructor = aClass.getDeclaredConstructor(DEFAULT_CONSTRUCTOR_PROFILE);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(MessageCodes.SB_031, aClass.getName());
        }

        return constructor;
    }

    @Override
    public void setInt(final Field aField, final Object aObject, final int aValue) throws Exception {
        aField.setInt(aObject, aValue);
    }

    @Override
    public void setLong(final Field aField, final Object aObj, final long aValue) throws Exception {
        aField.setLong(aObj, aValue);
    }

    @Override
    public void setShort(final Field aField, final Object aObj, final short aValue) throws Exception {
        aField.setShort(aObj, aValue);
    }

    @Override
    public void setChar(final Field aField, final Object aObject, final char aValue) throws Exception {
        aField.setChar(aObject, aValue);
    }

    @Override
    public void setByte(final Field aField, final Object aObj, final byte aValue) throws Exception {
        aField.setByte(aObj, aValue);
    }

    @Override
    public void setFloat(final Field aField, final Object aObj, final float aValue) throws Exception {
        aField.setFloat(aObj, aValue);
    }

    @Override
    public void setDouble(final Field aField, final Object aObj, final double aValue) throws Exception {
        aField.setDouble(aObj, aValue);
    }

    @Override
    public void setBoolean(final Field aField, final Object aObject, final boolean aValue) throws Exception {
        aField.setBoolean(aObject, aValue);
    }

    @Override
    public void set(final Field aField, final Object aObject, final Object aValue) throws Exception {
        aField.set(aObject, aValue);
    }

}
