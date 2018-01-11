
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardReflectionProvider implements ReflectionProvider {

    private final static Logger LOGGER = LoggerFactory.getLogger(StandardReflectionProvider.class);

    @SuppressWarnings("unchecked")
    static final Class[] defaultConstructorProfile = new Class[0];

    @Override
    @SuppressWarnings("unchecked")
    public Constructor getDefaultConstructor(final Class cls) throws Exception {
        final Constructor constructor = cls.getDeclaredConstructor(defaultConstructorProfile);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Getting default constructor for " + cls.getName());
        }

        return constructor;
    }

    @Override
    public void set(final Field field, final Object object, final Object value) throws Exception {
        field.set(object, value);
    }

    @Override
    public void setBoolean(final Field field, final Object object, final boolean value) throws Exception {
        field.setBoolean(object, value);
    }

    @Override
    public void setByte(final Field field, final Object object, final byte value) throws Exception {
        field.setByte(object, value);
    }

    @Override
    public void setChar(final Field field, final Object object, final char value) throws Exception {
        field.setChar(object, value);
    }

    @Override
    public void setDouble(final Field field, final Object object, final double value) throws Exception {
        field.setDouble(object, value);
    }

    @Override
    public void setFloat(final Field field, final Object object, final float value) throws Exception {
        field.setFloat(object, value);
    }

    @Override
    public void setInt(final Field field, final Object object, final int value) throws Exception {
        field.setInt(object, value);
    }

    @Override
    public void setLong(final Field field, final Object object, final long value) throws Exception {
        field.setLong(object, value);
    }

    @Override
    public void setShort(final Field field, final Object object, final short value) throws Exception {
        field.setShort(object, value);
    }
}