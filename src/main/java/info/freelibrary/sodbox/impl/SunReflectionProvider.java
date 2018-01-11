
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;

import sun.misc.Unsafe;
import sun.reflect.ReflectionFactory;

public class SunReflectionProvider implements ReflectionProvider {

    static final Class[] defaultConstructorProfile = new Class[0];

    private Constructor myCons;

    private Unsafe unsafe;

    private ReflectionFactory factory;

    private HashMap constructorHash;

    public SunReflectionProvider() {
        try {
            final Class osClass = Class.forName("java.io.ObjectStreamClass$FieldReflector");
            final Field unsafeField = osClass.getDeclaredField("unsafe");
            unsafeField.setAccessible(true);
            unsafe = (Unsafe) unsafeField.get(null);
            myCons = Object.class.getDeclaredConstructor(new Class[0]);
            factory = ReflectionFactory.getReflectionFactory();
            constructorHash = new HashMap();
        } catch (final Exception x) {
            throw new Error("Failed to initialize reflection provider");
        }
    }

    @Override
    public Constructor getDefaultConstructor(final Class aClass) throws Exception {
        Constructor cons = (Constructor) constructorHash.get(aClass);

        if (cons == null) {
            try {
                cons = aClass.getDeclaredConstructor(defaultConstructorProfile);
            } catch (final NoSuchMethodException x) {
                cons = factory.newConstructorForSerialization(aClass, myCons);
            }

            constructorHash.put(aClass, cons);
        }

        return cons;
    }

    @Override
    public void set(final Field field, final Object object, final Object value) throws Exception {
        unsafe.putObject(object, unsafe.objectFieldOffset(field), value);
    }

    @Override
    public void setBoolean(final Field field, final Object object, final boolean value) throws Exception {
        unsafe.putBoolean(object, unsafe.objectFieldOffset(field), value);
    }

    @Override
    public void setByte(final Field field, final Object object, final byte value) throws Exception {
        unsafe.putByte(object, unsafe.objectFieldOffset(field), value);
    }

    @Override
    public void setChar(final Field field, final Object object, final char value) throws Exception {
        unsafe.putChar(object, unsafe.objectFieldOffset(field), value);
    }

    @Override
    public void setDouble(final Field field, final Object object, final double value) throws Exception {
        unsafe.putDouble(object, unsafe.objectFieldOffset(field), value);
    }

    @Override
    public void setFloat(final Field field, final Object object, final float value) throws Exception {
        unsafe.putFloat(object, unsafe.objectFieldOffset(field), value);
    }

    @Override
    public void setInt(final Field field, final Object object, final int value) throws Exception {
        unsafe.putInt(object, unsafe.objectFieldOffset(field), value);
    }

    @Override
    public void setLong(final Field field, final Object object, final long value) throws Exception {
        unsafe.putLong(object, unsafe.objectFieldOffset(field), value);
    }

    @Override
    public void setShort(final Field field, final Object object, final short value) throws Exception {
        unsafe.putShort(object, unsafe.objectFieldOffset(field), value);
    }
}