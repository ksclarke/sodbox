
package info.freelibrary.sodbox.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import info.freelibrary.sodbox.Constants;
import info.freelibrary.sodbox.CustomAllocator;
import info.freelibrary.sodbox.CustomSerializable;
import info.freelibrary.sodbox.INamedClassLoader;
import info.freelibrary.sodbox.IPersistent;
import info.freelibrary.sodbox.IValue;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;

public final class ClassDescriptor extends Persistent {

    public static final int TP_BOOLEAN = 0;

    public static final int TP_BYTE = 1;

    public static final int TP_CHAR = 2;

    public static final int TP_SHORT = 3;

    public static final int TP_INT = 4;

    public static final int TP_LONG = 5;

    public static final int TP_FLOAT = 6;

    public static final int TP_DOUBLE = 7;

    public static final int TP_STRING = 8;

    public static final int TP_DATE = 9;

    public static final int TP_OBJECT = 10;

    public static final int TP_VALUE = 11;

    public static final int TP_RAW = 12;

    public static final int TP_LINK = 13;

    public static final int TP_ENUM = 14;

    public static final int TP_CUSTOM = 15;

    public static final int TP_ARRAY_OF_BOOLEANS = 20;

    public static final int TP_ARRAY_OF_BYTES = 21;

    public static final int TP_ARRAY_OF_CHARS = 22;

    public static final int TP_ARRAY_OF_SHORTS = 23;

    public static final int TP_ARRAY_OF_INTS = 24;

    public static final int TP_ARRAY_OF_LONGS = 25;

    public static final int TO_ARRAY_OF_FLOATS = 26;

    public static final int TP_ARRAY_OF_DOUBLES = 27;

    public static final int TP_ARRAY_OF_STRINGS = 28;

    public static final int TP_ARRAY_OF_DATES = 29;

    public static final int TP_ARRAY_OF_OBJECTS = 30;

    public static final int TP_ARRAY_OF_VALUES = 31;

    public static final int TP_ARRAY_OF_RAWS = 32;

    public static final int TP_ARRAY_OF_LINKS = 33; // not supported

    public static final int TP_ARRAY_OF_ENUMS = 34;

    public static final int TP_CLASS = 35;

    public static final int TP_VALUE_TYPE_BIAS = 100;

    static final String ARRAY_OF_ENUM = "ArrayOfEnum";

    static final String SIGNATURE[] = { "boolean", "byte", "char", "short", "int", "long", "float", "double",
        "String", "Date", "Object", "Value", "Raw", "Link", "enum", "", "", "", "", "", "", "ArrayOfBoolean",
        "ArrayOfByte", "ArrayOfChar", "ArrayOfShort", "ArrayOfInt", "ArrayOfLong", "ArrayOfFloat", "ArrayOfDouble",
        ARRAY_OF_ENUM, "ArrayOfString", "ArrayOfDate", "ArrayOfObject", "ArrayOfValue", "ArrayOfRaw", "ArrayOfLink",
        ARRAY_OF_ENUM, "Class" };

    static final int SIZE_OF[] = { 1, // tpBoolean
        1, // tpByte
        2, // tpChar
        2, // tpShort
        4, // tpInt
        8, // tpLong
        4, // tpFloat
        8, // tpDouble
        0, // tpString
        8, // tpDate
        4, // tpObject
        0, // tpValue
        0, // tpRaw
        0, // tpLink
        4 // tpEnum
    };

    static final Class[] SODBOX_CONSTRUCTOR_PROFILE = new Class[] { ClassDescriptor.class };

    static boolean TREAT_ANY_NONPERSISTENT_OBJ_AS_VALUE = Boolean.getBoolean(Constants.IMPLICIT_VALUES);

    static boolean SERIALIZE_NONPERSISTENT_OBJS = Boolean.getBoolean(Constants.SERIALIZE_TRANSIENT_OBJECTS);

    static ReflectionProvider myReflectionProvider;

    static boolean hasReverseMembersOrder;

    ClassDescriptor myNextCD;

    String myName;

    boolean hasReferences;

    FieldDescriptor[] myFields;

    CustomAllocator myAllocator;

    transient Class myClass;

    transient Constructor myLoadConstructor;

    transient LoadFactory myLoadFactory;

    transient Object[] myConstructorParams;

    transient boolean isCustomSerializable;

    transient boolean hasSubclasses;

    transient boolean isResolved;

    transient boolean isCollection;

    transient boolean isMap;

    ClassDescriptor() {
    }

    @SuppressWarnings("unchecked")
    ClassDescriptor(final StorageImpl aStorage, final Class aClass) {
        final ArrayList list = new ArrayList();

        myClass = aClass;
        isCustomSerializable = aStorage.mySerializer != null && aStorage.mySerializer.isApplicable(aClass);
        isCollection = Collection.class.isAssignableFrom(aClass);
        isMap = Map.class.isAssignableFrom(aClass);
        myName = getClassName(aClass);
        buildFieldList(aStorage, aClass, list);
        myFields = (FieldDescriptor[]) list.toArray(new FieldDescriptor[list.size()]);
        locateConstructor();
        isResolved = true;
    }

    static ReflectionProvider getReflectionProvider() {
        if (myReflectionProvider == null) {
            try {
                Class.forName("sun.misc.Unsafe");
                final String cls = "info.freelibrary.sodbox.impl.SunReflectionProvider";
                myReflectionProvider = (ReflectionProvider) Class.forName(cls).newInstance();
            } catch (final Throwable details) {
                myReflectionProvider = new StandardReflectionProvider();
            }
        }
        return myReflectionProvider;
    }

    /**
     * Tests supplied ClassDescriptor for equality.
     *
     * @param aClassDescriptor A ClassDescriptor to test
     * @return True if equals; else, false
     */
    public boolean equals(final ClassDescriptor aClassDescriptor) {
        if (aClassDescriptor == null || myFields.length != aClassDescriptor.myFields.length) {
            return false;
        }
        for (int i = 0; i < myFields.length; i++) {
            if (!myFields[i].equals(aClassDescriptor.myFields[i])) {
                return false;
            }
        }
        return true;
    }

    Object newInstance() {
        if (myLoadFactory != null) {
            return myLoadFactory.create(this);
        } else {
            try {
                return myLoadConstructor.newInstance(myConstructorParams);
            } catch (final Exception x) {
                throw new StorageError(StorageError.CONSTRUCTOR_FAILURE, myClass, x);
            }
        }
    }

    @SuppressWarnings("unchecked")
    void buildFieldList(final StorageImpl aStorage, final Class aClass, final ArrayList aList) {
        final Class superclass = aClass.getSuperclass();

        if (superclass != null) {
            buildFieldList(aStorage, superclass, aList);
        }

        final Field[] fields = aClass.getDeclaredFields();

        if (aStorage.getDatabaseFormatVersion() >= 2) {
            Arrays.sort(fields, (field1, field2) -> field1.getName().compareTo(field2.getName()));
        } else { // preserve backward compatibility
            if (ClassDescriptor.class.equals(aClass)) {
                for (int index = 0; index < fields.length; index++) {
                    if ((fields[index].getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0) {
                        if (!"next".equals(fields[index].getName())) {
                            hasReverseMembersOrder = true;
                        }

                        break;
                    }
                }
            }

            if (hasReverseMembersOrder) {
                for (int index = 0, n = fields.length; index < n >> 1; index++) {
                    final Field field = fields[index];

                    fields[index] = fields[n - index - 1];
                    fields[n - index - 1] = field;
                }
            }
        }

        for (int index = 0; index < fields.length; index++) {
            final Field field = fields[index];

            if (!field.isSynthetic() && (field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0) {
                try {
                    field.setAccessible(true);
                } catch (final Exception details) {
                    // FIXME
                }

                final FieldDescriptor fieldDescriptor = new FieldDescriptor();

                fieldDescriptor.myField = field;
                fieldDescriptor.myFieldName = field.getName();
                fieldDescriptor.myClassName = aClass.getName();

                final int type = getTypeCode(field.getType());

                switch (type) {
                    case TP_OBJECT:
                    case TP_LINK:
                    case TP_ARRAY_OF_OBJECTS:
                        hasReferences = true;
                        break;
                    case TP_VALUE:
                        fieldDescriptor.myClassDescriptor = aStorage.getClassDescriptor(field.getType());
                        hasReferences |= fieldDescriptor.myClassDescriptor.hasReferences;
                        break;
                    case TP_ARRAY_OF_VALUES:
                        final Class<?> componentType = field.getType().getComponentType();

                        fieldDescriptor.myClassDescriptor = aStorage.getClassDescriptor(componentType);
                        hasReferences |= fieldDescriptor.myClassDescriptor.hasReferences;
                        break;
                    default:
                        // FIXME
                }

                fieldDescriptor.myType = type;
                aList.add(fieldDescriptor);
            }
        }
    }

    /**
     * Tests whether object is embedded.
     *
     * @param aObject Object to test
     * @return True if embedded; else, false
     */
    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    public static boolean isEmbedded(final Object aObject) {
        if (aObject != null) {
            final Class cls = aObject.getClass();

            return aObject instanceof IValue || aObject instanceof Number || cls.isArray() ||
                    cls == Character.class || cls == Boolean.class || cls == Date.class || cls == String.class;
        }

        return false;
    }

    /**
     * Gets type code.
     *
     * @param aClass A class for which to get a type code
     * @return A type code
     */
    public static int getTypeCode(final Class aClass) {
        int type;

        if (aClass.equals(byte.class)) {
            type = TP_BYTE;
        } else if (aClass.equals(short.class)) {
            type = TP_SHORT;
        } else if (aClass.equals(char.class)) {
            type = TP_CHAR;
        } else if (aClass.equals(int.class)) {
            type = TP_INT;
        } else if (aClass.equals(long.class)) {
            type = TP_LONG;
        } else if (aClass.equals(float.class)) {
            type = TP_FLOAT;
        } else if (aClass.equals(double.class)) {
            type = TP_DOUBLE;
        } else if (aClass.equals(String.class)) {
            type = TP_STRING;
        } else if (aClass.equals(boolean.class)) {
            type = TP_BOOLEAN;
        } else if (aClass.isEnum()) {
            type = TP_ENUM;
        } else if (aClass.equals(Date.class)) {
            type = TP_DATE;
        } else if (IValue.class.isAssignableFrom(aClass)) {
            type = TP_VALUE;
        } else if (aClass.equals(Link.class)) {
            type = TP_LINK;
        } else if (aClass.equals(Class.class)) {
            type = TP_CLASS;
        } else if (aClass.isArray()) {
            type = getTypeCode(aClass.getComponentType());

            if (type >= TP_LINK && type != TP_ENUM) {
                throw new StorageError(StorageError.UNSUPPORTED_TYPE, aClass);
            }

            type += TP_ARRAY_OF_BOOLEANS;
        } else if (CustomSerializable.class.isAssignableFrom(aClass)) {
            type = TP_CUSTOM;
        } else if (IPersistent.class.isAssignableFrom(aClass)) {
            type = TP_OBJECT;
        } else if (SERIALIZE_NONPERSISTENT_OBJS) {
            type = TP_RAW;
        } else if (TREAT_ANY_NONPERSISTENT_OBJ_AS_VALUE) {
            if (aClass.equals(Object.class)) {
                throw new StorageError(StorageError.EMPTY_VALUE);
            }

            type = TP_VALUE;
        } else {
            type = TP_OBJECT;
        }

        return type;
    }

    private static Class loadClass(final String aName) throws ClassNotFoundException {
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();

        if (loader != null) {
            try {
                return loader.loadClass(aName);
            } catch (final ClassNotFoundException details) {
                // FIXME
            }
        }

        return Class.forName(aName);
    }

    /**
     * Locates field for supplied name and class.
     *
     * @param aScope A class scope
     * @param aName A field name
     * @return The field for the name
     */
    public static Field locateField(final Class aScope, final String aName) {
        Class scope = aScope;

        try {
            do {
                try {
                    final Field field = scope.getDeclaredField(aName);

                    try {
                        field.setAccessible(true);
                    } catch (final Exception details) {
                        // FIXME
                    }

                    return field;
                } catch (final NoSuchFieldException x) {
                    scope = scope.getSuperclass();
                }
            } while (scope != null);
        } catch (final Exception details) {
            throw new StorageError(StorageError.ACCESS_VIOLATION, scope.getName() + "." + aName, details);
        }

        return null;
    }

    /**
     * Gets class name from supplied class.
     *
     * @param aClass The class from which to get the name
     * @return The name of the supplied class
     */
    public static String getClassName(final Class aClass) {
        final ClassLoader loader = aClass.getClassLoader();

        return loader instanceof INamedClassLoader ? ((INamedClassLoader) loader).getName() + ':' + aClass.getName()
                : aClass.getName();
    }

    /**
     * Loads class
     *
     * @param aStorage A storage from which to load class
     * @param aName A name of class to load
     * @return The loaded class
     */
    public static Class loadClass(final Storage aStorage, final String aName) {
        String name = aName;

        if (aStorage != null) {
            final int col = name.indexOf(':');
            final ClassLoader loader;

            if (col >= 0) {
                loader = aStorage.findClassLoader(name.substring(0, col));

                if (loader == null) {
                    // just ignore this class
                    return null;
                }

                name = name.substring(col + 1);
            } else {
                loader = aStorage.getClassLoader();
            }

            if (loader != null) {
                try {
                    return loader.loadClass(name);
                } catch (final ClassNotFoundException details) {
                    // FIXME
                }
            }
        }

        try {
            return loadClass(name);
        } catch (final ClassNotFoundException details) {
            throw new StorageError(StorageError.CLASS_NOT_FOUND, name, details);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onLoad() {
        final StorageImpl storage = (StorageImpl) getStorage();

        myClass = loadClass(storage, myName);
        isCustomSerializable = storage.mySerializer != null && storage.mySerializer.isApplicable(myClass);
        isCollection = Collection.class.isAssignableFrom(myClass);
        isMap = Map.class.isAssignableFrom(myClass);

        final int fieldCount = myFields.length;

        Class scope = myClass;

        for (int index = fieldCount; --index >= 0;) {
            final FieldDescriptor fieldDescriptor = myFields[index];

            fieldDescriptor.load();

            if (!fieldDescriptor.myClassName.equals(scope.getName())) {
                for (scope = myClass; scope != null; scope = scope.getSuperclass()) {
                    if (fieldDescriptor.myClassName.equals(scope.getName())) {
                        break;
                    }
                }
            }

            if (scope != null) {
                try {
                    final Field field = scope.getDeclaredField(fieldDescriptor.myFieldName);

                    if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0) {
                        try {
                            field.setAccessible(true);
                        } catch (final Exception details) {
                            // FIXME
                        }

                        fieldDescriptor.myField = field;
                    }
                } catch (final NoSuchFieldException details) {
                    // FIXME
                }
            } else {
                scope = myClass;
            }
        }

        for (int i = fieldCount; --i >= 0;) {
            final FieldDescriptor fd = myFields[i];
            if (fd.myField == null) {
                hierarchyLoop:

                for (scope = myClass; scope != null; scope = scope.getSuperclass()) {
                    try {
                        final Field f = scope.getDeclaredField(fd.myFieldName);

                        if ((f.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0) {
                            for (int j = 0; j < fieldCount; j++) {
                                if (myFields[j].myField == f) {
                                    continue hierarchyLoop;
                                }
                            }

                            try {
                                f.setAccessible(true);
                            } catch (final Exception details) {
                                // FIXME
                            }

                            fd.myField = f;
                            break;
                        }
                    } catch (final NoSuchFieldException details) {
                        // FIXME
                    }
                }
            }
        }

        locateConstructor();

        if (storage.myClassDescriptorMap.get(myClass) == null) {
            storage.myClassDescriptorMap.put(myClass, this);
        }
    }

    void resolve() {
        if (!isResolved) {
            final StorageImpl classStorage = (StorageImpl) getStorage();
            final ClassDescriptor classDescriptor = new ClassDescriptor(classStorage, myClass);

            isResolved = true;

            if (!classDescriptor.equals(this)) {
                classStorage.registerClassDescriptor(classDescriptor);
            }
        }
    }

    @Override
    public boolean recursiveLoading() {
        return false;
    }

    @SuppressWarnings("unchecked")
    private void locateConstructor() {
        try {
            myLoadFactory = (LoadFactory) loadClass(myClass.getName() + "LoadFactory").newInstance();
        } catch (final Exception details) {
            try {
                myLoadConstructor = myClass.getDeclaredConstructor(SODBOX_CONSTRUCTOR_PROFILE);
                myConstructorParams = new Object[] { this };
            } catch (final NoSuchMethodException details2) {
                try {
                    myLoadConstructor = getReflectionProvider().getDefaultConstructor(myClass);
                    myConstructorParams = null;
                } catch (final Exception details3) {
                    throw new StorageError(StorageError.DESCRIPTOR_FAILURE, myClass, details3);
                }
            }

            try {
                myLoadConstructor.setAccessible(true);
            } catch (final Exception details2) {
                // FIXME
            }
        }
    }

    static class FieldDescriptor extends Persistent implements Comparable {

        String myFieldName;

        String myClassName;

        int myType;

        ClassDescriptor myClassDescriptor;

        transient Field myField;

        @Override
        public int compareTo(final Object aObject) {
            return myFieldName.compareTo(((FieldDescriptor) aObject).myFieldName);
        }

        public boolean equals(final FieldDescriptor aFieldDescriptor) {
            return myFieldName.equals(aFieldDescriptor.myFieldName) && myClassName.equals(
                    aFieldDescriptor.myClassName) && myClassDescriptor == aFieldDescriptor.myClassDescriptor &&
                    myType == aFieldDescriptor.myType;
        }
    }

}
