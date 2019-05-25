
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.MessageCodes;
import info.freelibrary.sodbox.XMLImportException;

public class XMLImporter {

    static final String DATE_FORMAT = "EEE, d MMM yyyy kk:mm:ss z";

    static final DateFormat HTTP_FORMATTER = new SimpleDateFormat(DATE_FORMAT, Locale.ENGLISH);

    private static final String DATABASE = "database";

    private static final String NULL = "null";

    private static final String ID = "id";

    private static final String TYPE = "type";

    private static final String CLASS = "class";

    private static final String FIELD = "field";

    private static final String ELEMENT = "element";

    private static final String KEY = "key";

    private static final String REF = "ref";

    StorageImpl myStorage;

    XMLScanner myScanner;

    int[] myIDMap;

    /**
     * Constructs an XML importer.
     *
     * @param aStorage A database storage implementation
     * @param aReader A reader
     */
    public XMLImporter(final StorageImpl aStorage, final Reader aReader) {
        myStorage = aStorage;
        myScanner = new XMLScanner(aReader);
    }

    /**
     * Import database.
     *
     * @throws XMLImportException If there is trouble importing the database
     */
    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    public void importDatabase() throws XMLImportException {
        int token;

        if (myScanner.scan() != XMLScanner.XML_LT || myScanner.scan() != XMLScanner.XML_IDENT || !DATABASE.equals(
                myScanner.getIdentifier())) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_026);
        }

        if (myScanner.scan() != XMLScanner.XML_IDENT || !myScanner.getIdentifier().equals("root") || myScanner
                .scan() != XMLScanner.XML_EQ || myScanner.scan() != XMLScanner.XML_SCONST || myScanner
                        .scan() != XMLScanner.XML_GT) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_025);
        }

        int rootId = 0;

        try {
            rootId = Integer.parseInt(myScanner.getString());
        } catch (final NumberFormatException details) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_024);
        }

        myIDMap = new int[rootId * 2];
        myIDMap[rootId] = myStorage.allocateId();
        myStorage.myHeader.myRoot[1 - myStorage.myCurrentIndex].myRootObject = myIDMap[rootId];

        while ((token = myScanner.scan()) == XMLScanner.XML_LT) {
            if (myScanner.scan() != XMLScanner.XML_IDENT) {
                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_005);
            }

            final String elementName = myScanner.getIdentifier();

            if (Btree.class.getName().equals(elementName) || PersistentSet.class.getName().equals(elementName) ||
                    elementName.equals(BtreeFieldIndex.class.getName()) || BtreeCaseInsensitiveFieldIndex.class
                            .getName().equals(elementName) || BtreeCompoundIndex.class.getName().equals(
                                    elementName) || BtreeMultiFieldIndex.class.getName().equals(elementName) ||
                    BtreeCaseInsensitiveMultiFieldIndex.class.getName().equals(elementName)) {
                createIndex(elementName);
            } else {
                createObject(readElement(elementName));
            }
        }

        if (token != XMLScanner.XML_LTS || myScanner.scan() != XMLScanner.XML_IDENT || !DATABASE.equals(myScanner
                .getIdentifier()) || myScanner.scan() != XMLScanner.XML_GT) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_023);
        }
    }

    final String getAttribute(final XMLElement aElement, final String aName) throws XMLImportException {
        final String value = aElement.getAttribute(aName);

        if (value == null) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_022, aName);
        }

        return value;
    }

    final int getIntAttribute(final XMLElement aElement, final String aName) throws XMLImportException {
        final String value = aElement.getAttribute(aName);

        if (value == null) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_022, aName);
        }

        try {
            return Integer.parseInt(value);
        } catch (final NumberFormatException details) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_021, value);
        }
    }

    final int mapId(final int aID) {
        int oid = 0;

        if (aID != 0) {
            if (aID >= myIDMap.length) {
                final int[] newMap = new int[aID * 2];

                System.arraycopy(myIDMap, 0, newMap, 0, myIDMap.length);

                myIDMap = newMap;
                myIDMap[aID] = oid = myStorage.allocateId();
            } else {
                oid = myIDMap[aID];

                if (oid == 0) {
                    myIDMap[aID] = oid = myStorage.allocateId();
                }
            }
        }

        return oid;
    }

    final int mapType(final String aSignature) throws XMLImportException {
        for (int index = 0; index < ClassDescriptor.SIGNATURE.length; index++) {
            if (ClassDescriptor.SIGNATURE[index].equals(aSignature)) {
                return index;
            }
        }

        throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_020);
    }

    final Key createCompoundKey(final int[] aTypes, final String[] aValues) throws XMLImportException {
        final ByteBuffer buffer = new ByteBuffer();

        int dest = 0;

        try {
            for (int index = 0; index < aTypes.length; index++) {
                final String value = aValues[index];

                switch (aTypes[index]) {
                    case ClassDescriptor.TP_BOOLEAN:
                        buffer.extend(dest + 1);
                        buffer.myByteArray[dest++] = (byte) (Integer.parseInt(value) != 0 ? 1 : 0);
                        break;
                    case ClassDescriptor.TP_BYTE:
                        buffer.extend(dest + 1);
                        buffer.myByteArray[dest++] = Byte.parseByte(value);
                        break;
                    case ClassDescriptor.TP_CHAR:
                        buffer.extend(dest + 2);
                        Bytes.pack2(buffer.myByteArray, dest, (short) Integer.parseInt(value));
                        dest += 2;
                        break;
                    case ClassDescriptor.TP_SHORT:
                        buffer.extend(dest + 2);
                        Bytes.pack2(buffer.myByteArray, dest, Short.parseShort(value));
                        dest += 2;
                        break;
                    case ClassDescriptor.TP_INT:
                    case ClassDescriptor.TP_ENUM:
                        buffer.extend(dest + 4);
                        Bytes.pack4(buffer.myByteArray, dest, Integer.parseInt(value));
                        dest += 4;
                        break;
                    case ClassDescriptor.TP_OBJECT:
                        buffer.extend(dest + 4);
                        Bytes.pack4(buffer.myByteArray, dest, mapId(Integer.parseInt(value)));
                        dest += 4;
                        break;
                    case ClassDescriptor.TP_LONG:
                    case ClassDescriptor.TP_DATE:
                        buffer.extend(dest + 8);
                        Bytes.pack8(buffer.myByteArray, dest, Long.parseLong(value));
                        dest += 8;
                        break;
                    case ClassDescriptor.TP_FLOAT:
                        buffer.extend(dest + 4);
                        Bytes.pack4(buffer.myByteArray, dest, Float.floatToIntBits(Float.parseFloat(value)));
                        dest += 4;
                        break;
                    case ClassDescriptor.TP_DOUBLE:
                        buffer.extend(dest + 8);
                        Bytes.pack8(buffer.myByteArray, dest, Double.doubleToLongBits(Double.parseDouble(value)));
                        dest += 8;
                        break;
                    case ClassDescriptor.TP_STRING:
                    case ClassDescriptor.TP_CLASS:
                        dest = buffer.packString(dest, value);
                        break;
                    case ClassDescriptor.TP_ARRAY_OF_BYTES:
                        buffer.extend(dest + 4 + (value.length() >>> 1));
                        Bytes.pack4(buffer.myByteArray, dest, value.length() >>> 1);
                        dest += 4;

                        for (int j = 0, n = value.length(); j < n; j += 2) {
                            buffer.myByteArray[dest++] = (byte) (getHexValue(value.charAt(j)) << 4 | getHexValue(value
                                    .charAt(j + 1)));
                        }

                        break;
                    default:
                        throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_019);
                }
            }
        } catch (final NumberFormatException details) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_018);
        }

        return new Key(buffer.toArray());
    }

    final Key createKey(final int aType, final String aValue) throws XMLImportException {
        try {
            final Date date;

            switch (aType) {
                case ClassDescriptor.TP_BOOLEAN:
                    return new Key(Integer.parseInt(aValue) != 0);
                case ClassDescriptor.TP_BYTE:
                    return new Key(Byte.parseByte(aValue));
                case ClassDescriptor.TP_CHAR:
                    return new Key((char) Integer.parseInt(aValue));
                case ClassDescriptor.TP_SHORT:
                    return new Key(Short.parseShort(aValue));
                case ClassDescriptor.TP_INT:
                case ClassDescriptor.TP_ENUM:
                    return new Key(Integer.parseInt(aValue));
                case ClassDescriptor.TP_OBJECT:
                    return new Key(new PersistentStub(myStorage, mapId(Integer.parseInt(aValue))));
                case ClassDescriptor.TP_LONG:
                    return new Key(Long.parseLong(aValue));
                case ClassDescriptor.TP_FLOAT:
                    return new Key(Float.parseFloat(aValue));
                case ClassDescriptor.TP_DOUBLE:
                    return new Key(Double.parseDouble(aValue));
                case ClassDescriptor.TP_STRING:
                    return new Key(aValue);
                case ClassDescriptor.TP_ARRAY_OF_BYTES: {
                    final byte[] buffer = new byte[aValue.length() >> 1];

                    for (int i = 0; i < buffer.length; i++) {
                        buffer[i] = (byte) (getHexValue(aValue.charAt(i * 2)) << 4 | getHexValue(aValue.charAt(i * 2 +
                                1)));
                    }

                    return new Key(buffer);
                }
                case ClassDescriptor.TP_DATE:
                    if (aValue.equals(NULL)) {
                        date = null;
                    } else {
                        date = HTTP_FORMATTER.parse(aValue, new ParsePosition(0));

                        if (date == null) {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_007);
                        }
                    }

                    return new Key(date);
                default:
                    throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_019);
            }
        } catch (final NumberFormatException details) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_018);
        }
    }

    final int parseInt(final String aString) throws XMLImportException {
        try {
            return Integer.parseInt(aString);
        } catch (final NumberFormatException details) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_017, aString);
        }
    }

    final void createIndex(final String aIndexType) throws XMLImportException {
        Btree btree = null;
        int oid = 0;
        boolean unique = false;
        String className = null;
        String fieldName = null;
        String[] fieldNames = null;
        int[] types = null;
        long autoinc = 0;
        String type = null;
        int token;

        while ((token = myScanner.scan()) == XMLScanner.XML_IDENT) {
            final String attrName = myScanner.getIdentifier();

            if (myScanner.scan() != XMLScanner.XML_EQ || myScanner.scan() != XMLScanner.XML_SCONST) {
                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_003);
            }

            final String attrValue = myScanner.getString();

            if (ID.equals(attrName)) {
                oid = mapId(parseInt(attrValue));
            } else if ("unique".equals(attrName)) {
                unique = parseInt(attrValue) != 0;
            } else if (CLASS.equals(attrName)) {
                className = attrValue;
            } else if (TYPE.equals(attrName)) {
                type = attrValue;
            } else if ("autoinc".equals(attrName)) {
                autoinc = parseInt(attrValue);
            } else if (FIELD.equals(attrName)) {
                fieldName = attrValue;
            } else if (attrName.startsWith(TYPE)) {
                final int typeNo = Integer.parseInt(attrName.substring(4));

                if (types == null || types.length <= typeNo) {
                    final int[] newTypes = new int[typeNo + 1];

                    if (types != null) {
                        System.arraycopy(types, 0, newTypes, 0, types.length);
                    }

                    types = newTypes;
                }

                types[typeNo] = mapType(attrValue);
            } else if (attrName.startsWith(FIELD)) {
                final int fieldNo = Integer.parseInt(attrName.substring(5));

                if (fieldNames == null || fieldNames.length <= fieldNo) {
                    final String[] newFieldNames = new String[fieldNo + 1];

                    if (fieldNames != null) {
                        System.arraycopy(fieldNames, 0, newFieldNames, 0, fieldNames.length);
                    }

                    fieldNames = newFieldNames;
                }

                fieldNames[fieldNo] = attrValue;
            }
        }

        if (token != XMLScanner.XML_GT) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_016);
        }

        if (oid == 0) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_015);
        }

        if (className != null) {
            final Class cls = ClassDescriptor.loadClass(myStorage, className);

            if (fieldName != null) {
                if (aIndexType.equals(BtreeCaseInsensitiveFieldIndex.class.getName())) {
                    btree = new BtreeCaseInsensitiveFieldIndex(cls, fieldName, unique, autoinc);
                } else {
                    btree = new BtreeFieldIndex(cls, fieldName, unique, autoinc);
                }
            } else if (fieldNames != null) {
                if (aIndexType.equals(BtreeCaseInsensitiveMultiFieldIndex.class.getName())) {
                    btree = new BtreeCaseInsensitiveMultiFieldIndex(cls, fieldNames, unique);
                } else {
                    btree = new BtreeMultiFieldIndex(cls, fieldNames, unique);
                }
            } else {
                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_014);
            }
        } else {
            if (types != null) {
                btree = new BtreeCompoundIndex(types, unique);
            } else if (type == null) {
                if (PersistentSet.class.getName().equals(aIndexType)) {
                    btree = new PersistentSet(unique);
                } else {
                    throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_013);
                }
            } else {
                btree = new Btree(mapType(type), unique);
            }
        }

        myStorage.assignOid(btree, oid, false);

        while ((token = myScanner.scan()) == XMLScanner.XML_LT) {
            if (myScanner.scan() != XMLScanner.XML_IDENT || !REF.equals(myScanner.getIdentifier())) {
                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_012);
            }

            final XMLElement ref = readElement(REF);

            Key key = null;

            if (fieldNames != null) {
                final String[] values = new String[fieldNames.length];

                for (int index = 0; index < values.length; index++) {
                    values[index] = getAttribute(ref, KEY + index);
                }

                key = createCompoundKey(((BtreeMultiFieldIndex) btree).myTypes, values);
            } else if (types != null) {
                final String[] values = new String[types.length];

                for (int index = 0; index < values.length; index++) {
                    values[index] = getAttribute(ref, KEY + index);
                }

                key = createCompoundKey(types, values);
            } else {
                key = createKey(btree.myType, getAttribute(ref, KEY));
            }

            btree.insert(key, new PersistentStub(myStorage, mapId(getIntAttribute(ref, ID))), false);
        }

        if (token != XMLScanner.XML_LTS || myScanner.scan() != XMLScanner.XML_IDENT || !myScanner.getIdentifier()
                .equals(aIndexType) || myScanner.scan() != XMLScanner.XML_GT) {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_004);
        }

        final byte[] data = myStorage.packObject(btree, false);
        final int size = ObjectHeader.getSize(data, 0);
        final long pos = myStorage.allocate(size, 0);

        myStorage.setPosition(oid, pos | StorageImpl.DB_MODIFIED_FLAG);
        myStorage.myPool.put(pos & ~StorageImpl.DB_FLAGS_MASK, data, size);
    }

    final void createObject(final XMLElement aElement) throws XMLImportException {
        final Class clazz = ClassDescriptor.loadClass(myStorage, aElement.myName);
        final ClassDescriptor classDescriptor = myStorage.getClassDescriptor(clazz);
        final int oid = mapId(getIntAttribute(aElement, ID));
        final ByteBuffer buffer = new ByteBuffer();
        final long position;

        int offset = ObjectHeader.SIZE_OF;

        buffer.extend(offset);
        offset = packObject(aElement, classDescriptor, offset, buffer);

        ObjectHeader.setSize(buffer.myByteArray, 0, offset);
        ObjectHeader.setType(buffer.myByteArray, 0, classDescriptor.getOid());

        position = myStorage.allocate(offset, 0);

        myStorage.setPosition(oid, position | StorageImpl.DB_MODIFIED_FLAG);
        myStorage.myPool.put(position, buffer.myByteArray, offset);
    }

    final int getHexValue(final char aChar) throws XMLImportException {
        if (aChar >= '0' && aChar <= '9') {
            return aChar - '0';
        } else if (aChar >= 'A' && aChar <= 'F') {
            return aChar - 'A' + 10;
        } else if (aChar >= 'a' && aChar <= 'f') {
            return aChar - 'a' + 10;
        } else {
            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_011);
        }
    }

    final int importBinary(final XMLElement aElement, final int aOffset, final ByteBuffer aBuffer,
            final String aFieldName) throws XMLImportException {
        int offset = aOffset;

        if (aElement == null || aElement.isNullValue()) {
            offset = aBuffer.packI4(aOffset, -1);
        } else if (aElement.isStringValue()) {
            final String hexStr = aElement.getStringValue();
            final int len = hexStr.length();

            aBuffer.extend(aOffset + 4 + len / 2);
            Bytes.pack4(aBuffer.myByteArray, aOffset, len / 2);
            offset += 4;

            for (int jndex = 0; jndex < len; jndex += 2) {
                aBuffer.myByteArray[offset++] = (byte) (getHexValue(hexStr.charAt(jndex)) << 4 | getHexValue(hexStr
                        .charAt(jndex + 1)));
            }
        } else {
            final XMLElement ref = aElement.getSibling(REF);

            if (ref != null) {
                aBuffer.extend(aOffset + 4);
                Bytes.pack4(aBuffer.myByteArray, aOffset, mapId(getIntAttribute(ref, ID)));
                offset += 4;
            } else {
                XMLElement item = aElement.getSibling(ELEMENT);
                int len = item == null ? 0 : item.getCounter();

                aBuffer.extend(aOffset + 4 + len);
                Bytes.pack4(aBuffer.myByteArray, aOffset, len);
                offset += 4;

                while (--len >= 0) {
                    if (item.isIntValue()) {
                        aBuffer.myByteArray[aOffset] = (byte) item.getIntValue();
                    } else if (item.isRealValue()) {
                        aBuffer.myByteArray[aOffset] = (byte) item.getRealValue();
                    } else {
                        throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_006,
                                aFieldName);
                    }

                    item = item.getNextSibling();
                    offset += 1;
                }
            }
        }

        return offset;
    }

    int importRef(final XMLElement aElement, final int aOffset, final ByteBuffer aBuffer) throws XMLImportException {
        int offset = aOffset;
        int oid = 0;

        if (aElement != null) {
            if (aElement.isStringValue()) {
                final String str = aElement.getStringValue();

                offset = aBuffer.packI4(offset, -1 - ClassDescriptor.TP_STRING);

                return aBuffer.packString(offset, str);
            } else {
                final XMLElement value = aElement.getFirstSibling();

                if (value == null) {
                    throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_010);
                }

                final String name = value.getName();

                if ("scalar".equals(name)) {
                    final int tid = getIntAttribute(value, TYPE);
                    final String hexStr = getAttribute(value, "value");
                    final int len = hexStr.length();

                    aBuffer.extend(offset + 4 + len / 2);
                    Bytes.pack4(aBuffer.myByteArray, offset, -1 - tid);
                    offset += 4;

                    if (tid == ClassDescriptor.TP_CUSTOM) {
                        try {
                            final Object obj = myStorage.mySerializer.parse(hexStr);

                            myStorage.mySerializer.pack(obj, aBuffer.getOutputStream());
                            offset = aBuffer.size();
                        } catch (final Exception details) {
                            throw new XMLImportException(details, myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_009, details.getMessage());
                        }
                    } else {
                        for (int jndex = 0; jndex < len; jndex += 2) {
                            aBuffer.myByteArray[offset++] = (byte) (getHexValue(hexStr.charAt(jndex)) << 4 |
                                    getHexValue(hexStr.charAt(jndex + 1)));
                        }
                    }

                    return offset;
                } else if (name == CLASS) {
                    final String className = getAttribute(value, "name");
                    offset = aBuffer.packI4(offset, -1 - ClassDescriptor.TP_CLASS);
                    return aBuffer.packString(offset, className);
                } else if (REF.equals(name)) {
                    oid = mapId(getIntAttribute(value, ID));
                } else {
                    final Class cls = ClassDescriptor.loadClass(myStorage, name);
                    final ClassDescriptor desc = myStorage.getClassDescriptor(cls);

                    offset = aBuffer.packI4(offset, -ClassDescriptor.TP_VALUE_TYPE_BIAS - desc.getOid());

                    if (desc.isCollection) {
                        XMLElement item = value.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        offset = aBuffer.packI4(offset, len);

                        while (--len >= 0) {
                            offset = importRef(item, offset, aBuffer);
                            item = item.getNextSibling();
                        }
                    } else {
                        offset = packObject(value, desc, offset, aBuffer);
                    }

                    return offset;
                }
            }
        }

        return aBuffer.packI4(offset, oid);
    }

    final int packObject(final XMLElement aObjElem, final ClassDescriptor aClassDescriptor, final int aOffset,
            final ByteBuffer aBuffer) throws XMLImportException {
        final ClassDescriptor.FieldDescriptor[] fields = aClassDescriptor.myFields;

        int offset = aOffset;

        for (int index = 0, n = fields.length; index < n; index++) {
            final ClassDescriptor.FieldDescriptor fileDescriptor = fields[index];
            final String fieldName = fileDescriptor.myFieldName;
            final XMLElement element = aObjElem != null ? aObjElem.getSibling(fieldName) : null;

            switch (fileDescriptor.myType) {
                case ClassDescriptor.TP_BYTE:
                    aBuffer.extend(offset + 1);

                    if (element != null) {
                        if (element.isIntValue()) {
                            aBuffer.myByteArray[offset] = (byte) element.getIntValue();
                        } else if (element.isRealValue()) {
                            aBuffer.myByteArray[offset] = (byte) element.getRealValue();
                        } else {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_006, fieldName);
                        }
                    }

                    offset += 1;
                    continue;
                case ClassDescriptor.TP_BOOLEAN:
                    aBuffer.extend(offset + 1);

                    if (element != null) {
                        if (element.isIntValue()) {
                            aBuffer.myByteArray[offset] = (byte) (element.getIntValue() != 0 ? 1 : 0);
                        } else if (element.isRealValue()) {
                            aBuffer.myByteArray[offset] = (byte) (element.getRealValue() != 0.0 ? 1 : 0);
                        } else {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_006, fieldName);
                        }
                    }

                    offset += 1;
                    continue;
                case ClassDescriptor.TP_SHORT:
                case ClassDescriptor.TP_CHAR:
                    aBuffer.extend(offset + 2);

                    if (element != null) {
                        if (element.isIntValue()) {
                            Bytes.pack2(aBuffer.myByteArray, offset, (short) element.getIntValue());
                        } else if (element.isRealValue()) {
                            Bytes.pack2(aBuffer.myByteArray, offset, (short) element.getRealValue());
                        } else {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_006, fieldName);
                        }
                    }

                    offset += 2;
                    continue;
                case ClassDescriptor.TP_INT:
                    aBuffer.extend(offset + 4);

                    if (element != null) {
                        if (element.isIntValue()) {
                            Bytes.pack4(aBuffer.myByteArray, offset, (int) element.getIntValue());
                        } else if (element.isRealValue()) {
                            Bytes.pack4(aBuffer.myByteArray, offset, (int) element.getRealValue());
                        } else {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_006, fieldName);
                        }
                    }

                    offset += 4;
                    continue;
                case ClassDescriptor.TP_LONG:
                    aBuffer.extend(offset + 8);

                    if (element != null) {
                        if (element.isIntValue()) {
                            Bytes.pack8(aBuffer.myByteArray, offset, element.getIntValue());
                        } else if (element.isRealValue()) {
                            Bytes.pack8(aBuffer.myByteArray, offset, (long) element.getRealValue());
                        } else {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_006, fieldName);
                        }
                    }

                    offset += 8;
                    continue;
                case ClassDescriptor.TP_FLOAT:
                    aBuffer.extend(offset + 4);

                    if (element != null) {
                        if (element.isIntValue()) {
                            Bytes.pack4(aBuffer.myByteArray, offset, Float.floatToIntBits(element.getIntValue()));
                        } else if (element.isRealValue()) {
                            Bytes.pack4(aBuffer.myByteArray, offset, Float.floatToIntBits((float) element
                                    .getRealValue()));
                        } else {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_006, fieldName);
                        }
                    }

                    offset += 4;
                    continue;
                case ClassDescriptor.TP_DOUBLE:
                    aBuffer.extend(offset + 8);

                    if (element != null) {
                        if (element.isIntValue()) {
                            Bytes.pack8(aBuffer.myByteArray, offset, Double.doubleToLongBits(element.getIntValue()));
                        } else if (element.isRealValue()) {
                            Bytes.pack8(aBuffer.myByteArray, offset, Double.doubleToLongBits(element.getRealValue()));
                        } else {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_006, fieldName);
                        }
                    }

                    offset += 8;
                    continue;
                case ClassDescriptor.TP_ENUM:
                    aBuffer.extend(offset + 4);

                    if (element != null) {
                        if (element.isIntValue()) {
                            Bytes.pack4(aBuffer.myByteArray, offset, (int) element.getIntValue());
                        } else if (element.isRealValue()) {
                            Bytes.pack4(aBuffer.myByteArray, offset, (int) element.getRealValue());
                        } else if (element.isNullValue()) {
                            Bytes.pack4(aBuffer.myByteArray, offset, -1);
                        } else if (element.isStringValue()) {
                            Bytes.pack4(aBuffer.myByteArray, offset, Enum.valueOf((Class) fileDescriptor.myField
                                    .getType(), element.getStringValue()).ordinal());
                        } else {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_006, fieldName);
                        }
                    }

                    offset += 4;
                    continue;
                case ClassDescriptor.TP_DATE:
                    aBuffer.extend(offset + 8);

                    if (element != null) {
                        if (element.isIntValue()) {
                            Bytes.pack8(aBuffer.myByteArray, offset, element.getIntValue());
                        } else if (element.isNullValue()) {
                            Bytes.pack8(aBuffer.myByteArray, offset, -1);
                        } else if (element.isStringValue()) {
                            final Date date = HTTP_FORMATTER.parse(element.getStringValue(), new ParsePosition(0));
                            if (date == null) {
                                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                        MessageCodes.SB_007);
                            }
                            Bytes.pack8(aBuffer.myByteArray, offset, date.getTime());
                        } else {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_006, fieldName);
                        }
                    }

                    offset += 8;
                    continue;
                case ClassDescriptor.TP_STRING:
                case ClassDescriptor.TP_CLASS:
                    if (element != null) {
                        String value = null;

                        if (element.isIntValue()) {
                            value = Long.toString(element.getIntValue());
                        } else if (element.isRealValue()) {
                            value = Double.toString(element.getRealValue());
                        } else if (element.isStringValue()) {
                            value = element.getStringValue();
                        } else if (element.isNullValue()) {
                            value = null;
                        } else {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_006, fieldName);
                        }

                        offset = aBuffer.packString(offset, value);
                        continue;
                    }

                    offset = aBuffer.packI4(offset, -1);
                    continue;
                case ClassDescriptor.TP_OBJECT:
                    offset = importRef(element, offset, aBuffer);
                    continue;
                case ClassDescriptor.TP_VALUE:
                    offset = packObject(element, fileDescriptor.myClassDescriptor, offset, aBuffer);
                    continue;
                case ClassDescriptor.TP_RAW:
                case ClassDescriptor.TP_ARRAY_OF_BYTES:
                    offset = importBinary(element, offset, aBuffer, fieldName);
                    continue;
                case ClassDescriptor.TP_CUSTOM: {
                    if (!element.isStringValue()) {
                        throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_009);
                    }

                    final String string = element.getStringValue();

                    try {
                        final Object obj = myStorage.mySerializer.parse(string);

                        myStorage.mySerializer.pack(obj, aBuffer.getOutputStream());
                        offset = aBuffer.size();
                    } catch (final Exception details) {
                        throw new XMLImportException(details, myScanner.getLine(), myScanner.getColumn(),
                                MessageCodes.SB_008, details.getMessage());
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_BOOLEANS:
                    if (element == null || element.isNullValue()) {
                        offset = aBuffer.packI4(offset, -1);
                    } else {
                        XMLElement item = element.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        aBuffer.extend(offset + 4 + len);
                        Bytes.pack4(aBuffer.myByteArray, offset, len);
                        offset += 4;

                        while (--len >= 0) {
                            if (item.isIntValue()) {
                                aBuffer.myByteArray[offset] = (byte) (item.getIntValue() != 0 ? 1 : 0);
                            } else if (item.isRealValue()) {
                                aBuffer.myByteArray[offset] = (byte) (item.getRealValue() != 0.0 ? 1 : 0);
                            } else {
                                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                        MessageCodes.SB_006, fieldName);
                            }

                            item = item.getNextSibling();
                            offset += 1;
                        }
                    }

                    continue;
                case ClassDescriptor.TP_ARRAY_OF_CHARS:
                case ClassDescriptor.TP_ARRAY_OF_SHORTS:
                    if (element == null || element.isNullValue()) {
                        offset = aBuffer.packI4(offset, -1);
                    } else {
                        XMLElement item = element.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        aBuffer.extend(offset + 4 + len * 2);
                        Bytes.pack4(aBuffer.myByteArray, offset, len);
                        offset += 4;

                        while (--len >= 0) {
                            if (item.isIntValue()) {
                                Bytes.pack2(aBuffer.myByteArray, offset, (short) item.getIntValue());
                            } else if (item.isRealValue()) {
                                Bytes.pack2(aBuffer.myByteArray, offset, (short) item.getRealValue());
                            } else {
                                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                        MessageCodes.SB_006, fieldName);
                            }

                            item = item.getNextSibling();
                            offset += 2;
                        }
                    }

                    continue;
                case ClassDescriptor.TP_ARRAY_OF_INTS:
                    if (element == null || element.isNullValue()) {
                        offset = aBuffer.packI4(offset, -1);
                    } else {
                        XMLElement item = element.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        aBuffer.extend(offset + 4 + len * 4);
                        Bytes.pack4(aBuffer.myByteArray, offset, len);
                        offset += 4;

                        while (--len >= 0) {
                            if (item.isIntValue()) {
                                Bytes.pack4(aBuffer.myByteArray, offset, (int) item.getIntValue());
                            } else if (item.isRealValue()) {
                                Bytes.pack4(aBuffer.myByteArray, offset, (int) item.getRealValue());
                            } else {
                                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                        MessageCodes.SB_006, fieldName);
                            }

                            item = item.getNextSibling();
                            offset += 4;
                        }
                    }

                    continue;
                case ClassDescriptor.TP_ARRAY_OF_ENUMS:
                    if (element == null || element.isNullValue()) {
                        offset = aBuffer.packI4(offset, -1);
                    } else {
                        final Class enumType = fileDescriptor.myField.getType();

                        XMLElement item = element.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        aBuffer.extend(offset + 4 + len * 4);
                        Bytes.pack4(aBuffer.myByteArray, offset, len);

                        offset += 4;

                        while (--len >= 0) {
                            if (item.isIntValue()) {
                                Bytes.pack4(aBuffer.myByteArray, offset, (int) item.getIntValue());
                            } else if (item.isRealValue()) {
                                Bytes.pack4(aBuffer.myByteArray, offset, (int) item.getRealValue());
                            } else if (item.isNullValue()) {
                                Bytes.pack4(aBuffer.myByteArray, offset, -1);
                            } else if (item.isStringValue()) {
                                Bytes.pack4(aBuffer.myByteArray, offset, Enum.valueOf(enumType, item.getStringValue())
                                        .ordinal());
                            } else {
                                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                        MessageCodes.SB_006, fieldName);
                            }

                            item = item.getNextSibling();
                            offset += 4;
                        }
                    }

                    continue;
                case ClassDescriptor.TP_ARRAY_OF_LONGS:
                    if (element == null || element.isNullValue()) {
                        offset = aBuffer.packI4(offset, -1);
                    } else {
                        XMLElement item = element.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        aBuffer.extend(offset + 4 + len * 8);
                        Bytes.pack4(aBuffer.myByteArray, offset, len);
                        offset += 4;

                        while (--len >= 0) {
                            if (item.isIntValue()) {
                                Bytes.pack8(aBuffer.myByteArray, offset, item.getIntValue());
                            } else if (item.isRealValue()) {
                                Bytes.pack8(aBuffer.myByteArray, offset, (long) item.getRealValue());
                            } else {
                                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                        MessageCodes.SB_006, fieldName);
                            }

                            item = item.getNextSibling();
                            offset += 8;
                        }
                    }

                    continue;
                case ClassDescriptor.TO_ARRAY_OF_FLOATS:
                    if (element == null || element.isNullValue()) {
                        offset = aBuffer.packI4(offset, -1);
                    } else {
                        XMLElement item = element.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        aBuffer.extend(offset + 4 + len * 4);
                        Bytes.pack4(aBuffer.myByteArray, offset, len);
                        offset += 4;

                        while (--len >= 0) {
                            if (item.isIntValue()) {
                                Bytes.pack4(aBuffer.myByteArray, offset, Float.floatToIntBits(item.getIntValue()));
                            } else if (item.isRealValue()) {
                                Bytes.pack4(aBuffer.myByteArray, offset, Float.floatToIntBits((float) item
                                        .getRealValue()));
                            } else {
                                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                        MessageCodes.SB_006, fieldName);
                            }

                            item = item.getNextSibling();
                            offset += 4;
                        }
                    }

                    continue;
                case ClassDescriptor.TP_ARRAY_OF_DOUBLES:
                    if (element == null || element.isNullValue()) {
                        offset = aBuffer.packI4(offset, -1);
                    } else {
                        XMLElement item = element.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        aBuffer.extend(offset + 4 + len * 8);
                        Bytes.pack4(aBuffer.myByteArray, offset, len);
                        offset += 4;

                        while (--len >= 0) {
                            if (item.isIntValue()) {
                                Bytes.pack8(aBuffer.myByteArray, offset, Double.doubleToLongBits(item.getIntValue()));
                            } else if (item.isRealValue()) {
                                Bytes.pack8(aBuffer.myByteArray, offset, Double.doubleToLongBits(item
                                        .getRealValue()));
                            } else {
                                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                        MessageCodes.SB_006, fieldName);
                            }

                            item = item.getNextSibling();
                            offset += 8;
                        }
                    }

                    continue;
                case ClassDescriptor.TP_ARRAY_OF_DATES:
                    if (element == null || element.isNullValue()) {
                        offset = aBuffer.packI4(offset, -1);
                    } else {
                        XMLElement item = element.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        aBuffer.extend(offset + 4 + len * 8);
                        Bytes.pack4(aBuffer.myByteArray, offset, len);
                        offset += 4;

                        while (--len >= 0) {
                            if (item.isNullValue()) {
                                Bytes.pack8(aBuffer.myByteArray, offset, -1);
                            } else if (item.isStringValue()) {
                                final Date date = HTTP_FORMATTER.parse(item.getStringValue(), new ParsePosition(0));

                                if (date == null) {
                                    throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                            MessageCodes.SB_007);
                                }

                                Bytes.pack8(aBuffer.myByteArray, offset, date.getTime());
                            } else {
                                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                        MessageCodes.SB_006, fieldName);
                            }

                            item = item.getNextSibling();
                            offset += 8;
                        }
                    }

                    continue;
                case ClassDescriptor.TP_ARRAY_OF_STRINGS:
                    if (element == null || element.isNullValue()) {
                        offset = aBuffer.packI4(offset, -1);
                    } else {
                        XMLElement item = element.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        aBuffer.extend(offset + 4);
                        Bytes.pack4(aBuffer.myByteArray, offset, len);
                        offset += 4;

                        while (--len >= 0) {
                            String value = null;

                            if (item.isIntValue()) {
                                value = Long.toString(item.getIntValue());
                            } else if (item.isRealValue()) {
                                value = Double.toString(item.getRealValue());
                            } else if (item.isStringValue()) {
                                value = item.getStringValue();
                            } else if (item.isNullValue()) {
                                value = null;
                            } else {
                                throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                        MessageCodes.SB_006, fieldName);
                            }

                            offset = aBuffer.packString(offset, value);
                            item = item.getNextSibling();
                        }
                    }

                    continue;
                case ClassDescriptor.TP_ARRAY_OF_OBJECTS:
                case ClassDescriptor.TP_LINK:
                    if (element == null || element.isNullValue()) {
                        offset = aBuffer.packI4(offset, -1);
                    } else {
                        XMLElement item = element.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        offset = aBuffer.packI4(offset, len);

                        while (--len >= 0) {
                            offset = importRef(item, offset, aBuffer);
                            item = item.getNextSibling();
                        }
                    }

                    continue;
                case ClassDescriptor.TP_ARRAY_OF_VALUES:
                    if (element == null || element.isNullValue()) {
                        offset = aBuffer.packI4(offset, -1);
                    } else {
                        final ClassDescriptor elemDesc = fileDescriptor.myClassDescriptor;

                        XMLElement item = element.getSibling(ELEMENT);
                        int len = item == null ? 0 : item.getCounter();

                        offset = aBuffer.packI4(offset, len);

                        while (--len >= 0) {
                            offset = packObject(item, elemDesc, offset, aBuffer);
                            item = item.getNextSibling();
                        }
                    }

                    continue;
                default:
                    // FIXME: Do what here?
            }
        }

        return offset;
    }

    final XMLElement readElement(final String aName) throws XMLImportException {
        final XMLElement element = new XMLElement(aName);

        String attribute;
        int token;

        while (true) {
            switch (myScanner.scan()) {
                case XMLScanner.XML_GTS:
                    return element;
                case XMLScanner.XML_GT:
                    while ((token = myScanner.scan()) == XMLScanner.XML_LT) {
                        if (myScanner.scan() != XMLScanner.XML_IDENT) {
                            throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(),
                                    MessageCodes.SB_005);
                        }

                        final String siblingName = myScanner.getIdentifier();
                        final XMLElement sibling = readElement(siblingName);

                        element.addSibling(sibling);
                    }

                    switch (token) {
                        case XMLScanner.XML_SCONST:
                            element.setStringValue(myScanner.getString());
                            token = myScanner.scan();
                            break;
                        case XMLScanner.XML_ICONST:
                            element.setIntValue(myScanner.getInt());
                            token = myScanner.scan();
                            break;
                        case XMLScanner.XML_FCONST:
                            element.setRealValue(myScanner.getReal());
                            token = myScanner.scan();
                            break;
                        case XMLScanner.XML_IDENT:
                            if (myScanner.getIdentifier().equals(NULL)) {
                                element.setNullValue();
                            } else {
                                element.setStringValue(myScanner.getIdentifier());
                            }

                            token = myScanner.scan();
                            break;
                        default:
                            // FIXME: Do what here?
                    }

                    if (token != XMLScanner.XML_LTS || myScanner.scan() != XMLScanner.XML_IDENT || !myScanner
                            .getIdentifier().equals(aName) || myScanner.scan() != XMLScanner.XML_GT) {
                        throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_004);
                    }

                    return element;
                case XMLScanner.XML_IDENT:
                    attribute = myScanner.getIdentifier();

                    if (myScanner.scan() != XMLScanner.XML_EQ || myScanner.scan() != XMLScanner.XML_SCONST) {
                        throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_003);
                    }

                    element.addAttribute(attribute, myScanner.getString());

                    continue;
                default:
                    throw new XMLImportException(myScanner.getLine(), myScanner.getColumn(), MessageCodes.SB_002);
            }
        }
    }

    static class XMLElement {

        static final int NO_VALUE = 0;

        static final int STRING_VALUE = 1;

        static final int INT_VALUE = 2;

        static final int REAL_VALUE = 3;

        static final int NULL_VALUE = 4;

        static final Collection<XMLElement> EMPTY_COLLECTION = new ArrayList<>();

        private XMLElement myNext;

        private XMLElement myPrev;

        private final String myName;

        private HashMap<String, XMLElement> mySiblings;

        private HashMap<String, String> myAttributes;

        private String myStringValue;

        private long myIntValue;

        private double myRealValue;

        private int myValueType;

        private int myCounter;

        XMLElement(final String aName) {
            myName = aName;
            myValueType = NO_VALUE;
        }

        final void addSibling(final XMLElement aElement) {
            if (mySiblings == null) {
                mySiblings = new HashMap<>();
            }

            final XMLElement prev = mySiblings.get(aElement.myName);

            if (prev != null) {
                aElement.myNext = null;
                aElement.myPrev = prev.myPrev;
                aElement.myPrev.myNext = aElement;
                prev.myPrev = aElement;
                prev.myCounter += 1;
            } else {
                mySiblings.put(aElement.myName, aElement);
                aElement.myPrev = aElement;
                aElement.myCounter = 1;
            }
        }

        final void addAttribute(final String aName, final String aValue) {
            if (myAttributes == null) {
                myAttributes = new HashMap();
            }

            myAttributes.put(aName, aValue);
        }

        final XMLElement getSibling(final String aName) {
            if (mySiblings != null) {
                return mySiblings.get(aName);
            }

            return null;
        }

        final Collection<XMLElement> getSiblings() {
            return mySiblings != null ? mySiblings.values() : EMPTY_COLLECTION;
        }

        final XMLElement getFirstSibling() {
            for (final XMLElement element : getSiblings()) {
                return element;
            }

            return null;
        }

        final XMLElement getNextSibling() {
            return myNext;
        }

        final String getName() {
            return myName;
        }

        final int getCounter() {
            return myCounter;
        }

        final String getAttribute(final String aName) {
            return myAttributes != null ? myAttributes.get(aName) : null;
        }

        final void setIntValue(final long aValue) {
            myIntValue = aValue;
            myValueType = INT_VALUE;
        }

        final void setRealValue(final double aValue) {
            myRealValue = aValue;
            myValueType = REAL_VALUE;
        }

        final void setStringValue(final String aValue) {
            myStringValue = aValue;
            myValueType = STRING_VALUE;
        }

        final void setNullValue() {
            myValueType = NULL_VALUE;
        }

        final String getStringValue() {
            return myStringValue;
        }

        final long getIntValue() {
            return myIntValue;
        }

        final double getRealValue() {
            return myRealValue;
        }

        final boolean isIntValue() {
            return myValueType == INT_VALUE;
        }

        final boolean isRealValue() {
            return myValueType == REAL_VALUE;
        }

        final boolean isStringValue() {
            return myValueType == STRING_VALUE;
        }

        final boolean isNullValue() {
            return myValueType == NULL_VALUE;
        }
    }

    static class XMLScanner {

        static final int XML_IDENT = 0;

        static final int XML_SCONST = 1;

        static final int XML_ICONST = 2;

        static final int XML_FCONST = 3;

        static final int XML_LT = 4;

        static final int XML_GT = 5;

        static final int XML_LTS = 6;

        static final int XML_GTS = 7;

        static final int XML_EQ = 8;

        static final int XML_EOF = 9;

        Reader myReader;

        int myLine;

        int myColumn;

        char[] myStringConst;

        long myIntConst;

        double myFpConst;

        int myStringLen;

        String myIdent;

        int mySize;

        int myUngetChar;

        boolean hasUngetChar;

        XMLScanner(final Reader aReader) {
            myReader = aReader;
            myStringConst = new char[mySize = 1024];
            myLine = 1;
            myColumn = 0;
            hasUngetChar = false;
        }

        final int get() throws XMLImportException {
            if (hasUngetChar) {
                hasUngetChar = false;

                return myUngetChar;
            }

            try {
                final int character = myReader.read();

                if (character == '\n') {
                    myLine += 1;
                    myColumn = 0;
                } else if (character == '\t') {
                    myColumn += myColumn + 8 & ~7;
                } else {
                    myColumn += 1;
                }

                return character;
            } catch (final IOException x) {
                throw new XMLImportException(myLine, myColumn, x.getMessage());
            }
        }

        final void unget(final int aCharacter) {
            if (aCharacter == '\n') {
                myLine -= 1;
            } else {
                myColumn -= 1;
            }

            myUngetChar = aCharacter;
            hasUngetChar = true;
        }

        final int scan() throws XMLImportException {
            boolean floatingPoint;
            int character;
            int index;
            int quote;

            while (true) {
                do {
                    if ((character = get()) < 0) {
                        return XML_EOF;
                    }
                } while (character <= ' ');

                switch (character) {
                    case '<':
                        character = get();
                        if (character == '?') {
                            while ((character = get()) != '?') {
                                if (character < 0) {
                                    throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                                }
                            }

                            if ((character = get()) != '>') {
                                throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                            }

                            continue;
                        }

                        if (character != '/') {
                            unget(character);
                            return XML_LT;
                        }

                        return XML_LTS;
                    case '>':
                        return XML_GT;
                    case '/':
                        character = get();

                        if (character != '>') {
                            unget(character);
                            throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                        }

                        return XML_GTS;
                    case '=':
                        return XML_EQ;
                    case '"':
                    case '\'':
                        quote = character;
                        index = 0;

                        while (true) {
                            character = get();

                            if (character < 0) {
                                throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                            } else if (character == '&') {
                                switch (get()) {
                                    case 'a':
                                        character = get();

                                        if (character == 'm') {
                                            if (get() == 'p' && get() == ';') {
                                                character = '&';
                                                break;
                                            }
                                        } else if (character == 'p' && get() == 'o' && get() == 's' && get() == ';') {
                                            character = '\'';
                                            break;
                                        }

                                        throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                                    case 'l':
                                        if (get() != 't' || get() != ';') {
                                            throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                                        }

                                        character = '<';
                                        break;
                                    case 'g':
                                        if (get() != 't' || get() != ';') {
                                            throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                                        }

                                        character = '>';
                                        break;
                                    case 'q':
                                        if (get() != 'u' || get() != 'o' || get() != 't' || get() != ';') {
                                            throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                                        }

                                        character = '"';
                                        break;
                                    default:
                                        throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                                }
                            } else if (character == quote) {
                                myStringLen = index;
                                return XML_SCONST;
                            }

                            if (index == mySize) {
                                final char[] newBuf = new char[mySize *= 2];
                                System.arraycopy(myStringConst, 0, newBuf, 0, index);
                                myStringConst = newBuf;
                            }

                            myStringConst[index++] = (char) character;
                        }
                    case '-':
                    case '+':
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                        index = 0;
                        floatingPoint = false;

                        while (true) {
                            if (!Character.isDigit((char) character) && character != '-' && character != '+' &&
                                    character != '.' && character != 'E') {
                                unget(character);

                                try {
                                    if (floatingPoint) {
                                        myFpConst = Double.parseDouble(new String(myStringConst, 0, index));
                                        return XML_FCONST;
                                    } else {
                                        myIntConst = Long.parseLong(new String(myStringConst, 0, index));
                                        return XML_ICONST;
                                    }
                                } catch (final NumberFormatException details) {
                                    throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                                }
                            }

                            if (index == mySize) {
                                throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                            }

                            myStringConst[index++] = (char) character;

                            if (character == '.') {
                                floatingPoint = true;
                            }

                            character = get();
                        }
                    default:
                        index = 0;

                        while (Character.isLetterOrDigit((char) character) || character == '-' || character == ':' ||
                                character == '_' || character == '.') {
                            if (index == mySize) {
                                throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                            }

                            if (character == '-') {
                                character = '$';
                            }

                            myStringConst[index++] = (char) character;
                            character = get();
                        }

                        unget(character);

                        if (index == 0) {
                            throw new XMLImportException(myLine, myColumn, MessageCodes.SB_001);
                        }

                        myIdent = new String(myStringConst, 0, index);

                        return XML_IDENT;
                }
            }
        }

        final String getIdentifier() {
            return myIdent;
        }

        final String getString() {
            return new String(myStringConst, 0, myStringLen);
        }

        final long getInt() {
            return myIntConst;
        }

        final double getReal() {
            return myFpConst;
        }

        final int getLine() {
            return myLine;
        }

        final int getColumn() {
            return myColumn;
        }
    }
}
