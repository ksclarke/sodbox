
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.Writer;
import java.util.Date;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.StorageError;

@SuppressWarnings("MultipleStringLiterals")
public class XMLExporter {

    static final char HEX_DIGIT[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E',
        'F' };

    private final StorageImpl myStorage;

    private final Writer myWriter;

    private int[] myMarkedBitmap;

    private int[] myExportedBitmap;

    private int[] myCompoundKeyTypes;

    /**
     * Create an XML exporter.
     *
     * @param aStorage A database storage
     * @param aWriter A writer
     */
    public XMLExporter(final StorageImpl aStorage, final Writer aWriter) {
        myStorage = aStorage;
        myWriter = aWriter;
    }

    /**
     * Export database.
     *
     * @param aRootOid A root OID.
     * @throws IOException If there is trouble reading from the database.
     */
    public void exportDatabase(final int aRootOid) throws IOException {
        if (myStorage.myEncoding != null) {
            myWriter.write("<?xml version=\"1.0\" encoding=\"" + myStorage.myEncoding + "\"?>\n");
        } else {
            myWriter.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        }

        myWriter.write("<database root=\"" + aRootOid + "\">\n");
        myExportedBitmap = new int[(myStorage.myCurrentIndexSize + 31) / 32];
        myMarkedBitmap = new int[(myStorage.myCurrentIndexSize + 31) / 32];
        myMarkedBitmap[aRootOid >> 5] |= 1 << (aRootOid & 31);

        int exportedObjectCount;

        do {
            exportedObjectCount = 0;

            for (int index = 0; index < myMarkedBitmap.length; index++) {
                final int mask = myMarkedBitmap[index];

                if (mask != 0) {
                    for (int jndex = 0, bit = 1; jndex < 32; jndex++, bit <<= 1) {
                        if ((mask & bit) != 0) {
                            final int oid = (index << 5) + jndex;

                            myExportedBitmap[index] |= bit;
                            myMarkedBitmap[index] &= ~bit;

                            try {
                                final byte[] obj = myStorage.get(oid);
                                final int typeOid = ObjectHeader.getType(obj, 0);
                                final ClassDescriptor desc = myStorage.findClassDescriptor(typeOid);

                                if (desc.myClass == Btree.class) {
                                    exportIndex(oid, obj, Btree.class.getName());
                                } else if (desc.myClass == PersistentSet.class) {
                                    exportSet(oid, obj);
                                } else if (desc.myClass == BtreeFieldIndex.class) {
                                    exportFieldIndex(oid, obj, BtreeFieldIndex.class.getName());
                                } else if (desc.myClass == BtreeCaseInsensitiveFieldIndex.class) {
                                    exportFieldIndex(oid, obj, BtreeCaseInsensitiveFieldIndex.class.getName());
                                } else if (desc.myClass == BtreeMultiFieldIndex.class) {
                                    exportMultiFieldIndex(oid, obj, BtreeMultiFieldIndex.class.getName());
                                } else if (desc.myClass == BtreeCaseInsensitiveMultiFieldIndex.class) {
                                    exportMultiFieldIndex(oid, obj, BtreeCaseInsensitiveMultiFieldIndex.class
                                            .getName());
                                } else if (desc.myClass == BtreeCompoundIndex.class) {
                                    exportCompoundIndex(oid, obj);
                                } else {
                                    final String className = exportIdentifier(desc.myName);

                                    myWriter.write(" <" + className + " id=\"" + oid + "\">\n");
                                    exportObject(desc, obj, ObjectHeader.SIZE_OF, 2);
                                    myWriter.write(" </" + className + ">\n");
                                }

                                exportedObjectCount += 1;
                            } catch (final StorageError details) {
                                if (myStorage.myListener != null) {
                                    myStorage.myListener.objectNotExported(oid, details);
                                } else {
                                    System.err.println("XML export failed for object " + oid + ": " + details);
                                }
                            }
                        }
                    }
                }
            }
        } while (exportedObjectCount != 0);

        myWriter.write("</database>\n");
        myWriter.flush(); // writer should be closed by calling code
    }

    final String exportIdentifier(final String aName) {
        return aName.replace('$', '-');
    }

    final void exportSet(final int aOid, final byte[] aData) throws IOException {
        final Btree btree = new Btree(aData, ObjectHeader.SIZE_OF);

        myStorage.assignOid(btree, aOid, false);
        myWriter.write(" <" + PersistentSet.class.getName() + " id=\"" + aOid + "\" unique=\"" +
                (btree.isUniqueKeyIndex ? '1' : '0') + "\">\n");
        btree.export(this);
        myWriter.write(" </" + PersistentSet.class.getName() + ">\n");
    }

    final void exportIndex(final int aOid, final byte[] aData, final String aName) throws IOException {
        final Btree btree = new Btree(aData, ObjectHeader.SIZE_OF);

        myStorage.assignOid(btree, aOid, false);
        myWriter.write(" <" + aName + " id=\"" + aOid + "\" unique=\"" + (btree.isUniqueKeyIndex ? '1' : '0') +
                "\" type=\"" + ClassDescriptor.SIGNATURE[btree.myType] + "\">\n");
        btree.export(this);
        myWriter.write(" </" + aName + ">\n");
    }

    final void exportFieldIndex(final int aOid, final byte[] aData, final String aName) throws IOException {
        final Btree btree = new Btree(aData, ObjectHeader.SIZE_OF);
        int offset;

        myStorage.assignOid(btree, aOid, false);
        myWriter.write(" <" + aName + " id=\"" + aOid + "\" unique=\"" + (btree.isUniqueKeyIndex ? '1' : '0') + "\"");

        offset = Btree.SIZE_OF;

        myWriter.write(" autoinc=\"" + Bytes.unpack8(aData, offset) + "\"");
        offset += 8;
        myWriter.write(" class=");
        offset = exportString(aData, offset);
        myWriter.write(" field=");
        offset = exportString(aData, offset);
        myWriter.write(">\n");
        btree.export(this);
        myWriter.write(" </" + aName + ">\n");
    }

    final void exportMultiFieldIndex(final int aOid, final byte[] aData, final String aName) throws IOException {
        final Btree btree = new Btree(aData, ObjectHeader.SIZE_OF);

        myStorage.assignOid(btree, aOid, false);
        myWriter.write(" <" + aName + " id=\"" + aOid + "\" unique=\"" + (btree.isUniqueKeyIndex ? '1' : '0') +
                "\" class=");

        int offset = exportString(aData, Btree.SIZE_OF);

        final int nFields = Bytes.unpack4(aData, offset);

        offset += 4;

        for (int i = 0; i < nFields; i++) {
            myWriter.write(" field" + i + "=");
            offset = exportString(aData, offset);
        }

        myWriter.write(">\n");

        final int nTypes = Bytes.unpack4(aData, offset);

        offset += 4;
        myCompoundKeyTypes = new int[nTypes];

        for (int i = 0; i < nTypes; i++) {
            myCompoundKeyTypes[i] = Bytes.unpack4(aData, offset);
            offset += 4;
        }

        btree.export(this);
        myCompoundKeyTypes = null;
        myWriter.write(" </" + aName + ">\n");
    }

    final void exportCompoundIndex(final int aOid, final byte[] aData) throws IOException {
        final Btree btree = new Btree(aData, ObjectHeader.SIZE_OF);

        myStorage.assignOid(btree, aOid, false);
        myWriter.write(" <info.freelibrary.sodbox.impl.BtreeCompoundIndex id=\"" + aOid + "\" unique=\"" +
                (btree.isUniqueKeyIndex ? '1' : '0') + "\"");

        int offset = Btree.SIZE_OF;
        final int nTypes = Bytes.unpack4(aData, offset);

        offset += 4;
        myCompoundKeyTypes = new int[nTypes];

        for (int index = 0; index < nTypes; index++) {
            final int type = Bytes.unpack4(aData, offset);

            myWriter.write(" type" + index + "=\"" + ClassDescriptor.SIGNATURE[type] + "\"");

            myCompoundKeyTypes[index] = type;
            offset += 4;
        }

        myWriter.write(">\n");
        btree.export(this);
        myCompoundKeyTypes = null;
        myWriter.write(" </info.freelibrary.sodbox.impl.BtreeCompoundIndex>\n");
    }

    final int exportKey(final byte[] aBody, final int aOffset, final int aSize, final int aType) throws IOException {
        int offset = aOffset;

        switch (aType) {
            case ClassDescriptor.TP_BOOLEAN:
                myWriter.write(aBody[offset++] != 0 ? "1" : "0");
                break;
            case ClassDescriptor.TP_BYTE:
                myWriter.write(Integer.toString(aBody[offset++]));
                break;
            case ClassDescriptor.TP_CHAR:
                myWriter.write(Integer.toString((char) Bytes.unpack2(aBody, offset)));
                offset += 2;
                break;
            case ClassDescriptor.TP_SHORT:
                myWriter.write(Integer.toString(Bytes.unpack2(aBody, offset)));
                offset += 2;
                break;
            case ClassDescriptor.TP_INT:
            case ClassDescriptor.TP_OBJECT:
            case ClassDescriptor.TP_ENUM:
                myWriter.write(Integer.toString(Bytes.unpack4(aBody, offset)));
                offset += 4;
                break;
            case ClassDescriptor.TP_LONG:
                myWriter.write(Long.toString(Bytes.unpack8(aBody, offset)));
                offset += 8;
                break;
            case ClassDescriptor.TP_FLOAT:
                myWriter.write(Float.toString(Float.intBitsToFloat(Bytes.unpack4(aBody, offset))));
                offset += 4;
                break;
            case ClassDescriptor.TP_DOUBLE:
                myWriter.write(Double.toString(Double.longBitsToDouble(Bytes.unpack8(aBody, offset))));
                offset += 8;
                break;
            case ClassDescriptor.TP_STRING:
            case ClassDescriptor.TP_CLASS:
                for (int index = 0; index < aSize; index++) {
                    exportChar((char) Bytes.unpack2(aBody, offset));
                    offset += 2;
                }

                break;
            case ClassDescriptor.TP_ARRAY_OF_BYTES:
                for (int index = 0; index < aSize; index++) {
                    final byte singleByte = aBody[offset++];

                    myWriter.write(HEX_DIGIT[(singleByte >>> 4) & 0xF]);
                    myWriter.write(HEX_DIGIT[singleByte & 0xF]);
                }
                break;
            case ClassDescriptor.TP_DATE: {
                final long msec = Bytes.unpack8(aBody, offset);

                offset += 8;

                if (msec >= 0) {
                    myWriter.write(XMLImporter.HTTP_FORMATTER.format(new Date(msec)));
                } else {
                    myWriter.write("null");
                }

                break;
            }
            default:
                Assert.that(false);
        }

        return offset;
    }

    final void exportCompoundKey(final byte[] aBody, final int aOffset, final int aSize, final int aType)
            throws IOException {
        Assert.that(aType == ClassDescriptor.TP_ARRAY_OF_BYTES);

        int offset = aOffset;
        int size = aSize;
        int type = aType;

        final int end = offset + size;

        for (int index = 0; index < myCompoundKeyTypes.length; index++) {
            type = myCompoundKeyTypes[index];

            if (type == ClassDescriptor.TP_ARRAY_OF_BYTES || type == ClassDescriptor.TP_STRING) {
                size = Bytes.unpack4(aBody, offset);
                offset += 4;
            }

            myWriter.write(" key" + index + "=\"");
            offset = exportKey(aBody, offset, size, type);
            myWriter.write("\"");
        }

        Assert.that(offset == end);
    }

    final void exportAssoc(final int aOid, final byte[] aBody, final int aOffset, final int aSize, final int aType)
            throws IOException {
        myWriter.write("  <ref id=\"" + aOid + "\"");

        if ((myExportedBitmap[aOid >> 5] & (1 << (aOid & 31))) == 0) {
            myMarkedBitmap[aOid >> 5] |= 1 << (aOid & 31);
        }

        if (myCompoundKeyTypes != null) {
            exportCompoundKey(aBody, aOffset, aSize, aType);
        } else {
            myWriter.write(" key=\"");
            exportKey(aBody, aOffset, aSize, aType);
            myWriter.write("\"");
        }

        myWriter.write("/>\n");
    }

    final void indentation(final int aIndention) throws IOException {
        int indention = aIndention;

        while (--indention >= 0) {
            myWriter.write(' ');
        }
    }

    final void exportChar(final char aCharacter) throws IOException {
        switch (aCharacter) {
            case '<':
                myWriter.write("&lt;");
                break;
            case '>':
                myWriter.write("&gt;");
                break;
            case '&':
                myWriter.write("&amp;");
                break;
            case '"':
                myWriter.write("&quot;");
                break;
            case '\'':
                myWriter.write("&apos;");
                break;
            default:
                myWriter.write(aCharacter);
        }
    }

    final int exportString(final byte[] aBody, final int aOffset) throws IOException {
        int offset = aOffset;
        int length = Bytes.unpack4(aBody, offset);

        offset += 4;

        if (length >= 0) {
            myWriter.write("\"");

            while (--length >= 0) {
                exportChar((char) Bytes.unpack2(aBody, offset));
                offset += 2;
            }

            myWriter.write("\"");
        } else if (length < -1) {
            myWriter.write("\"");

            final String string;

            if (myStorage.myEncoding != null) {
                string = new String(aBody, offset, -length - 2, myStorage.myEncoding);
            } else {
                string = new String(aBody, offset, -length - 2);
            }

            offset -= length + 2;

            for (int index = 0, n = string.length(); index < n; index++) {
                exportChar(string.charAt(index));
            }

            myWriter.write("\"");
        } else {
            myWriter.write("null");
        }

        return offset;
    }

    final int exportBinary(final byte[] aBody, final int aOffset) throws IOException {
        int offset = aOffset;
        int length = Bytes.unpack4(aBody, offset);

        offset += 4;

        if (length < 0) {
            myWriter.write("null");
        } else {
            myWriter.write('\"');

            while (--length >= 0) {
                final byte singleByte = aBody[offset++];

                myWriter.write(HEX_DIGIT[(singleByte >>> 4) & 0xF]);
                myWriter.write(HEX_DIGIT[singleByte & 0xF]);
            }

            myWriter.write('\"');
        }

        return offset;
    }

    final int exportRef(final byte[] aBody, final int aJOffset, final int aIndention) throws IOException {
        final int oid;

        int offset = aJOffset;

        oid = Bytes.unpack4(aBody, offset);
        offset += 4;

        if (oid < 0) {
            final int tid = -1 - oid;

            switch (tid) {
                case ClassDescriptor.TP_STRING:
                    offset = exportString(aBody, offset);
                    break;
                case ClassDescriptor.TP_CLASS:
                    myWriter.write("<class name=\"");
                    offset = exportString(aBody, offset);
                    myWriter.write("\"/>");
                    break;
                case ClassDescriptor.TP_BOOLEAN:
                case ClassDescriptor.TP_BYTE:
                case ClassDescriptor.TP_CHAR:
                case ClassDescriptor.TP_SHORT:
                case ClassDescriptor.TP_INT:
                case ClassDescriptor.TP_LONG:
                case ClassDescriptor.TP_FLOAT:
                case ClassDescriptor.TP_DOUBLE:
                case ClassDescriptor.TP_DATE:
                case ClassDescriptor.TP_ENUM: {
                    int length = ClassDescriptor.SIZE_OF[tid];

                    myWriter.write("<scalar type=\"" + tid + "\" value=\"");

                    while (--length >= 0) {
                        final byte b = aBody[offset++];

                        myWriter.write(HEX_DIGIT[(b >> 4) & 0xF]);
                        myWriter.write(HEX_DIGIT[b & 0xF]);
                    }

                    myWriter.write("\"/>");
                    break;
                }
                case ClassDescriptor.TP_CUSTOM: {
                    final StorageImpl.ByteArrayObjectInputStream in = myStorage.new ByteArrayObjectInputStream(aBody,
                            offset, null, true, false);
                    final Object obj = myStorage.mySerializer.unpack(in);

                    offset = in.getPosition();
                    myWriter.write("<scalar type=\"" + tid + "\" value=\"");

                    final String s = myStorage.mySerializer.print(obj);

                    for (int index = 0; index < s.length(); index++) {
                        exportChar(s.charAt(index));
                    }

                    myWriter.write("\"/>");
                    break;
                }
                default:
                    if (tid >= ClassDescriptor.TP_VALUE_TYPE_BIAS) {
                        final int typeOid = -ClassDescriptor.TP_VALUE_TYPE_BIAS - oid;
                        final ClassDescriptor desc = myStorage.findClassDescriptor(typeOid);

                        if (desc.isCollection) {
                            final int len = Bytes.unpack4(aBody, offset);

                            offset += 4;

                            final String className = exportIdentifier(desc.myName);

                            myWriter.write("\n");
                            indentation(aIndention + 1);
                            myWriter.write("<" + className + ">\n");

                            for (int index = 0; index < len; index++) {
                                indentation(aIndention + 2);
                                myWriter.write("<element>");
                                offset = exportRef(aBody, offset, aIndention + 2);
                                myWriter.write("</element>");
                            }

                            indentation(aIndention + 1);
                            myWriter.write("</" + className + ">\n");
                            indentation(aIndention);
                        } else {
                            final String className = exportIdentifier(desc.myName);

                            myWriter.write("\n");
                            indentation(aIndention + 1);
                            myWriter.write("<" + className + ">\n");
                            offset = exportObject(desc, aBody, offset, aIndention + 2);
                            indentation(aIndention + 1);
                            myWriter.write("</" + className + ">\n");
                            indentation(aIndention);
                        }
                    } else {
                        throw new StorageError(StorageError.UNSUPPORTED_TYPE);
                    }
            }
        } else {
            myWriter.write("<ref id=\"" + oid + "\"/>");

            if (oid != 0 && (myExportedBitmap[oid >> 5] & (1 << (oid & 31))) == 0) {
                myMarkedBitmap[oid >> 5] |= 1 << (oid & 31);
            }
        }

        return offset;
    }

    final int exportObject(final ClassDescriptor aDescriptor, final byte[] aBody, final int aOffset,
            final int aIndention) throws IOException {
        final ClassDescriptor.FieldDescriptor[] all = aDescriptor.myFields;

        int offset = aOffset;

        for (int index = 0, count = all.length; index < count; index++) {
            final ClassDescriptor.FieldDescriptor fieldDescriptor = all[index];

            indentation(aIndention);

            final String fieldName = exportIdentifier(fieldDescriptor.myFieldName);

            myWriter.write("<" + fieldName + ">");

            switch (fieldDescriptor.myType) {
                case ClassDescriptor.TP_BOOLEAN:
                    myWriter.write(aBody[offset++] != 0 ? "1" : "0");
                    break;
                case ClassDescriptor.TP_BYTE:
                    myWriter.write(Integer.toString(aBody[offset++]));
                    break;
                case ClassDescriptor.TP_CHAR:
                    myWriter.write(Integer.toString((char) Bytes.unpack2(aBody, offset)));
                    offset += 2;
                    break;
                case ClassDescriptor.TP_SHORT:
                    myWriter.write(Integer.toString(Bytes.unpack2(aBody, offset)));
                    offset += 2;
                    break;
                case ClassDescriptor.TP_INT:
                    myWriter.write(Integer.toString(Bytes.unpack4(aBody, offset)));
                    offset += 4;
                    break;
                case ClassDescriptor.TP_LONG:
                    myWriter.write(Long.toString(Bytes.unpack8(aBody, offset)));
                    offset += 8;
                    break;
                case ClassDescriptor.TP_FLOAT:
                    myWriter.write(Float.toString(Float.intBitsToFloat(Bytes.unpack4(aBody, offset))));
                    offset += 4;
                    break;
                case ClassDescriptor.TP_DOUBLE:
                    myWriter.write(Double.toString(Double.longBitsToDouble(Bytes.unpack8(aBody, offset))));
                    offset += 8;
                    break;
                case ClassDescriptor.TP_ENUM: {
                    final int ordinal = Bytes.unpack4(aBody, offset);

                    if (ordinal < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write("\"" + ((Enum) fieldDescriptor.myField.getType().getEnumConstants()[ordinal])
                                .name() + "\"");
                    }

                    offset += 4;
                    break;
                }
                case ClassDescriptor.TP_STRING:
                case ClassDescriptor.TP_CLASS:
                    offset = exportString(aBody, offset);
                    break;
                case ClassDescriptor.TP_DATE: {
                    final long msec = Bytes.unpack8(aBody, offset);
                    offset += 8;

                    if (msec >= 0) {
                        myWriter.write("\"" + XMLImporter.HTTP_FORMATTER.format(new Date(msec)) + "\"");
                    } else {
                        myWriter.write("null");
                    }

                    break;
                }
                case ClassDescriptor.TP_OBJECT:
                    offset = exportRef(aBody, offset, aIndention);
                    break;
                case ClassDescriptor.TP_VALUE:
                    myWriter.write('\n');
                    offset = exportObject(fieldDescriptor.myClassDescriptor, aBody, offset, aIndention + 1);
                    indentation(aIndention);
                    break;
                case ClassDescriptor.TP_RAW:
                case ClassDescriptor.TP_ARRAY_OF_BYTES:
                    offset = exportBinary(aBody, offset);
                    break;
                case ClassDescriptor.TP_CUSTOM: {
                    final StorageImpl.ByteArrayObjectInputStream in = myStorage.new ByteArrayObjectInputStream(aBody,
                            offset, null, true, false);
                    final Object obj = myStorage.mySerializer.unpack(in);

                    offset = in.getPosition();
                    myWriter.write("\"");

                    final String s = myStorage.mySerializer.print(obj);

                    for (int j = 0; j < s.length(); j++) {
                        exportChar(s.charAt(j));
                    }

                    myWriter.write("\"");
                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_BOOLEANS: {
                    int length = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (length < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        while (--length >= 0) {
                            indentation(aIndention + 1);
                            myWriter.write("<element>" + (aBody[offset++] != 0 ? "1" : "0") + "</element>\n");
                        }

                        indentation(aIndention);
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_CHARS: {
                    int length = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (length < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        while (--length >= 0) {
                            indentation(aIndention + 1);
                            myWriter.write("<element>" + (Bytes.unpack2(aBody, offset) & 0xFFFF) + "</element>\n");
                            offset += 2;
                        }

                        indentation(aIndention);
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_SHORTS: {
                    int length = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (length < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        while (--length >= 0) {
                            indentation(aIndention + 1);
                            myWriter.write("<element>" + Bytes.unpack2(aBody, offset) + "</element>\n");
                            offset += 2;
                        }

                        indentation(aIndention);
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_ENUMS: {
                    int length = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (length < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        final Enum[] enumConstants = (Enum[]) fieldDescriptor.myField.getType().getEnumConstants();

                        while (--length >= 0) {
                            indentation(aIndention + 1);

                            final int ordinal = Bytes.unpack4(aBody, offset);

                            if (ordinal < 0) {
                                myWriter.write("null");
                            } else {
                                myWriter.write("<element>\"" + enumConstants[ordinal].name() + "\"</element>\n");
                            }

                            offset += 4;
                        }

                        indentation(aIndention);
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_INTS: {
                    int len = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (len < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        while (--len >= 0) {
                            indentation(aIndention + 1);
                            myWriter.write("<element>" + Bytes.unpack4(aBody, offset) + "</element>\n");
                            offset += 4;
                        }

                        indentation(aIndention);
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_LONGS: {
                    int length = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (length < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        while (--length >= 0) {
                            indentation(aIndention + 1);
                            myWriter.write("<element>" + Bytes.unpack8(aBody, offset) + "</element>\n");
                            offset += 8;
                        }

                        indentation(aIndention);
                    }

                    break;
                }
                case ClassDescriptor.TO_ARRAY_OF_FLOATS: {
                    int length = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (length < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        while (--length >= 0) {
                            indentation(aIndention + 1);
                            myWriter.write("<element>" + Float.intBitsToFloat(Bytes.unpack4(aBody, offset)) +
                                    "</element>\n");
                            offset += 4;
                        }

                        indentation(aIndention);
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_DOUBLES: {
                    int length = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (length < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        while (--length >= 0) {
                            indentation(aIndention + 1);
                            myWriter.write("<element>" + Double.longBitsToDouble(Bytes.unpack8(aBody, offset)) +
                                    "</element>\n");
                            offset += 8;
                        }

                        indentation(aIndention);
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_DATES: {
                    int length = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (length < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        while (--length >= 0) {
                            indentation(aIndention + 1);

                            final long msec = Bytes.unpack8(aBody, offset);

                            offset += 8;

                            if (msec >= 0) {
                                myWriter.write("<element>\"");
                                myWriter.write(XMLImporter.HTTP_FORMATTER.format(new Date(msec)));
                                myWriter.write("\"</element>\n");
                            } else {
                                myWriter.write("<element>null</element>\n");
                            }
                        }
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_STRINGS: {
                    int length = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (length < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        while (--length >= 0) {
                            indentation(aIndention + 1);
                            myWriter.write("<element>");
                            offset = exportString(aBody, offset);
                            myWriter.write("</element>\n");
                        }

                        indentation(aIndention);
                    }

                    break;
                }
                case ClassDescriptor.TP_LINK:
                case ClassDescriptor.TP_ARRAY_OF_OBJECTS: {
                    int length = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (length < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        while (--length >= 0) {
                            indentation(aIndention + 1);
                            myWriter.write("<element>");
                            offset = exportRef(aBody, offset, aIndention + 1);
                            myWriter.write("</element>\n");
                        }

                        indentation(aIndention);
                    }

                    break;
                }
                case ClassDescriptor.TP_ARRAY_OF_VALUES: {
                    int length = Bytes.unpack4(aBody, offset);

                    offset += 4;

                    if (length < 0) {
                        myWriter.write("null");
                    } else {
                        myWriter.write('\n');

                        while (--length >= 0) {
                            indentation(aIndention + 1);
                            myWriter.write("<element>\n");
                            offset = exportObject(fieldDescriptor.myClassDescriptor, aBody, offset, aIndention + 2);
                            indentation(aIndention + 1);
                            myWriter.write("</element>\n");
                        }

                        indentation(aIndention);
                    }

                    break;
                }
                default:
                    // FIXME: Do something here
            }

            myWriter.write("</" + fieldName + ">\n");
        }

        return offset;
    }
}
