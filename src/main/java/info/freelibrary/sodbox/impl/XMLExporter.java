
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.Writer;
import java.util.Date;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.StorageError;
import info.freelibrary.sodbox.impl.ClassDescriptor.FieldDescriptor;

public class XMLExporter {

    static final char hexDigit[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    private final StorageImpl storage;

    private final Writer writer;

    private int[] markedBitmap;

    private int[] exportedBitmap;

    private int[] compoundKeyTypes;

    public XMLExporter(final StorageImpl storage, final Writer writer) {
        this.storage = storage;
        this.writer = writer;
    }

    final void exportAssoc(final int oid, final byte[] body, final int offs, final int size, final int type)
            throws IOException {
        writer.write("  <ref id=\"" + oid + "\"");
        if ((exportedBitmap[oid >> 5] & 1 << (oid & 31)) == 0) {
            markedBitmap[oid >> 5] |= 1 << (oid & 31);
        }
        if (compoundKeyTypes != null) {
            exportCompoundKey(body, offs, size, type);
        } else {
            writer.write(" key=\"");
            exportKey(body, offs, size, type);
            writer.write("\"");
        }
        writer.write("/>\n");
    }

    final int exportBinary(final byte[] body, int offs) throws IOException {
        int len = Bytes.unpack4(body, offs);
        offs += 4;
        if (len < 0) {
            writer.write("null");
        } else {
            writer.write('\"');
            while (--len >= 0) {
                final byte b = body[offs++];
                writer.write(hexDigit[b >>> 4 & 0xF]);
                writer.write(hexDigit[b & 0xF]);
            }
            writer.write('\"');
        }
        return offs;
    }

    final void exportChar(final char ch) throws IOException {
        switch (ch) {
            case '<':
                writer.write("&lt;");
                break;
            case '>':
                writer.write("&gt;");
                break;
            case '&':
                writer.write("&amp;");
                break;
            case '"':
                writer.write("&quot;");
                break;
            case '\'':
                writer.write("&apos;");
                break;
            default:
                writer.write(ch);
        }
    }

    final void exportCompoundIndex(final int oid, final byte[] data) throws IOException {
        final Btree btree = new Btree(data, ObjectHeader.sizeof);
        storage.assignOid(btree, oid, false);
        writer.write(" <info.freelibrary.sodbox.impl.BtreeCompoundIndex id=\"" + oid + "\" unique=\"" + (btree.unique
                ? '1' : '0') + "\"");
        int offs = Btree.sizeof;
        final int nTypes = Bytes.unpack4(data, offs);
        offs += 4;
        compoundKeyTypes = new int[nTypes];
        for (int i = 0; i < nTypes; i++) {
            final int type = Bytes.unpack4(data, offs);
            writer.write(" type" + i + "=\"" + ClassDescriptor.signature[type] + "\"");

            compoundKeyTypes[i] = type;
            offs += 4;
        }
        writer.write(">\n");
        btree.export(this);
        compoundKeyTypes = null;
        writer.write(" </info.freelibrary.sodbox.impl.BtreeCompoundIndex>\n");
    }

    final void exportCompoundKey(final byte[] body, int offs, int size, int type) throws IOException {
        Assert.that(type == ClassDescriptor.tpArrayOfByte);
        final int end = offs + size;
        for (int i = 0; i < compoundKeyTypes.length; i++) {
            type = compoundKeyTypes[i];
            if (type == ClassDescriptor.tpArrayOfByte || type == ClassDescriptor.tpString) {
                size = Bytes.unpack4(body, offs);
                offs += 4;
            }
            writer.write(" key" + i + "=\"");
            offs = exportKey(body, offs, size, type);
            writer.write("\"");
        }
        Assert.that(offs == end);
    }

    public void exportDatabase(final int rootOid) throws IOException {
        if (storage.myEncoding != null) {
            writer.write("<?xml version=\"1.0\" encoding=\"" + storage.myEncoding + "\"?>\n");
        } else {
            writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        }
        writer.write("<database root=\"" + rootOid + "\">\n");
        exportedBitmap = new int[(storage.myCurrentIndexSize + 31) / 32];
        markedBitmap = new int[(storage.myCurrentIndexSize + 31) / 32];
        markedBitmap[rootOid >> 5] |= 1 << (rootOid & 31);
        int nExportedObjects;
        do {
            nExportedObjects = 0;
            for (int i = 0; i < markedBitmap.length; i++) {
                final int mask = markedBitmap[i];
                if (mask != 0) {
                    for (int j = 0, bit = 1; j < 32; j++, bit <<= 1) {
                        if ((mask & bit) != 0) {
                            final int oid = (i << 5) + j;
                            exportedBitmap[i] |= bit;
                            markedBitmap[i] &= ~bit;
                            try {
                                final byte[] obj = storage.get(oid);
                                final int typeOid = ObjectHeader.getType(obj, 0);
                                final ClassDescriptor desc = storage.findClassDescriptor(typeOid);
                                if (desc.cls == Btree.class) {
                                    exportIndex(oid, obj, "info.freelibrary.sodbox.impl.Btree");
                                } else if (desc.cls == PersistentSet.class) {
                                    exportSet(oid, obj);
                                } else if (desc.cls == BtreeFieldIndex.class) {
                                    exportFieldIndex(oid, obj, "info.freelibrary.sodbox.impl.BtreeFieldIndex");
                                } else if (desc.cls == BtreeCaseInsensitiveFieldIndex.class) {
                                    exportFieldIndex(oid, obj,
                                            "info.freelibrary.sodbox.impl.BtreeCaseInsensitiveFieldIndex");
                                } else if (desc.cls == BtreeMultiFieldIndex.class) {
                                    exportMultiFieldIndex(oid, obj,
                                            "info.freelibrary.sodbox.impl.BtreeMultiFieldIndex");
                                } else if (desc.cls == BtreeCaseInsensitiveMultiFieldIndex.class) {
                                    exportMultiFieldIndex(oid, obj,
                                            "info.freelibrary.sodbox.impl.BtreeCaseInsensitiveMultiFieldIndex");
                                } else if (desc.cls == BtreeCompoundIndex.class) {
                                    exportCompoundIndex(oid, obj);
                                } else {
                                    final String className = exportIdentifier(desc.name);
                                    writer.write(" <" + className + " id=\"" + oid + "\">\n");
                                    exportObject(desc, obj, ObjectHeader.sizeof, 2);
                                    writer.write(" </" + className + ">\n");
                                }
                                nExportedObjects += 1;
                            } catch (final StorageError x) {
                                if (storage.myListener != null) {
                                    storage.myListener.objectNotExported(oid, x);
                                } else {
                                    System.err.println("XML export failed for object " + oid + ": " + x);
                                }
                            }
                        }
                    }
                }
            }
        } while (nExportedObjects != 0);
        writer.write("</database>\n");
        writer.flush(); // writer should be closed by calling code
    }

    final void exportFieldIndex(final int oid, final byte[] data, final String name) throws IOException {
        final Btree btree = new Btree(data, ObjectHeader.sizeof);
        storage.assignOid(btree, oid, false);
        writer.write(" <" + name + " id=\"" + oid + "\" unique=\"" + (btree.unique ? '1' : '0') + "\"");
        int offs = Btree.sizeof;
        writer.write(" autoinc=\"" + Bytes.unpack8(data, offs) + "\"");
        offs += 8;
        writer.write(" class=");
        offs = exportString(data, offs);
        writer.write(" field=");
        offs = exportString(data, offs);
        writer.write(">\n");
        btree.export(this);
        writer.write(" </" + name + ">\n");
    }

    final String exportIdentifier(final String name) {
        return name.replace('$', '-');
    }

    final void exportIndex(final int oid, final byte[] data, final String name) throws IOException {
        final Btree btree = new Btree(data, ObjectHeader.sizeof);
        storage.assignOid(btree, oid, false);
        writer.write(" <" + name + " id=\"" + oid + "\" unique=\"" + (btree.unique ? '1' : '0') + "\" type=\"" +
                ClassDescriptor.signature[btree.type] + "\">\n");
        btree.export(this);
        writer.write(" </" + name + ">\n");
    }

    final int exportKey(final byte[] body, int offs, final int size, final int type) throws IOException {
        switch (type) {
            case ClassDescriptor.tpBoolean:
                writer.write(body[offs++] != 0 ? "1" : "0");
                break;
            case ClassDescriptor.tpByte:
                writer.write(Integer.toString(body[offs++]));
                break;
            case ClassDescriptor.tpChar:
                writer.write(Integer.toString((char) Bytes.unpack2(body, offs)));
                offs += 2;
                break;
            case ClassDescriptor.tpShort:
                writer.write(Integer.toString(Bytes.unpack2(body, offs)));
                offs += 2;
                break;
            case ClassDescriptor.tpInt:
            case ClassDescriptor.tpObject:
            case ClassDescriptor.tpEnum:
                writer.write(Integer.toString(Bytes.unpack4(body, offs)));
                offs += 4;
                break;
            case ClassDescriptor.tpLong:
                writer.write(Long.toString(Bytes.unpack8(body, offs)));
                offs += 8;
                break;
            case ClassDescriptor.tpFloat:
                writer.write(Float.toString(Float.intBitsToFloat(Bytes.unpack4(body, offs))));
                offs += 4;
                break;
            case ClassDescriptor.tpDouble:
                writer.write(Double.toString(Double.longBitsToDouble(Bytes.unpack8(body, offs))));
                offs += 8;
                break;
            case ClassDescriptor.tpString:
            case ClassDescriptor.tpClass:
                for (int i = 0; i < size; i++) {
                    exportChar((char) Bytes.unpack2(body, offs));
                    offs += 2;
                }
                break;
            case ClassDescriptor.tpArrayOfByte:
                for (int i = 0; i < size; i++) {
                    final byte b = body[offs++];
                    writer.write(hexDigit[b >>> 4 & 0xF]);
                    writer.write(hexDigit[b & 0xF]);
                }
                break;
            case ClassDescriptor.tpDate: {
                final long msec = Bytes.unpack8(body, offs);
                offs += 8;
                if (msec >= 0) {
                    writer.write(XMLImporter.httpFormatter.format(new Date(msec)));
                } else {
                    writer.write("null");
                }
                break;
            }
            default:
                Assert.that(false);
        }
        return offs;
    }

    final void exportMultiFieldIndex(final int oid, final byte[] data, final String name) throws IOException {
        final Btree btree = new Btree(data, ObjectHeader.sizeof);
        storage.assignOid(btree, oid, false);
        writer.write(" <" + name + " id=\"" + oid + "\" unique=\"" + (btree.unique ? '1' : '0') + "\" class=");
        int offs = exportString(data, Btree.sizeof);
        final int nFields = Bytes.unpack4(data, offs);
        offs += 4;
        for (int i = 0; i < nFields; i++) {
            writer.write(" field" + i + "=");
            offs = exportString(data, offs);
        }
        writer.write(">\n");
        final int nTypes = Bytes.unpack4(data, offs);
        offs += 4;
        compoundKeyTypes = new int[nTypes];
        for (int i = 0; i < nTypes; i++) {
            compoundKeyTypes[i] = Bytes.unpack4(data, offs);
            offs += 4;
        }
        btree.export(this);
        compoundKeyTypes = null;
        writer.write(" </" + name + ">\n");
    }

    final int exportObject(final ClassDescriptor desc, final byte[] body, int offs, final int indent)
            throws IOException {
        final ClassDescriptor.FieldDescriptor[] all = desc.allFields;

        for (final FieldDescriptor fd : all) {
            indentation(indent);
            final String fieldName = exportIdentifier(fd.fieldName);
            writer.write("<" + fieldName + ">");
            switch (fd.type) {
                case ClassDescriptor.tpBoolean:
                    writer.write(body[offs++] != 0 ? "1" : "0");
                    break;
                case ClassDescriptor.tpByte:
                    writer.write(Integer.toString(body[offs++]));
                    break;
                case ClassDescriptor.tpChar:
                    writer.write(Integer.toString((char) Bytes.unpack2(body, offs)));
                    offs += 2;
                    break;
                case ClassDescriptor.tpShort:
                    writer.write(Integer.toString(Bytes.unpack2(body, offs)));
                    offs += 2;
                    break;
                case ClassDescriptor.tpInt:
                    writer.write(Integer.toString(Bytes.unpack4(body, offs)));
                    offs += 4;
                    break;
                case ClassDescriptor.tpLong:
                    writer.write(Long.toString(Bytes.unpack8(body, offs)));
                    offs += 8;
                    break;
                case ClassDescriptor.tpFloat:
                    writer.write(Float.toString(Float.intBitsToFloat(Bytes.unpack4(body, offs))));
                    offs += 4;
                    break;
                case ClassDescriptor.tpDouble:
                    writer.write(Double.toString(Double.longBitsToDouble(Bytes.unpack8(body, offs))));
                    offs += 8;
                    break;
                case ClassDescriptor.tpEnum: {
                    final int ordinal = Bytes.unpack4(body, offs);
                    if (ordinal < 0) {
                        writer.write("null");
                    } else {
                        writer.write("\"" + ((Enum) fd.field.getType().getEnumConstants()[ordinal]).name() + "\"");
                    }
                    offs += 4;
                    break;
                }
                case ClassDescriptor.tpString:
                case ClassDescriptor.tpClass:
                    offs = exportString(body, offs);
                    break;
                case ClassDescriptor.tpDate: {
                    final long msec = Bytes.unpack8(body, offs);
                    offs += 8;
                    if (msec >= 0) {
                        writer.write("\"" + XMLImporter.httpFormatter.format(new Date(msec)) + "\"");
                    } else {
                        writer.write("null");
                    }
                    break;
                }
                case ClassDescriptor.tpObject:
                    offs = exportRef(body, offs, indent);
                    break;
                case ClassDescriptor.tpValue:
                    writer.write('\n');
                    offs = exportObject(fd.valueDesc, body, offs, indent + 1);
                    indentation(indent);
                    break;
                case ClassDescriptor.tpRaw:
                case ClassDescriptor.tpArrayOfByte:
                    offs = exportBinary(body, offs);
                    break;
                case ClassDescriptor.tpCustom: {
                    final StorageImpl.ByteArrayObjectInputStream in = storage.new ByteArrayObjectInputStream(body,
                            offs, null, true, false);
                    final Object obj = storage.mySerializer.unpack(in);
                    offs = in.getPosition();
                    writer.write("\"");
                    final String s = storage.mySerializer.print(obj);
                    for (int j = 0; j < s.length(); j++) {
                        exportChar(s.charAt(j));
                    }
                    writer.write("\"");
                    break;
                }
                case ClassDescriptor.tpArrayOfBoolean: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) {
                            indentation(indent + 1);
                            writer.write("<element>" + (body[offs++] != 0 ? "1" : "0") + "</element>\n");
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfChar: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) {
                            indentation(indent + 1);
                            writer.write("<element>" + (Bytes.unpack2(body, offs) & 0xFFFF) + "</element>\n");
                            offs += 2;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfShort: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) {
                            indentation(indent + 1);
                            writer.write("<element>" + Bytes.unpack2(body, offs) + "</element>\n");
                            offs += 2;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfEnum: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        final Enum[] enumConstants = (Enum[]) fd.field.getType().getEnumConstants();
                        while (--len >= 0) {
                            indentation(indent + 1);
                            final int ordinal = Bytes.unpack4(body, offs);
                            if (ordinal < 0) {
                                writer.write("null");
                            } else {
                                writer.write("<element>\"" + enumConstants[ordinal].name() + "\"</element>\n");
                            }
                            offs += 4;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfInt: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) {
                            indentation(indent + 1);
                            writer.write("<element>" + Bytes.unpack4(body, offs) + "</element>\n");
                            offs += 4;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfLong: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) {
                            indentation(indent + 1);
                            writer.write("<element>" + Bytes.unpack8(body, offs) + "</element>\n");
                            offs += 8;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfFloat: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) {
                            indentation(indent + 1);
                            writer.write("<element>" + Float.intBitsToFloat(Bytes.unpack4(body, offs)) +
                                    "</element>\n");
                            offs += 4;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfDouble: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) {
                            indentation(indent + 1);
                            writer.write("<element>" + Double.longBitsToDouble(Bytes.unpack8(body, offs)) +
                                    "</element>\n");
                            offs += 8;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfDate: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) {
                            indentation(indent + 1);
                            final long msec = Bytes.unpack8(body, offs);
                            offs += 8;
                            if (msec >= 0) {
                                writer.write("<element>\"");
                                writer.write(XMLImporter.httpFormatter.format(new Date(msec)));
                                writer.write("\"</element>\n");
                            } else {
                                writer.write("<element>null</element>\n");
                            }
                        }
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfString: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) {
                            indentation(indent + 1);
                            writer.write("<element>");
                            offs = exportString(body, offs);
                            writer.write("</element>\n");
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpLink:
                case ClassDescriptor.tpArrayOfObject: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) {
                            indentation(indent + 1);
                            writer.write("<element>");
                            offs = exportRef(body, offs, indent + 1);
                            writer.write("</element>\n");
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfValue: {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) {
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) {
                            indentation(indent + 1);
                            writer.write("<element>\n");
                            offs = exportObject(fd.valueDesc, body, offs, indent + 2);
                            indentation(indent + 1);
                            writer.write("</element>\n");
                        }
                        indentation(indent);
                    }
                    break;
                }
            }
            writer.write("</" + fieldName + ">\n");
        }
        return offs;
    }

    final int exportRef(final byte[] body, int offs, final int indent) throws IOException {
        final int oid = Bytes.unpack4(body, offs);
        offs += 4;
        if (oid < 0) {
            final int tid = -1 - oid;
            switch (tid) {
                case ClassDescriptor.tpString:
                    offs = exportString(body, offs);
                    break;
                case ClassDescriptor.tpClass:
                    writer.write("<class name=\"");
                    offs = exportString(body, offs);
                    writer.write("\"/>");
                    break;
                case ClassDescriptor.tpBoolean:
                case ClassDescriptor.tpByte:
                case ClassDescriptor.tpChar:
                case ClassDescriptor.tpShort:
                case ClassDescriptor.tpInt:
                case ClassDescriptor.tpLong:
                case ClassDescriptor.tpFloat:
                case ClassDescriptor.tpDouble:
                case ClassDescriptor.tpDate:
                case ClassDescriptor.tpEnum: {
                    int len = ClassDescriptor.sizeof[tid];
                    writer.write("<scalar type=\"" + tid + "\" value=\"");
                    while (--len >= 0) {
                        final byte b = body[offs++];
                        writer.write(hexDigit[b >> 4 & 0xF]);
                        writer.write(hexDigit[b & 0xF]);
                    }
                    writer.write("\"/>");
                    break;
                }
                case ClassDescriptor.tpCustom: {
                    final StorageImpl.ByteArrayObjectInputStream in = storage.new ByteArrayObjectInputStream(body,
                            offs, null, true, false);
                    final Object obj = storage.mySerializer.unpack(in);
                    offs = in.getPosition();
                    writer.write("<scalar type=\"" + tid + "\" value=\"");
                    final String s = storage.mySerializer.print(obj);
                    for (int i = 0; i < s.length(); i++) {
                        exportChar(s.charAt(i));
                    }
                    writer.write("\"/>");
                    break;
                }
                default:
                    if (tid >= ClassDescriptor.tpValueTypeBias) {
                        final int typeOid = -ClassDescriptor.tpValueTypeBias - oid;
                        final ClassDescriptor desc = storage.findClassDescriptor(typeOid);
                        if (desc.isCollection) {
                            final int len = Bytes.unpack4(body, offs);
                            offs += 4;
                            final String className = exportIdentifier(desc.name);
                            writer.write("\n");
                            indentation(indent + 1);
                            writer.write("<" + className + ">\n");
                            for (int i = 0; i < len; i++) {
                                indentation(indent + 2);
                                writer.write("<element>");
                                offs = exportRef(body, offs, indent + 2);
                                writer.write("</element>");
                            }
                            indentation(indent + 1);
                            writer.write("</" + className + ">\n");
                            indentation(indent);
                        } else {
                            final String className = exportIdentifier(desc.name);
                            writer.write("\n");
                            indentation(indent + 1);
                            writer.write("<" + className + ">\n");
                            offs = exportObject(desc, body, offs, indent + 2);
                            indentation(indent + 1);
                            writer.write("</" + className + ">\n");
                            indentation(indent);
                        }
                    } else {
                        throw new StorageError(StorageError.UNSUPPORTED_TYPE);
                    }
            }
        } else {
            writer.write("<ref id=\"" + oid + "\"/>");
            if (oid != 0 && (exportedBitmap[oid >> 5] & 1 << (oid & 31)) == 0) {
                markedBitmap[oid >> 5] |= 1 << (oid & 31);
            }
        }
        return offs;
    }

    final void exportSet(final int oid, final byte[] data) throws IOException {
        final Btree btree = new Btree(data, ObjectHeader.sizeof);
        storage.assignOid(btree, oid, false);
        writer.write(" <info.freelibrary.sodbox.impl.PersistentSet id=\"" + oid + "\" unique=\"" + (btree.unique ? '1'
                : '0') + "\">\n");
        btree.export(this);
        writer.write(" </info.freelibrary.sodbox.impl.PersistentSet>\n");
    }

    final int exportString(final byte[] body, int offs) throws IOException {
        int len = Bytes.unpack4(body, offs);
        offs += 4;
        if (len >= 0) {
            writer.write("\"");
            while (--len >= 0) {
                exportChar((char) Bytes.unpack2(body, offs));
                offs += 2;
            }
            writer.write("\"");
        } else if (len < -1) {
            writer.write("\"");
            String s;
            if (storage.myEncoding != null) {
                s = new String(body, offs, -len - 2, storage.myEncoding);
            } else {
                s = new String(body, offs, -len - 2);
            }
            offs -= len + 2;
            for (int i = 0, n = s.length(); i < n; i++) {
                exportChar(s.charAt(i));
            }
            writer.write("\"");
        } else {
            writer.write("null");
        }
        return offs;
    }

    final void indentation(int indent) throws IOException {
        while (--indent >= 0) {
            writer.write(' ');
        }
    }
}
