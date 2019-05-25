
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Key;

class BtreeKey {

    Key myKey;

    int myOID;

    int myOldOID;

    BtreeKey(final Key aKey, final int aOID) {
        myKey = aKey;
        myOID = aOID;
    }

    final void getStr(final Page aPage, final int aIndex) {
        final int len = BtreePage.getKeyStrSize(aPage, aIndex);
        final char[] chars = new char[len];

        int offs = BtreePage.FIRST_KEY_OFFSET + BtreePage.getKeyStrOffs(aPage, aIndex);

        for (int j = 0; j < len; j++) {
            chars[j] = (char) Bytes.unpack2(aPage.myData, offs);
            offs += 2;
        }

        myKey = new Key(chars);
    }

    final void getByteArray(final Page aPage, final int aIndex) {
        final int len = BtreePage.getKeyStrSize(aPage, aIndex);
        final int offs = BtreePage.FIRST_KEY_OFFSET + BtreePage.getKeyStrOffs(aPage, aIndex);
        final byte[] bval = new byte[len];

        System.arraycopy(aPage.myData, offs, bval, 0, len);

        myKey = new Key(bval);
    }

    final void extract(final Page aPage, final int aOffset, final int aType) {
        final byte[] data = aPage.myData;

        switch (aType) {
            case ClassDescriptor.TP_BOOLEAN:
                myKey = new Key(data[aOffset] != 0);
                break;
            case ClassDescriptor.TP_BYTE:
                myKey = new Key(data[aOffset]);
                break;
            case ClassDescriptor.TP_SHORT:
                myKey = new Key(Bytes.unpack2(data, aOffset));
                break;
            case ClassDescriptor.TP_CHAR:
                myKey = new Key((char) Bytes.unpack2(data, aOffset));
                break;
            case ClassDescriptor.TP_INT:
            case ClassDescriptor.TP_OBJECT:
            case ClassDescriptor.TP_ENUM:
                myKey = new Key(Bytes.unpack4(data, aOffset));
                break;
            case ClassDescriptor.TP_LONG:
            case ClassDescriptor.TP_DATE:
                myKey = new Key(Bytes.unpack8(data, aOffset));
                break;
            case ClassDescriptor.TP_FLOAT:
                myKey = new Key(Bytes.unpackF4(data, aOffset));
                break;
            case ClassDescriptor.TP_DOUBLE:
                myKey = new Key(Bytes.unpackF8(data, aOffset));
                break;
            default:
                Assert.failed("Invalid type: " + aType);
        }
    }

    final void pack(final Page aPage, final int aIndex) {
        final byte[] dst = aPage.myData;

        switch (myKey.myType) {
            case ClassDescriptor.TP_BOOLEAN:
            case ClassDescriptor.TP_BYTE:
                dst[BtreePage.FIRST_KEY_OFFSET + aIndex] = (byte) myKey.myIntValue;
                break;
            case ClassDescriptor.TP_SHORT:
            case ClassDescriptor.TP_CHAR:
                Bytes.pack2(dst, BtreePage.FIRST_KEY_OFFSET + aIndex * 2, (short) myKey.myIntValue);
                break;
            case ClassDescriptor.TP_INT:
            case ClassDescriptor.TP_OBJECT:
            case ClassDescriptor.TP_ENUM:
                Bytes.pack4(dst, BtreePage.FIRST_KEY_OFFSET + aIndex * 4, myKey.myIntValue);
                break;
            case ClassDescriptor.TP_LONG:
            case ClassDescriptor.TP_DATE:
                Bytes.pack8(dst, BtreePage.FIRST_KEY_OFFSET + aIndex * 8, myKey.myLongValue);
                break;
            case ClassDescriptor.TP_FLOAT:
                Bytes.pack4(dst, BtreePage.FIRST_KEY_OFFSET + aIndex * 4, Float.floatToIntBits(
                        (float) myKey.myDoubleValue));
                break;
            case ClassDescriptor.TP_DOUBLE:
                Bytes.pack8(dst, BtreePage.FIRST_KEY_OFFSET + aIndex * 8, Double.doubleToLongBits(myKey.myDoubleValue));
                break;
            default:
                Assert.failed("Invalid type - " + myKey.myType);
        }

        Bytes.pack4(dst, BtreePage.FIRST_KEY_OFFSET + (BtreePage.MAX_ITEMS - aIndex - 1) * 4, myOID);
    }

}
