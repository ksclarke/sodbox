
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.Assert;

public class Compressor {

    private final byte[] myBytes;

    private byte myAcc;

    private int myPosition;

    private int myBtg;

    /**
     * Creates a compressor.
     *
     * @param aBytes Bytes to compress
     */
    public Compressor(final byte[] aBytes) {
        myBytes = aBytes;
    }

    /**
     * Encodes starting point.
     */
    public final void encodeStart() {
        myBtg = 8;
        myAcc = 0;
        myPosition = 0;
    }

    private void encodeBit(final int aBit) {
        myBtg -= 1;
        myAcc |= aBit << myBtg;

        if (myBtg == 0) {
            myBytes[myPosition++] = myAcc;
            myAcc = 0;
            myBtg = 8;
        }
    }

    private int log2(final int aX) {
        int x = aX;
        int v;

        for (v = -1; x != 0; x >>>= 1, v++) {
        }

        return v;
    }

    /**
     * Encode.
     *
     * @param aX An X to encode
     */
    public final void encode(final int aX) {
        Assert.that(aX != 0);

        int logofx = log2(aX);
        int nbits = logofx + 1;

        while (logofx-- != 0) {
            encodeBit(0);
        }

        while (--nbits >= 0) {
            encodeBit(aX >>> nbits & 1);
        }
    }

    /**
     * Stop encoding.
     *
     * @return A byte array
     */
    public final byte[] encodeStop() {
        if (myBtg != 8) {
            myBytes[myPosition++] = myAcc;
        }

        final byte[] packedArray = new byte[myPosition];

        System.arraycopy(myBytes, 0, packedArray, 0, myPosition);

        return packedArray;
    }

    /**
     * Decode start.
     */
    public final void decodeStart() {
        myBtg = 0;
        myAcc = 0;
        myPosition = 0;
    }

    private int decodeBit() {
        if (myBtg == 0) {
            myAcc = myBytes[myPosition++];
            myBtg = 8;
        }

        return myAcc >> --myBtg & 1;
    }

    /**
     * Decode.
     *
     * @return Decoded
     */
    public int decode() {
        int nbits = 0;
        int x = 1;

        while (decodeBit() == 0) {
            nbits += 1;
        }

        while (nbits-- > 0) {
            x += x + decodeBit();
        }

        return x;
    }

}
