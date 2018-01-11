
package info.freelibrary.sodbox.impl;

import info.freelibrary.sodbox.Assert;

public class Compressor {

    private final byte[] buf;

    private byte acc;

    private int pos;

    private int btg;

    public Compressor(final byte[] buf) {
        this.buf = buf;
    }

    public int decode() {
        int x = 1;
        int nbits = 0;
        while (decodeBit() == 0) {
            nbits += 1;
        }
        while (nbits-- > 0) {
            x += x + decodeBit();
        }
        return x;
    }

    private final int decodeBit() {
        if (btg == 0) {
            acc = buf[pos++];
            btg = 8;
        }
        return acc >> --btg & 1;
    }

    public final void decodeStart() {
        btg = 0;
        acc = 0;
        pos = 0;
    }

    public final void encode(final int x) {
        Assert.that(x != 0);
        int logofx = log2(x);
        int nbits = logofx + 1;
        while (logofx-- != 0) {
            encodeBit(0);
        }
        while (--nbits >= 0) {
            encodeBit(x >>> nbits & 1);
        }
    }

    private final void encodeBit(final int b) {
        btg -= 1;
        acc |= b << btg;
        if (btg == 0) {
            buf[pos++] = acc;
            acc = 0;
            btg = 8;
        }
    }

    public final void encodeStart() {
        btg = 8;
        acc = 0;
        pos = 0;
    }

    public final byte[] encodeStop() {
        if (btg != 8) {
            buf[pos++] = acc;
        }
        final byte[] packedArray = new byte[pos];
        System.arraycopy(buf, 0, packedArray, 0, pos);
        return packedArray;
    }

    private int log2(int x) {
        int v;
        for (v = -1; x != 0; x >>>= 1, v++) {
            ;
        }
        return v;
    }
}