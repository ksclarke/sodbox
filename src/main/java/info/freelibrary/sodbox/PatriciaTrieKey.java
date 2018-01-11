
package info.freelibrary.sodbox;

import java.net.InetAddress;

/**
 * Convert different type of keys to 64-bit long value used in PATRICIA trie (Practical Algorithm To Retrieve
 * Information Coded In Alphanumeric).
 */
public class PatriciaTrieKey {

    /**
     * Bit mask representing bit vector. The last digit of the key is the right most bit of the mask.
     */
    public final long myMask;

    /**
     * Length of bit vector (can not be larger than 64).
     */
    public final int myLength;

    /**
     * Creates a PATRICIA trie from supplied mask and length.
     *
     * @param aMask A mask
     * @param aLength A length
     */
    public PatriciaTrieKey(final long aMask, final int aLength) {
        myMask = aMask;
        myLength = aLength;
    }

    /**
     * Creates a PATRICIA trie key from the supplied 7 bit string.
     *
     * @param aString A 7 bit string
     * @return The key
     */
    public static PatriciaTrieKey from7bitString(final String aString) {
        long mask = 0;
        final int n = aString.length();

        Assert.that((n * 7) <= 64);

        for (int i = 0; i < n; i++) {
            final char ch = aString.charAt(i);

            mask = (mask << 7) | (ch & 0x7F);
        }

        return new PatriciaTrieKey(mask, n * 7);
    }

    /**
     * Creates a PATRICIA trie key from the supplied 8 bit string.
     *
     * @param aString A 8 bit string
     * @return The key
     */
    public static PatriciaTrieKey from8bitString(final String aString) {
        long mask = 0;
        final int n = aString.length();

        Assert.that(n <= 8);

        for (int i = 0; i < n; i++) {
            final char ch = aString.charAt(i);

            mask = (mask << 8) | (ch & 0xFF);
        }

        return new PatriciaTrieKey(mask, n * 8);
    }

    /**
     * Creates a PATRICIA trie key from the supplied byte array.
     *
     * @param aByteArray A byte array
     * @return The key
     */
    public static PatriciaTrieKey fromByteArray(final byte[] aByteArray) {
        long mask = 0;
        final int n = aByteArray.length;

        Assert.that(n <= 8);

        for (int i = 0; i < n; i++) {
            mask = (mask << 8) | (aByteArray[i] & 0xFF);
        }

        return new PatriciaTrieKey(mask, n * 8);
    }

    /**
     * Creates a PATRICIA trie key from the supplied decimal digits.
     *
     * @param aDigits A decimal digits
     * @return The key
     */
    public static PatriciaTrieKey fromDecimalDigits(final String aDigits) {
        long mask = 0;
        final int n = aDigits.length();

        Assert.that(n <= 16);

        for (int i = 0; i < n; i++) {
            final char ch = aDigits.charAt(i);

            Assert.that((ch >= '0') && (ch <= '9'));

            mask = (mask << 4) | (ch - '0');
        }

        return new PatriciaTrieKey(mask, n * 4);
    }

    /**
     * Creates a PATRICIA trie key from the supplied IP address.
     *
     * @param aInetAddress An IP address
     * @return The key
     */
    public static PatriciaTrieKey fromIpAddress(final InetAddress aInetAddress) {
        final byte[] bytes = aInetAddress.getAddress();
        long mask = 0;

        for (final byte b : bytes) {
            mask = (mask << 8) | (b & 0xFF);
        }

        return new PatriciaTrieKey(mask, bytes.length * 8);
    }

    /**
     * Creates a PATRICIA trie key from the supplied IP address string.
     *
     * @param aInetAddress An IP address string
     * @return The key
     */
    public static PatriciaTrieKey fromIpAddress(final String aInetAddress) throws NumberFormatException {
        long mask = 0;
        int pos = 0;
        int len = 0;

        do {
            final int dot = aInetAddress.indexOf('.', pos);
            final String part = dot < 0 ? aInetAddress.substring(pos) : aInetAddress.substring(pos, dot);
            pos = dot + 1;
            final int b = Integer.parseInt(part, 10);
            mask = (mask << 8) | (b & 0xFF);
            len += 8;
        } while (pos > 0);

        return new PatriciaTrieKey(mask, len);
    }

}
