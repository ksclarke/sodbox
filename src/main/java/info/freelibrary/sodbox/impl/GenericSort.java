
package info.freelibrary.sodbox.impl;

/**
 * A generic sort.
 */
public final class GenericSort {

    private GenericSort() {
        super();
    }

    static void sort(final GenericSortArray aArray) {
        sort1(aArray, 0, aArray.size());
    }

    private static void sort1(final GenericSortArray aArray, final int aOffset, final int aLength) {
        // Insertion sort on smallest arrays
        if (aLength < 7) {
            for (int i = aOffset; i < aLength + aOffset; i++) {
                for (int j = i; j > aOffset && aArray.compare(j - 1, j) > 0; j--) {
                    aArray.swap(j, j - 1);
                }
            }

            return;
        }

        // Choose a partition element, v
        int m = aOffset + (aLength >> 1); // Small arrays, middle element

        if (aLength > 7) {
            int l = aOffset;
            int n = aOffset + aLength - 1;

            if (aLength > 40) { // Big arrays, pseudo-median of 9
                final int s = aLength / 8;

                l = med3(aArray, l, l + s, l + 2 * s);
                m = med3(aArray, m - s, m, m + s);
                n = med3(aArray, n - 2 * s, n - s, n);
            }

            m = med3(aArray, l, m, n); // Mid-size, med of 3
        }

        // Establish Invariant: v* (<v)* (>v)* v*
        int a = aOffset;
        int b = a;
        int c = aOffset + aLength - 1;
        int d = c;
        int diff;

        while (true) {
            while (b <= c && (diff = aArray.compare(b, m)) <= 0) {
                if (diff == 0) {
                    aArray.swap(a++, b);
                }

                b++;
            }

            while (c >= b && (diff = aArray.compare(c, m)) >= 0) {
                if (diff == 0) {
                    aArray.swap(c, d--);
                }

                c--;
            }

            if (b > c) {
                break;
            }

            aArray.swap(b++, c--);
        }

        // Swap partition elements back to middle
        final int n = aOffset + aLength;

        int s;

        s = Math.min(a - aOffset, b - a);
        vecswap(aArray, aOffset, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswap(aArray, b, n - s, s);

        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            sort1(aArray, aOffset, s);
        }

        if ((s = d - c) > 1) {
            sort1(aArray, n - s, s);
        }
    }

    private static void vecswap(final GenericSortArray aArray, final int aA, final int aB, final int aCount) {
        int a = aA;
        int b = aB;

        for (int i = 0; i < aCount; i++, a++, b++) {
            aArray.swap(a, b);
        }
    }

    private static int med3(final GenericSortArray aArray, final int aA, final int aB, final int aC) {
        return aArray.compare(aA, aB) < 0 ? aArray.compare(aB, aC) < 0 ? aB : aArray.compare(aA, aC) < 0 ? aC : aA
                : aArray.compare(aB, aC) > 0 ? aB : aArray.compare(aA, aC) > 0 ? aC : aA;
    }

}
