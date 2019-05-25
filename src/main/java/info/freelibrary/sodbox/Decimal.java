
package info.freelibrary.sodbox;

/**
 * Class representing 64-bit decimal number. It provides fixed point arithmetic and can be also used in Lucene full
 * text search engine to convert number to strings with proper lexicographical order (so range operator in Lucene
 * query can be used for numeric fields). This class supports operations only for number having the same precision and
 * width.
 */
public class Decimal extends Number implements Comparable<Decimal>, IValue {

    private static final long serialVersionUID = -975937002307937042L;

    private static final String DOT = ".";

    private long myValue;

    private transient long myMaxValue;

    private transient long myScale;

    /**
     * Constructor of decimal number from scaled value with given width and precision
     *
     * @param aValue scaled value of the number, i.e. 123456 for 1234.56
     * @param aNIntegerDigits maximal number of digits in integer part of the number
     * @param aNFractionDigits number of digits in fractional part of the number
     */
    public Decimal(final long aValue, final int aNIntegerDigits, final int aNFractionDigits) {
        checkArguments(aNIntegerDigits >= 0 && aNFractionDigits >= 0 && aNIntegerDigits + aNFractionDigits <= 16);
        myMaxValue = ipow(10, aNIntegerDigits + aNFractionDigits);
        myScale = ipow(10, aNFractionDigits);
        checkOverflow(aValue);
        this.myValue = aValue << 8 | aNIntegerDigits << 4 | aNFractionDigits;
    }

    /**
     * Constructor of decimal number from floating point value with given width and precision
     *
     * @param aValue floating point value
     * @param aNIntegerDigits maximal number of digits in integer part of the number
     * @param aNFractionDigits number of digits in fractional part of the number
     */
    public Decimal(final double aValue, final int aNIntegerDigits, final int aNFractionDigits) {
        this((long) (aValue * ipow(10, aNFractionDigits)), aNIntegerDigits, aNFractionDigits);
    }

    /**
     * Constructor of decimal number from string representation with given width and precision
     *
     * @param aValue string representation of the number
     * @param aNIntegerDigits maximal number of digits in integer part of the number
     * @param aNFractionDigits number of digits in fractional part of the number
     */
    public Decimal(final String aValue, final int aNIntegerDigits, final int aNFractionDigits) {
        checkArguments(aNIntegerDigits >= 0 && aNFractionDigits >= 0 && aNIntegerDigits + aNFractionDigits <= 16);
        final int dot = aValue.indexOf('.');
        myMaxValue = ipow(10, aNIntegerDigits + aNFractionDigits);
        myScale = ipow(10, aNFractionDigits);
        String intPart;
        int intPartSize;

        if (dot < 0) {
            intPart = aValue;
            intPartSize = aValue.length();
        } else {
            intPart = aValue.substring(0, dot);
            intPartSize = dot;
        }

        intPart = intPart.trim();

        int sign = 1;

        if (intPart.charAt(0) == '-') {
            intPart = intPart.substring(1).trim();
            sign = -1;
            intPartSize -= 1;
        } else if (intPart.charAt(0) == '+') {
            intPart = intPart.substring(1).trim();
            intPartSize -= 1;
        }

        checkArguments(intPartSize <= aNIntegerDigits);

        if (dot >= 0) {
            checkArguments(aValue.length() - dot - 1 <= aNFractionDigits);
            myValue = sign * (Long.parseLong(intPart) * myScale + Long.parseLong(aValue.substring(dot + 1))) << 8 |
                    aNIntegerDigits << 4 | aNFractionDigits;
        } else {
            this.myValue = sign * Long.parseLong(intPart) * myScale << 8 | aNIntegerDigits << 4 | aNFractionDigits;
        }
    }

    /**
     * Constructor of decimal number from string representation. Maximal number of digits in integer and fractional
     * part of the number are calculated based on the given string value, i.e. for string "00123.45" it will be 5 and
     * 2.
     *
     * @param aValue string representation of the number
     */
    public Decimal(final String aValue) {
        final int dot = aValue.indexOf('.');
        final int nFractionDigits;

        int nIntegerDigits;
        String intPart;

        if (dot < 0) {
            intPart = aValue;
            nIntegerDigits = aValue.length();
        } else {
            intPart = aValue.substring(0, dot);
            nIntegerDigits = dot;
        }

        intPart = intPart.trim();

        int sign = 1;

        if (intPart.charAt(0) == '-') {
            intPart = intPart.substring(1).trim();
            sign = -1;
            nIntegerDigits -= 1;
        } else if (intPart.charAt(0) == '+') {
            intPart = intPart.substring(1).trim();
            nIntegerDigits -= 1;
        }

        if (dot >= 0) {
            nFractionDigits = aValue.length() - dot - 1;
            myScale = ipow(10, nFractionDigits);
            myValue = sign * (Long.parseLong(intPart) * myScale + Long.parseLong(aValue.substring(dot + 1))) << 8 |
                    nIntegerDigits << 4 | nFractionDigits;
        } else {
            nFractionDigits = 0;
            myScale = 1;
            this.myValue = sign * Long.parseLong(intPart) << 8 | nIntegerDigits << 4 | nFractionDigits;
        }

        checkArguments(nIntegerDigits >= 0 && nFractionDigits >= 0 && nIntegerDigits + nFractionDigits <= 16);
        myMaxValue = ipow(10, nIntegerDigits + nFractionDigits);
    }

    private Decimal() {
    }

    /**
     * Get number of digits in integer part of the number, i.e. 5 in XXXXX.YY
     */
    public int getIntegerDigits() {
        return (int) myValue >> 4 & 0xF;
    }

    /**
     * Get number of digits in fractional part of the number, i.e. 2 in XXXXX.YY
     */
    public int getFractionDigits() {
        return (int) myValue & 0xF;
    }

    private void calculateScale() {
        if (myScale == 0) {
            myMaxValue = ipow(10, getIntegerDigits() + getFractionDigits());
            myScale = ipow(10, getFractionDigits());
        }
    }

    private static String pad(final long aValue, final int aWidth, final char aFiller) {
        final StringBuffer buf = new StringBuffer();

        int width = aWidth;

        buf.append(aValue);
        width -= buf.length();

        while (--width >= 0) {
            buf.insert(0, aFiller);
        }

        return buf.toString();
    }

    private static long ipow(final long aValue, final long aExponent) {
        long exponent = aExponent;
        long value = aValue;
        long result = 1;

        while (exponent != 0) {
            if ((exponent & 1) != 0) {
                result *= value;
            }

            value *= value;
            exponent >>>= 1;
        }

        return result;
    }

    private void checkOverflow(final long aNewValue) {
        if (aNewValue <= -myMaxValue || aNewValue >= myMaxValue) {
            throw new ArithmeticException("Overflow Exception");
        }
    }

    private void checkFormat(final Decimal aX) {
        calculateScale();

        if ((byte) myValue != (byte) aX.myValue) {
            throw new IllegalArgumentException("Decimal numbers have different format");
        }
    }

    private static void checkArguments(final boolean aCondition) {
        if (!aCondition) {
            throw new IllegalArgumentException("Invalid decimal width or precision");
        }
    }

    private Decimal checkedCreate(final long aValue) {
        checkOverflow(aValue);

        final Decimal d = new Decimal();

        d.myValue = aValue << 8 | this.myValue & 0xFF;
        d.myScale = myScale;
        d.myMaxValue = myMaxValue;

        return d;
    }

    /**
     * Create decimal with the same formating as the target object and specified integer value.
     *
     * @param aValue integer value
     * @return created decimal number with the same format as target object
     */
    public Decimal create(final long aValue) {
        calculateScale();

        final long scaledValue = aValue * myScale;

        if (scaledValue / myScale != aValue) {
            throw new ArithmeticException("Overflow");
        }

        return checkedCreate(scaledValue);
    }

    /**
     * Create decimal with the same formating as the target object and specified floating point value.
     *
     * @param aValue floating point value
     * @return created decimal number with the same format as target object
     */
    public Decimal create(final double aValue) {
        calculateScale();
        return checkedCreate((long) (aValue * myScale));
    }

    /**
     * Create decimal with the same formating as the target object and specified string value.
     *
     * @param aValue string value
     * @return created decimal number with the same format as target object
     */
    public Decimal create(final String aValue) {
        return new Decimal(aValue, getIntegerDigits(), getFractionDigits());
    }

    /**
     * Add two decimal numbers. The values should have the same with and precision.
     *
     * @param aX number to be added
     * @return new decimal value with result of operation
     */
    public Decimal add(final Decimal aX) {
        checkFormat(aX);

        final long newValue = (myValue >> 8) + (aX.myValue >> 8);

        checkOverflow(newValue);

        final Decimal result = new Decimal();

        result.myValue = newValue << 8 | myValue & 0xFF;
        result.myMaxValue = myMaxValue;
        result.myScale = myScale;

        return result;
    }

    /**
     * Subtract two decimal numbers. The values should have the same with and precision.
     *
     * @param aX number to be subtracted
     * @return new decimal value with result of operation
     */
    public Decimal sub(final Decimal aX) {
        checkFormat(aX);

        final long newValue = (myValue >> 8) - (aX.myValue >> 8);

        checkOverflow(newValue);

        final Decimal result = new Decimal();

        result.myValue = newValue << 8 | myValue & 0xFF;
        result.myMaxValue = myMaxValue;
        result.myScale = myScale;

        return result;
    }

    /**
     * Multiply two decimal numbers. The values should have the same with and precision.
     *
     * @param aX number to be multiplied
     * @return new decimal value with result of operation
     */
    public Decimal mul(final Decimal aX) {
        checkFormat(aX);

        long newValue = (myValue >> 8) * (aX.myValue >> 8);

        if (newValue / (myValue >> 8) != aX.myValue >> 8) {
            throw new ArithmeticException("Multiplication cause overflow");
        }

        newValue /= myScale;
        checkOverflow(newValue);

        final Decimal result = new Decimal();

        result.myValue = newValue << 8 | myValue & 0xFF;
        result.myMaxValue = myMaxValue;
        result.myScale = myScale;

        return result;
    }

    /**
     * Divide two decimal numbers. The values should have the same with and precision.
     *
     * @param aX number to be divided
     * @return new decimal value with result of operation
     */
    public Decimal div(final Decimal aX) {
        checkFormat(aX);

        if (aX.myValue >> 8 == 0) {
            throw new ArithmeticException("Divide by zero");
        }

        final Decimal result = new Decimal();

        result.myValue = (myValue >> 8) / (aX.myValue >> 8) / myScale;
        result.myMaxValue = myMaxValue;
        result.myScale = myScale;

        return result;
    }

    /**
     * Add a long to the decimal.
     *
     * @param aX A long
     * @return A decimal
     */
    public Decimal add(final long aX) {
        return add(create(aX));
    }

    /**
     * Subtract a long from the decimal.
     *
     * @param aX A long
     * @return A decimal
     */
    public Decimal sub(final long aX) {
        return sub(create(aX));
    }

    /**
     * Multiple a long with the decimal.
     *
     * @param aX A long
     * @return A decimal
     */
    public Decimal mul(final long aX) {
        return mul(create(aX));
    }

    /**
     * Divide the decimal by a long.
     *
     * @param aX A long
     * @return A decimal
     */
    public Decimal div(final long aX) {
        return div(create(aX));
    }

    /**
     * Add a double to the decimal.
     *
     * @param aX A double
     * @return A decimal
     */
    public Decimal add(final double aX) {
        return add(create(aX));
    }

    /**
     * Subtract a double from the decimal.
     *
     * @param aX A double
     * @return A decimal
     */
    public Decimal sub(final double aX) {
        return sub(create(aX));
    }

    /**
     * Multiply a double by the decimal.
     *
     * @param aX A double
     * @return A long
     */
    public Decimal mul(final double aX) {
        return mul(create(aX));
    }

    /**
     * Divide the decimal by the double.
     *
     * @param aX A double
     * @return A decimal
     */
    public Decimal div(final double aX) {
        return div(create(aX));
    }

    /**
     * Add the string value to the decimal.
     *
     * @param aX A String value
     * @return A decimal
     */
    public Decimal add(final String aX) {
        return add(create(aX));
    }

    /**
     * Subtract the string value from the decimal.
     *
     * @param aX A String value
     * @return A decimal
     */
    public Decimal sub(final String aX) {
        return sub(create(aX));
    }

    /**
     * Multiply the string value by the decimal.
     *
     * @param aX A String value
     * @return A decimal
     */
    public Decimal mul(final String aX) {
        return mul(create(aX));
    }

    /**
     * Divide the decimal by the string value.
     *
     * @param aX A String value
     * @return A decimal
     */
    public Decimal div(final String aX) {
        return div(create(aX));
    }

    /**
     * Returns the closest integer to the decimal value.
     *
     * @return the closest integer to the decimal value
     */
    public long round() {
        calculateScale();
        return (myValue < 0 ? (myValue >> 8) - myScale / 2 + 1 : (myValue >> 8) + myScale / 2) / myScale;
    }

    /**
     * Returns the largest (closest to positive infinity) integer value that is less than or equal to this decimal
     * value.
     *
     * @return the largest (closest to positive infinity) integer value that is less than or equal to this decimal
     *         value
     */
    public long floor() {
        calculateScale();
        return (myValue < 0 ? (myValue >> 8) - myScale + 1 : myValue >> 8) / myScale;
    }

    /**
     * Returns the smallest (closest to negative infinity) integer value that is greater than or equal to this decimal
     * value.
     *
     * @return the smallest (closest to negative infinity) integer value that is greater than or equal to this decimal
     *         value
     */
    public long ceil() {
        calculateScale();
        return (myValue < 0 ? myValue >> 8 : (myValue >> 8) + myScale - 1) / myScale;
    }

    @Override
    public boolean equals(final Object aObject) {
        return aObject instanceof Decimal ? ((Decimal) aObject).myValue == myValue : aObject instanceof Number
                ? equals(create(((Number) aObject).doubleValue())) : aObject instanceof String ? equals(create(
                        (String) aObject)) : false;
    }

    @Override
    public int compareTo(final Decimal aX) {
        checkFormat(aX);
        return myValue < aX.myValue ? -1 : myValue == aX.myValue ? 0 : 1;
    }

    @Override
    public int hashCode() {
        return (int) myValue ^ (int) (myValue >>> 32);
    }

    /**
     * Return the absolute value of the decimal.
     *
     * @return decimal
     */
    public Decimal abs() {
        final Decimal result = new Decimal();

        result.myValue = myValue < 0 ? -(myValue >> 8) << 8 | myValue & 0xFF : myValue;
        result.myMaxValue = myMaxValue;
        result.myScale = myScale;

        return result;
    }

    /**
     * Get negative value.
     *
     * @return A decimal
     */
    public Decimal neg() {
        final Decimal result = new Decimal();

        result.myValue = -(myValue >> 8) << 8 | myValue & 0xFF;
        result.myMaxValue = myMaxValue;
        result.myScale = myScale;

        return result;
    }

    @Override
    public long longValue() {
        calculateScale();
        return (myValue >> 8) / myScale;
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public double doubleValue() {
        calculateScale();
        return (double) (myValue >> 8) / myScale;
    }

    @Override
    public float floatValue() {
        return (float) doubleValue();
    }

    /**
     * Get readable representation of decimal number.
     */
    @Override
    public String toString() {
        calculateScale();

        return Long.toString((myValue >> 8) / myScale) + DOT + pad((myValue < 0 ? -(myValue >> 8) : myValue >> 8) %
                myScale, getFractionDigits(), '0');
    }

    /**
     * Get readable representation of decimal number using specified filler.
     *
     * @param aFiller character for padding integer part of the string
     */
    public String toString(final char aFiller) {
        calculateScale();

        return pad((myValue >> 8) / myScale, getIntegerDigits() + 1, aFiller) + DOT + pad((myValue < 0
                ? -(myValue >> 8) : myValue >> 8) % myScale, getFractionDigits(), '0');
    }

    /**
     * Get string representation of the given decimal number which been compared with another such string
     * representation of decimal number will produce the same comparison result as of original decimal numbers.
     *
     * @return string which can be used in comparison instead of decimal value
     */
    public String toLexicographicString() {
        calculateScale();

        return pad(((myValue >> 8) + myMaxValue) / myScale, getIntegerDigits() + 1, '0') + DOT + pad(((myValue >> 8) +
                myMaxValue) % myScale, getFractionDigits(), '0');
    }

}
