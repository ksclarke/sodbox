
package info.freelibrary.sodbox;

/**
 * Class representing 64-bit decimal number. It provides fixed point arithmetic and can be also used in Lucene full
 * text search engine to convert number to strings with proper lexicographical order (so range operator in Lucene
 * query can be used for numeric fields). This class supports operations only for number having the same precision and
 * width.
 */
public class Decimal extends Number implements Comparable<Decimal>, IValue {

    private static final long serialVersionUID = -975937002307937042L;

    private static void checkArguments(final boolean cond) {
        if (!cond) {
            throw new IllegalArgumentException("Invalid decimal width or precision");
        }
    }

    private static long ipow(long value, long exponent) {
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

    private static String pad(final long value, int width, final char filler) {
        final StringBuffer buf = new StringBuffer();
        buf.append(value);
        width -= buf.length();
        while (--width >= 0) {
            buf.insert(0, filler);
        }
        return buf.toString();
    }

    private long value;

    private transient long maxValue;

    private transient long scale;

    private Decimal() {
    }

    /**
     * Constructor of decimal number from floating point value with given width and precision
     *
     * @param value floating point value
     * @param nIntegerDigits maximal number of digits in integer part of the number
     * @param nFractionDigits number of digits in fractional part of the number
     */
    public Decimal(final double value, final int nIntegerDigits, final int nFractionDigits) {
        this((long) (value * ipow(10, nFractionDigits)), nIntegerDigits, nFractionDigits);
    }

    /**
     * Constructor of decimal number from scaled value with given width and precision
     *
     * @param value scaled value of the number, i.e. 123456 for 1234.56
     * @param nIntegerDigits maximal number of digits in integer part of the number
     * @param nFractionDigits number of digits in fractional part of the number
     */
    public Decimal(final long value, final int nIntegerDigits, final int nFractionDigits) {
        checkArguments(nIntegerDigits >= 0 && nFractionDigits >= 0 && nIntegerDigits + nFractionDigits <= 16);
        maxValue = ipow(10, nIntegerDigits + nFractionDigits);
        scale = ipow(10, nFractionDigits);
        checkOverflow(value);
        this.value = value << 8 | nIntegerDigits << 4 | nFractionDigits;
    }

    /**
     * Constructor of decimal number from string representation. Maximal number of digits in integer and fractional
     * part of the number are calculated based on the given string value, i.e. for string "00123.45" it will be 5 and
     * 2.
     *
     * @param value string representation of the number
     */
    public Decimal(final String value) {
        final int dot = value.indexOf('.');
        int nIntegerDigits, nFractionDigits;
        String intPart;
        if (dot < 0) {
            intPart = value;
            nIntegerDigits = value.length();
        } else {
            intPart = value.substring(0, dot);
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
            nFractionDigits = value.length() - dot - 1;
            scale = ipow(10, nFractionDigits);
            this.value = sign * (Long.parseLong(intPart) * scale + Long.parseLong(value.substring(dot + 1))) << 8 |
                    nIntegerDigits << 4 | nFractionDigits;
        } else {
            nFractionDigits = 0;
            scale = 1;
            this.value = sign * Long.parseLong(intPart) << 8 | nIntegerDigits << 4 | nFractionDigits;
        }
        checkArguments(nIntegerDigits >= 0 && nFractionDigits >= 0 && nIntegerDigits + nFractionDigits <= 16);
        maxValue = ipow(10, nIntegerDigits + nFractionDigits);
    }

    /**
     * Constructor of decimal number from string representation with given width and precision
     *
     * @param value string representation of the number
     * @param nIntegerDigits maximal number of digits in integer part of the number
     * @param nFractionDigits number of digits in fractional part of the number
     */
    public Decimal(final String value, final int nIntegerDigits, final int nFractionDigits) {
        checkArguments(nIntegerDigits >= 0 && nFractionDigits >= 0 && nIntegerDigits + nFractionDigits <= 16);
        final int dot = value.indexOf('.');
        maxValue = ipow(10, nIntegerDigits + nFractionDigits);
        scale = ipow(10, nFractionDigits);
        String intPart;
        int intPartSize;
        if (dot < 0) {
            intPart = value;
            intPartSize = value.length();
        } else {
            intPart = value.substring(0, dot);
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
        checkArguments(intPartSize <= nIntegerDigits);
        if (dot >= 0) {
            checkArguments(value.length() - dot - 1 <= nFractionDigits);
            this.value = sign * (Long.parseLong(intPart) * scale + Long.parseLong(value.substring(dot + 1))) << 8 |
                    nIntegerDigits << 4 | nFractionDigits;
        } else {
            this.value = sign * Long.parseLong(intPart) * scale << 8 | nIntegerDigits << 4 | nFractionDigits;
        }
    }

    public Decimal abs() {
        final Decimal result = new Decimal();
        result.value = value < 0 ? -(value >> 8) << 8 | value & 0xFF : value;
        result.maxValue = maxValue;
        result.scale = scale;
        return result;
    }

    /**
     * Add two decimal numbers. The values should have the same with and precision.
     *
     * @param x number to be added
     * @return new decimal value with result of operation
     */
    public Decimal add(final Decimal x) {
        checkFormat(x);
        final long newValue = (value >> 8) + (x.value >> 8);
        checkOverflow(newValue);
        final Decimal result = new Decimal();
        result.value = newValue << 8 | value & 0xFF;
        result.maxValue = maxValue;
        result.scale = scale;
        return result;
    }

    public Decimal add(final double x) {
        return add(create(x));
    }

    public Decimal add(final long x) {
        return add(create(x));
    }

    public Decimal add(final String x) {
        return add(create(x));
    }

    private void calculateScale() {
        if (scale == 0) {
            maxValue = ipow(10, getIntegerDigits() + getFractionDigits());
            scale = ipow(10, getFractionDigits());
        }
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
        return (value < 0 ? value >> 8 : (value >> 8) + scale - 1) / scale;
    }

    private Decimal checkedCreate(final long value) {
        checkOverflow(value);
        final Decimal d = new Decimal();
        d.value = value << 8 | this.value & 0xFF;
        d.scale = scale;
        d.maxValue = maxValue;
        return d;
    }

    private void checkFormat(final Decimal x) {
        calculateScale();
        if ((byte) value != (byte) x.value) {
            throw new IllegalArgumentException("Decimal numbers have different format");
        }
    }

    private void checkOverflow(final long newValue) {
        if (newValue <= -maxValue || newValue >= maxValue) {
            throw new ArithmeticException("Overflow");
        }
    }

    @Override
    public int compareTo(final Decimal x) {
        checkFormat(x);
        return value < x.value ? -1 : value == x.value ? 0 : 1;
    }

    /**
     * Create decimal with the same formating as the target object and specified floating point value.
     *
     * @param value floating point value
     * @return created decimal number with the same format as target object
     */
    public Decimal create(final double value) {
        calculateScale();
        return checkedCreate((long) (value * scale));
    }

    /**
     * Create decimal with the same formating as the target object and specified integer value.
     *
     * @param value integer value
     * @return created decimal number with the same format as target object
     */
    public Decimal create(final long value) {
        calculateScale();
        final long scaledValue = value * scale;
        if (scaledValue / scale != value) {
            throw new ArithmeticException("Overflow");
        }
        return checkedCreate(scaledValue);
    }

    /**
     * Create decimal with the same formating as the target object and specified string value.
     *
     * @param value string value
     * @return created decimal number with the same format as target object
     */
    public Decimal create(final String value) {
        return new Decimal(value, getIntegerDigits(), getFractionDigits());
    }

    /**
     * Divide two decimal numbers. The values should have the same with and precision.
     *
     * @param x number to be divided
     * @return new decimal value with result of operation
     */
    public Decimal div(final Decimal x) {
        checkFormat(x);
        if (x.value >> 8 == 0) {
            throw new ArithmeticException("Divide by zero");
        }
        final Decimal result = new Decimal();
        result.value = (value >> 8) / (x.value >> 8) / scale;
        result.maxValue = maxValue;
        result.scale = scale;
        return result;
    }

    public Decimal div(final double x) {
        return div(create(x));
    }

    public Decimal div(final long x) {
        return div(create(x));
    }

    public Decimal div(final String x) {
        return div(create(x));
    }

    @Override
    public double doubleValue() {
        calculateScale();
        return (double) (value >> 8) / scale;
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof Decimal ? ((Decimal) o).value == value : o instanceof Number ? equals(create(((Number) o)
                .doubleValue())) : o instanceof String ? equals(create((String) o)) : false;
    }

    @Override
    public float floatValue() {
        return (float) doubleValue();
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
        return (value < 0 ? (value >> 8) - scale + 1 : value >> 8) / scale;
    }

    /**
     * Get number of digits in fractional part of the number, i.e. 2 in XXXXX.YY
     */
    public int getFractionDigits() {
        return (int) value & 0xF;
    }

    /**
     * Get number of digits in integer part of the number, i.e. 5 in XXXXX.YY
     */
    public int getIntegerDigits() {
        return (int) value >> 4 & 0xF;
    }

    @Override
    public int hashCode() {
        return (int) value ^ (int) (value >>> 32);
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public long longValue() {
        calculateScale();
        return (value >> 8) / scale;
    }

    /**
     * Multiply two decimal numbers. The values should have the same with and precision.
     *
     * @param x number to be multiplied
     * @return new decimal value with result of operation
     */
    public Decimal mul(final Decimal x) {
        checkFormat(x);
        long newValue = (value >> 8) * (x.value >> 8);
        if (newValue / (value >> 8) != x.value >> 8) {
            throw new ArithmeticException("Multiplication cause overflow");
        }
        newValue /= scale;
        checkOverflow(newValue);
        final Decimal result = new Decimal();
        result.value = newValue << 8 | value & 0xFF;
        result.maxValue = maxValue;
        result.scale = scale;
        return result;
    }

    public Decimal mul(final double x) {
        return mul(create(x));
    }

    public Decimal mul(final long x) {
        return mul(create(x));
    }

    public Decimal mul(final String x) {
        return mul(create(x));
    }

    public Decimal neg() {
        final Decimal result = new Decimal();
        result.value = -(value >> 8) << 8 | value & 0xFF;
        result.maxValue = maxValue;
        result.scale = scale;
        return result;
    }

    /**
     * Returns the closest integer to the decimal value.
     *
     * @return the closest integer to the decimal value
     */
    public long round() {
        calculateScale();
        return (value < 0 ? (value >> 8) - scale / 2 + 1 : (value >> 8) + scale / 2) / scale;
    }

    /**
     * Subtract two decimal numbers. The values should have the same with and precision.
     *
     * @param x number to be subtracted
     * @return new decimal value with result of operation
     */
    public Decimal sub(final Decimal x) {
        checkFormat(x);
        final long newValue = (value >> 8) - (x.value >> 8);
        checkOverflow(newValue);
        final Decimal result = new Decimal();
        result.value = newValue << 8 | value & 0xFF;
        result.maxValue = maxValue;
        result.scale = scale;
        return result;
    }

    public Decimal sub(final double x) {
        return sub(create(x));
    }

    public Decimal sub(final long x) {
        return sub(create(x));
    }

    public Decimal sub(final String x) {
        return sub(create(x));
    }

    /**
     * Get string representation of the given decimal number which been compared with another such string
     * representation of decimal number will produce the same comparison result as of original decimal numbers.
     *
     * @return string which can be used in comparison instead of decimal value
     */
    public String toLexicographicString() {
        calculateScale();
        return pad(((value >> 8) + maxValue) / scale, getIntegerDigits() + 1, '0') + "." + pad(((value >> 8) +
                maxValue) % scale, getFractionDigits(), '0');
    }

    /**
     * Get readable representation of decimal number.
     */
    @Override
    public String toString() {
        calculateScale();
        return Long.toString((value >> 8) / scale) + "." + pad((value < 0 ? -(value >> 8) : value >> 8) % scale,
                getFractionDigits(), '0');
    }

    /**
     * Get readable representation of decimal number using specified filler.
     *
     * @param filler character for padding integer part of the string
     */
    public String toString(final char filler) {
        calculateScale();
        return pad((value >> 8) / scale, getIntegerDigits() + 1, filler) + "." + pad((value < 0 ? -(value >> 8)
                : value >> 8) % scale, getFractionDigits(), '0');
    }

}
