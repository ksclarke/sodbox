
package info.freelibrary.sodbox;

/**
 * R2 rectangle class. This class is used in spatial index.
 */
public class RectangleR2 implements IValue, Cloneable {

    /**
     * Non destructive join of two rectangles.
     *
     * @param a first joined rectangle
     * @param b second joined rectangle
     * @return rectangle containing cover of these two rectangles
     */
    public static RectangleR2 join(final RectangleR2 a, final RectangleR2 b) {
        final RectangleR2 r = new RectangleR2(a);

        r.join(b);

        return r;
    }

    /**
     * Area of covered rectangle for two specified rectangles.
     */
    public static double joinArea(final RectangleR2 a, final RectangleR2 b) {
        final double left = a.left < b.left ? a.left : b.left;
        final double right = a.right > b.right ? a.right : b.right;
        final double top = a.top < b.top ? a.top : b.top;
        final double bottom = a.bottom > b.bottom ? a.bottom : b.bottom;

        return (bottom - top) * (right - left);
    }

    private double top;

    private double left;

    private double bottom;

    private double right;

    /**
     * Default constructor.
     */
    public RectangleR2() {
    }

    /**
     * Construct rectangle with specified coordinates.
     */
    public RectangleR2(final double top, final double left, final double bottom, final double right) {
        Assert.that(top <= bottom && left <= right);

        this.top = top;
        this.left = left;
        this.bottom = bottom;
        this.right = right;
    }

    /**
     * Create copy of the rectangle.
     */
    public RectangleR2(final RectangleR2 r) {
        top = r.top;
        left = r.left;
        bottom = r.bottom;
        right = r.right;
    }

    /**
     * Rectangle area.
     */
    public final double area() {
        return (bottom - top) * (right - left);
    }

    /**
     * Clone rectangle.
     */
    @Override
    public Object clone() {
        try {
            final RectangleR2 r = (RectangleR2) super.clone();

            r.top = top;
            r.left = left;
            r.bottom = bottom;
            r.right = right;

            return r;
        } catch (final CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }

    /**
     * Checks if this rectangle contains the specified rectangle.
     */
    public final boolean contains(final RectangleR2 r) {
        return left <= r.left && top <= r.top && right >= r.right && bottom >= r.bottom;
    }

    /**
     * Calculate distance from the specified point to the rectangle.
     */
    public double distance(final double x, final double y) {
        if (x >= left && x <= right) {
            if (y >= top) {
                if (y <= bottom) {
                    return 0;
                } else {
                    return y - bottom;
                }
            } else {
                return top - y;
            }
        } else if (y >= top && y <= bottom) {
            if (x < left) {
                return left - x;
            } else {
                return x - right;
            }
        }

        final double dx = x < left ? left - x : x - right;
        final double dy = y < top ? top - y : y - bottom;

        return Math.sqrt(dx * dx + dy * dy);
    }

    /**
     * Check if two rectangles are equal.
     */
    @Override
    public boolean equals(final Object o) {
        if (o instanceof RectangleR2) {
            final RectangleR2 r = (RectangleR2) o;

            return left == r.left && top == r.top && right == r.right && bottom == r.bottom;
        }

        return false;
    }

    /**
     * Greatest Y coordinate of the rectangle.
     */
    public final double getBottom() {
        return bottom;
    }

    /**
     * Smallest X coordinate of the rectangle.
     */
    public final double getLeft() {
        return left;
    }

    /**
     * Greatest X coordinate of the rectangle.
     */
    public final double getRight() {
        return right;
    }

    /**
     * Smallest Y coordinate of the rectangle.
     */
    public final double getTop() {
        return top;
    }

    /**
     * Hash code consists of all rectangle coordinates.
     */
    @Override
    public int hashCode() {
        return (int) (Double.doubleToLongBits(top) ^ Double.doubleToLongBits(bottom) << 1 ^ Double.doubleToLongBits(
                left) << 2 ^ Double.doubleToLongBits(right) << 3);
    }

    /**
     * Checks if this rectangle intersects with specified rectangle.
     */
    public final boolean intersects(final RectangleR2 r) {
        return left <= r.right && top <= r.bottom && right >= r.left && bottom >= r.top;
    }

    /**
     * Join two rectangles. This rectangle is updates to contain cover of this and specified rectangle.
     *
     * @param r rectangle to be joined with this rectangle
     */
    public final void join(final RectangleR2 r) {
        if (left > r.left) {
            left = r.left;
        }

        if (right < r.right) {
            right = r.right;
        }

        if (top > r.top) {
            top = r.top;
        }

        if (bottom < r.bottom) {
            bottom = r.bottom;
        }
    }

    @Override
    public String toString() {
        return "top=" + top + ", left=" + left + ", bottom=" + bottom + ", right=" + right;
    }

}
