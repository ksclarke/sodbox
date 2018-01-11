
package info.freelibrary.sodbox;

/**
 * R2 rectangle class. This class is used in spatial index.
 */
public class Rectangle implements IValue, Cloneable {

    /**
     * Non destructive join of two rectangles.
     *
     * @param a first joined rectangle
     * @param b second joined rectangle
     * @return rectangle containing cover of these two rectangles
     */
    public static Rectangle join(final Rectangle a, final Rectangle b) {
        final Rectangle r = new Rectangle(a);

        r.join(b);

        return r;
    }

    /**
     * Area of covered rectangle for two specified rectangles.
     */
    public static long joinArea(final Rectangle a, final Rectangle b) {
        final int left = a.left < b.left ? a.left : b.left;
        final int right = a.right > b.right ? a.right : b.right;
        final int top = a.top < b.top ? a.top : b.top;
        final int bottom = a.bottom > b.bottom ? a.bottom : b.bottom;

        return (long) (bottom - top) * (right - left);
    }

    private int top;

    private int left;

    private int bottom;

    private int right;

    /**
     * Default constructor for <code>Rectangle</code>.
     */
    public Rectangle() {
    }

    /**
     * Construct rectangle with specified coordinates.
     */
    public Rectangle(final int top, final int left, final int bottom, final int right) {
        Assert.that(top <= bottom && left <= right);

        this.top = top;
        this.left = left;
        this.bottom = bottom;
        this.right = right;
    }

    /**
     * Create copy of the rectangle.
     */
    public Rectangle(final Rectangle r) {
        top = r.top;
        left = r.left;
        bottom = r.bottom;
        right = r.right;
    }

    /**
     * Rectangle's area.
     */
    public final long area() {
        return (long) (bottom - top) * (right - left);
    }

    /**
     * Clone the rectangle.
     */
    @Override
    public Object clone() {
        try {
            final Rectangle r = (Rectangle) super.clone();

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
    public final boolean contains(final Rectangle r) {
        return left <= r.left && top <= r.top && right >= r.right && bottom >= r.bottom;
    }

    /**
     * Calculate distance from the specified point to the rectangle.
     */
    public double distance(final int x, final int y) {
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

        final int dx = x < left ? left - x : x - right;
        final int dy = y < top ? top - y : y - bottom;

        return Math.sqrt((double) dx * dx + (double) dy * dy);
    }

    /**
     * Check if two rectangles are equal.
     */
    @Override
    public boolean equals(final Object o) {
        if (o instanceof Rectangle) {
            final Rectangle r = (Rectangle) o;

            return left == r.left && top == r.top && right == r.right && bottom == r.bottom;
        }

        return false;
    }

    /**
     * Greatest Y coordinate of the rectangle.
     */
    public final int getBottom() {
        return bottom;
    }

    /**
     * Smallest X coordinate of the rectangle.
     */
    public final int getLeft() {
        return left;
    }

    /**
     * Greatest X coordinate of the rectangle.
     */
    public final int getRight() {
        return right;
    }

    /**
     * Smallest Y coordinate of the rectangle.
     */
    public final int getTop() {
        return top;
    }

    /**
     * Hash code consists of all rectangle coordinates.
     */
    @Override
    public int hashCode() {
        return top ^ bottom << 1 ^ left << 2 ^ right << 3;
    }

    /**
     * Checks if this rectangle intersects with specified rectangle.
     */
    public final boolean intersects(final Rectangle r) {
        return left <= r.right && top <= r.bottom && right >= r.left && bottom >= r.top;
    }

    /**
     * Join two rectangles. This rectangle is updates to contain cover of this and specified rectangle.
     *
     * @param r rectangle to be joined with this rectangle
     */
    public final void join(final Rectangle r) {
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
