
package info.freelibrary.sodbox;

/**
 * R2 rectangle class. This class is used in spatial index.
 */
public class Rectangle implements IValue, Cloneable {

    private int myTop;

    private int myLeft;

    private int myBottom;

    private int myRight;

    /**
     * Default constructor for <code>Rectangle</code>.
     */
    public Rectangle() {
    }

    /**
     * Create copy of the rectangle.
     *
     * @param aRectangle A rectangle
     */
    public Rectangle(final Rectangle aRectangle) {
        this.myTop = aRectangle.myTop;
        this.myLeft = aRectangle.myLeft;
        this.myBottom = aRectangle.myBottom;
        this.myRight = aRectangle.myRight;
    }

    /**
     * Construct rectangle with specified coordinates.
     */
    public Rectangle(final int aTop, final int aLeft, final int aBottom, final int aRight) {
        Assert.that(aTop <= aBottom && aLeft <= aRight);

        this.myTop = aTop;
        this.myLeft = aLeft;
        this.myBottom = aBottom;
        this.myRight = aRight;
    }

    /**
     * Smallest Y coordinate of the rectangle.
     */
    public final int getTop() {
        return myTop;
    }

    /**
     * Smallest X coordinate of the rectangle.
     */
    public final int getLeft() {
        return myLeft;
    }

    /**
     * Greatest Y coordinate of the rectangle.
     */
    public final int getBottom() {
        return myBottom;
    }

    /**
     * Greatest X coordinate of the rectangle.
     */
    public final int getRight() {
        return myRight;
    }

    /**
     * Rectangle's area.
     */
    public final long area() {
        return (long) (myBottom - myTop) * (myRight - myLeft);
    }

    /**
     * Area of covered rectangle for two specified rectangles.
     */
    public static long joinArea(final Rectangle a1stRectangle, final Rectangle a2ndRectangle) {
        final int left = a1stRectangle.myLeft < a2ndRectangle.myLeft ? a1stRectangle.myLeft : a2ndRectangle.myLeft;
        final int right = a1stRectangle.myRight > a2ndRectangle.myRight ? a1stRectangle.myRight
                : a2ndRectangle.myRight;
        final int top = a1stRectangle.myTop < a2ndRectangle.myTop ? a1stRectangle.myTop : a2ndRectangle.myTop;
        final int bottom = a1stRectangle.myBottom > a2ndRectangle.myBottom ? a1stRectangle.myBottom
                : a2ndRectangle.myBottom;

        return (long) (bottom - top) * (right - left);
    }

    /**
     * Calculate distance from the specified point to the rectangle.
     */
    public double distance(final int aX, final int aY) {
        if (aX >= myLeft && aX <= myRight) {
            if (aY >= myTop) {
                if (aY <= myBottom) {
                    return 0;
                } else {
                    return aY - myBottom;
                }
            } else {
                return myTop - aY;
            }
        } else if (aY >= myTop && aY <= myBottom) {
            if (aX < myLeft) {
                return myLeft - aX;
            } else {
                return aX - myRight;
            }
        }

        final int dx = aX < myLeft ? myLeft - aX : aX - myRight;
        final int dy = aY < myTop ? myTop - aY : aY - myBottom;

        return Math.sqrt((double) dx * dx + (double) dy * dy);
    }

    /**
     * Clone the rectangle.
     */
    @Override
    public Object clone() {
        try {
            final Rectangle r = (Rectangle) super.clone();

            r.myTop = this.myTop;
            r.myLeft = this.myLeft;
            r.myBottom = this.myBottom;
            r.myRight = this.myRight;

            return r;
        } catch (final CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }

    /**
     * Join two rectangles. This rectangle is updates to contain cover of this and specified rectangle.
     *
     * @param aRectangle rectangle to be joined with this rectangle
     */
    public final void join(final Rectangle aRectangle) {
        if (myLeft > aRectangle.myLeft) {
            myLeft = aRectangle.myLeft;
        }

        if (myRight < aRectangle.myRight) {
            myRight = aRectangle.myRight;
        }

        if (myTop > aRectangle.myTop) {
            myTop = aRectangle.myTop;
        }

        if (myBottom < aRectangle.myBottom) {
            myBottom = aRectangle.myBottom;
        }
    }

    /**
     * Non destructive join of two rectangles.
     *
     * @param a1stRectangle first joined rectangle
     * @param a2ndRectangle second joined rectangle
     * @return rectangle containing cover of these two rectangles
     */
    public static Rectangle join(final Rectangle a1stRectangle, final Rectangle a2ndRectangle) {
        final Rectangle rectangle = new Rectangle(a1stRectangle);

        rectangle.join(a2ndRectangle);

        return rectangle;
    }

    /**
     * Checks if this rectangle intersects with specified rectangle.
     */
    public final boolean intersects(final Rectangle aRectangle) {
        return myLeft <= aRectangle.myRight && myTop <= aRectangle.myBottom && myRight >= aRectangle.myLeft &&
                myBottom >= aRectangle.myTop;
    }

    /**
     * Checks if this rectangle contains the specified rectangle.
     */
    public final boolean contains(final Rectangle aRectangle) {
        return myLeft <= aRectangle.myLeft && myTop <= aRectangle.myTop && myRight >= aRectangle.myRight &&
                myBottom >= aRectangle.myBottom;
    }

    /**
     * Check if two rectangles are equal.
     */
    @Override
    public boolean equals(final Object aObject) {
        if (aObject instanceof Rectangle) {
            final Rectangle r = (Rectangle) aObject;

            return myLeft == r.myLeft && myTop == r.myTop && myRight == r.myRight && myBottom == r.myBottom;
        }

        return false;
    }

    /**
     * Hash code consists of all rectangle coordinates.
     */
    @Override
    public int hashCode() {
        return myTop ^ myBottom << 1 ^ myLeft << 2 ^ myRight << 3;
    }

    @Override
    public String toString() {
        return "top=" + myTop + ", left=" + myLeft + ", bottom=" + myBottom + ", right=" + myRight;
    }

}
