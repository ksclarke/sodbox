
package info.freelibrary.sodbox;

/**
 * R2 rectangle class. This class is used in spatial index.
 */
public class RectangleR2 implements IValue, Cloneable {

    private double myTop;

    private double myLeft;

    private double myBottom;

    private double myRight;

    /**
     * Default constructor.
     */
    public RectangleR2() {
    }

    /**
     * Construct rectangle with specified coordinates.
     */
    public RectangleR2(final double aTop, final double aLeft, final double aBottom, final double aRight) {
        Assert.that(aTop <= aBottom && aLeft <= aRight);

        myTop = aTop;
        myLeft = aLeft;
        myBottom = aBottom;
        myRight = aRight;
    }

    /**
     * Create copy of the rectangle.
     */
    public RectangleR2(final RectangleR2 aRectangle) {
        myTop = aRectangle.myTop;
        myLeft = aRectangle.myLeft;
        myBottom = aRectangle.myBottom;
        myRight = aRectangle.myRight;
    }

    /**
     * Smallest Y coordinate of the rectangle.
     */
    public final double getTop() {
        return myTop;
    }

    /**
     * Smallest X coordinate of the rectangle.
     */
    public final double getLeft() {
        return myLeft;
    }

    /**
     * Greatest Y coordinate of the rectangle.
     */
    public final double getBottom() {
        return myBottom;
    }

    /**
     * Greatest X coordinate of the rectangle.
     */
    public final double getRight() {
        return myRight;
    }

    /**
     * Rectangle area.
     */
    public final double area() {
        return (myBottom - myTop) * (myRight - myLeft);
    }

    /**
     * Area of covered rectangle for two specified rectangles.
     */
    public static double joinArea(final RectangleR2 a1stRectangle, final RectangleR2 a2ndRectangle) {
        final double left = a1stRectangle.myLeft < a2ndRectangle.myLeft ? a1stRectangle.myLeft : a2ndRectangle.myLeft;
        final double right = a1stRectangle.myRight > a2ndRectangle.myRight ? a1stRectangle.myRight
                : a2ndRectangle.myRight;
        final double top = a1stRectangle.myTop < a2ndRectangle.myTop ? a1stRectangle.myTop : a2ndRectangle.myTop;
        final double bottom = a1stRectangle.myBottom > a2ndRectangle.myBottom ? a1stRectangle.myBottom
                : a2ndRectangle.myBottom;

        return (bottom - top) * (right - left);
    }

    /**
     * Calculate distance from the specified point to the rectangle.
     */
    public double distance(final double aX, final double aY) {
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

        final double dx = aX < myLeft ? myLeft - aX : aX - myRight;
        final double dy = aY < myTop ? myTop - aY : aY - myBottom;

        return Math.sqrt(dx * dx + dy * dy);
    }

    /**
     * Clone rectangle.
     */
    @Override
    public Object clone() {
        try {
            final RectangleR2 r = (RectangleR2) super.clone();

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
    public final void join(final RectangleR2 aRectangle) {
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
    public static RectangleR2 join(final RectangleR2 a1stRectangle, final RectangleR2 a2ndRectangle) {
        final RectangleR2 rectangle = new RectangleR2(a1stRectangle);

        rectangle.join(a2ndRectangle);

        return rectangle;
    }

    /**
     * Checks if this rectangle intersects with specified rectangle.
     */
    public final boolean intersects(final RectangleR2 aRectangle) {
        return myLeft <= aRectangle.myRight && myTop <= aRectangle.myBottom && myRight >= aRectangle.myLeft &&
                myBottom >= aRectangle.myTop;
    }

    /**
     * Checks if this rectangle contains the specified rectangle.
     */
    public final boolean contains(final RectangleR2 aRectangle) {
        return myLeft <= aRectangle.myLeft && myTop <= aRectangle.myTop && myRight >= aRectangle.myRight &&
                myBottom >= aRectangle.myBottom;
    }

    /**
     * Check if two rectangles are equal.
     */
    @Override
    public boolean equals(final Object aObject) {
        if (aObject instanceof RectangleR2) {
            final RectangleR2 r = (RectangleR2) aObject;

            return myLeft == r.myLeft && myTop == r.myTop && myRight == r.myRight && myBottom == r.myBottom;
        }

        return false;
    }

    /**
     * Hash code consists of all rectangle coordinates.
     */
    @Override
    public int hashCode() {
        return (int) (Double.doubleToLongBits(myTop) ^ Double.doubleToLongBits(myBottom) << 1 ^ Double
                .doubleToLongBits(myLeft) << 2 ^ Double.doubleToLongBits(myRight) << 3);
    }

    @Override
    public String toString() {
        return "top=" + myTop + ", left=" + myLeft + ", bottom=" + myBottom + ", right=" + myRight;
    }

}
