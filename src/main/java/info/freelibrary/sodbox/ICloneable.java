
package info.freelibrary.sodbox;

/**
 * This interface allows to clone its implementor. It is needed because Object.clone is protected and
 * java.lang.Cloneable interface contains no method definition.
 */
public interface ICloneable extends Cloneable {

    /**
     * Indicator that an object is cloneable.
     *
     * @return A clone of the object
     * @throws CloneNotSupportedException If the object doesn't support cloning
     */
    Object clone() throws CloneNotSupportedException;

}
