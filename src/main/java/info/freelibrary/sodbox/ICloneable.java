package info.freelibrary.sodbox;

/**
 * This interface allows to clone its implementor. It is needed because
 * Object.clone is protected and java.lang.Cloneable interface contains no
 * method definition
 */
public interface ICloneable extends Cloneable {
	Object clone() throws CloneNotSupportedException;
}
