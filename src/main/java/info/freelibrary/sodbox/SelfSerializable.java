package info.freelibrary.sodbox;

/**
 * Interface of the classes left responsible for their serialization.
 */
public interface SelfSerializable {

	/**
	 * Serialize object.
	 * 
	 * @param out writer to be used for object serialization
	 */
	void pack(SodboxOutputStream out) throws java.io.IOException;

	/**
	 * Deserialize object.
	 * 
	 * @param in reader to be used for object deserialization
	 */
	void unpack(SodboxInputStream in) throws java.io.IOException;

}
