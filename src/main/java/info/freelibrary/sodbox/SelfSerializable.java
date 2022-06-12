
package info.freelibrary.sodbox;

import java.io.IOException;

/**
 * Interface of the classes left responsible for their serialization.
 */
public interface SelfSerializable {

    /**
     * Serialize object.
     *
     * @param aOutputStream writer to be used for object serialization
     */
    void pack(SodboxOutputStream aOutputStream) throws IOException;

    /**
     * Deserialize object.
     *
     * @param aInputStream reader to be used for object deserialization
     */
    void unpack(SodboxInputStream aInputStream) throws IOException;

}
