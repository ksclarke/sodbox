
package info.freelibrary.sodbox;

import java.io.IOException;

/**
 * Interface of custom serializer
 */
public interface CustomSerializer {

    /**
     * Serialize object
     * 
     * @param aObject object to be packed
     * @param aOutStream output stream to which object should be serialized
     */
    void pack(Object aObject, SodboxOutputStream aOutStream) throws IOException;

    /**
     * Deserialize object
     * 
     * @param aInStream input stream from which object should be deserialized
     * @return created and unpacked object
     */
    Object unpack(SodboxInputStream aInStream) throws IOException;

    /**
     * Create instance of specified class
     * 
     * @param aClass created object class
     */
    Object create(Class aClass);

    /**
     * Deserialize object
     * 
     * @param aObject unpacked object
     * @param aInStream input stream from which object should be deserialized
     */
    void unpack(Object aObject, SodboxInputStream aInStream) throws IOException;

    /**
     * Create object from its string representation
     * 
     * @param aString string representation of object (created by toString() method)
     */
    Object parse(String aString) throws Exception;

    /**
     * Get string representation of the object
     * 
     * @param aString object which string representation is taken
     */
    String print(Object aString);

    /**
     * Check if serializer can pack objects of this class
     * 
     * @param aClass inspected object class
     * @return true if serializer can pack instances of this class
     */
    boolean isApplicable(Class aClass);

    /**
     * Check if serializer can pack this object component
     * 
     * @param aObject object component to be packed
     * @return true if serializer can pack this object inside some other object
     */
    boolean isEmbedded(Object aObject);

}
