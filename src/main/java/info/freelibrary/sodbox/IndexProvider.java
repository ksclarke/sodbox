
package info.freelibrary.sodbox;

/**
 * Interface used by Query to get index for the specified key.
 */
public interface IndexProvider {

    /**
     * Get index for the specified field of the class.
     * 
     * @param aClass class where index is located
     * @param aKey field of the class
     * @return Index for this field or null if index doesn't exist
     */
    GenericIndex getIndex(Class aClass, String aKey);

}
