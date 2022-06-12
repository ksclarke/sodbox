
package info.freelibrary.sodbox;

/**
 * Interface for classes which need onLoad callback to be invoked when Sodbox loads object from the storage.
 */
public interface ILoadable {

    /**
     * Method called by the database after loading of the object. It can be used to initialize transient fields of the
     * object. Default implementation of this method do nothing.
     */
    void onLoad();

}
