
package info.freelibrary.sodbox;

/**
 * There are two kind of links in Sodbox: embedded and implemented as standalone object (collection). This interface
 * extends Link interface with getOwner/setOwner methods allowing to bind link with its container. Such binding is
 * needed for embedded link implementation to make modify() in link methods work properly. This interface is needed
 * mostly if you are going to provide you own implementation of embedded links.
 */
public interface EmbeddedLink<T> extends Link<T> {

    /**
     * Get container object for this embedded link.
     *
     * @return container object
     */
    Object getOwner();

    /**
     * Set container object for this embedded link.
     *
     * @param obj container object
     */
    void setOwner(Object obj);

}
