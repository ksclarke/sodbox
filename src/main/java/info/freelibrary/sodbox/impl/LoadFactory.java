
package info.freelibrary.sodbox.impl;

public interface LoadFactory {

    /**
     * Create an object.
     *
     * @param aClassDescriptor A class descriptor
     * @return The constructed object
     */
    Object create(ClassDescriptor aClassDescriptor);

}
