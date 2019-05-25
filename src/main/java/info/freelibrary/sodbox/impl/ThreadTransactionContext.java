
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;
import java.util.IdentityHashMap;

/**
 * This class store transaction context associated with thread. Content of this class is opaque for application, but
 * it can use this context to share the single transaction between multiple threads
 */
public class ThreadTransactionContext {

    int myNested;

    IdentityHashMap myLocked = new IdentityHashMap();

    ArrayList myModified = new ArrayList();

    ArrayList myDeleted = new ArrayList();

}
