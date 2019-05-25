
package info.freelibrary.sodbox.impl;

class LRU {

    LRU myNext;

    LRU myPrevious;

    LRU() {
        myNext = myPrevious = this;
    }

    final void unlink() {
        myNext.myPrevious = myPrevious;
        myPrevious.myNext = myNext;
    }

    final void link(final LRU aNode) {
        aNode.myNext = myNext;
        aNode.myPrevious = this;
        myNext.myPrevious = aNode;
        myNext = aNode;
    }

}
