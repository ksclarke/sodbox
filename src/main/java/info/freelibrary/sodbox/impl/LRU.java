
package info.freelibrary.sodbox.impl;

class LRU {

    LRU next;

    LRU prev;

    LRU() {
        next = prev = this;
    }

    final void link(final LRU node) {
        node.next = next;
        node.prev = this;
        next.prev = node;
        next = node;
    }

    final void unlink() {
        next.prev = prev;
        prev.next = next;
    }
}
