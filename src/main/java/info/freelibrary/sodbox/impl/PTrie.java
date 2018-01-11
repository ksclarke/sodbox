
package info.freelibrary.sodbox.impl;

import java.util.ArrayList;
import java.util.Iterator;

import info.freelibrary.sodbox.PatriciaTrie;
import info.freelibrary.sodbox.PatriciaTrieKey;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentCollection;

class PTrie<T> extends PersistentCollection<T> implements PatriciaTrie<T> {

    private PTrieNode<T> myRootZero;

    private PTrieNode<T> myRootOne;

    private int myCount;

    private static <E> void fill(final ArrayList<E> aList, final PTrieNode<E> aNode) {
        if (aNode != null) {
            aList.add(aNode.myObj);
            fill(aList, aNode.myChildZero);
            fill(aList, aNode.myChildOne);
        }
    }

    private static int firstBit(final long aKey, final int aKeyLength) {
        return (int) (aKey >>> (aKeyLength - 1)) & 1;
    }

    private static int getCommonPartLength(long aKeyA, int aKeyLengthA, long aKeyB, int aKeyLengthB) {
        if (aKeyLengthA > aKeyLengthB) {
            aKeyA >>>= aKeyLengthA - aKeyLengthB;
            aKeyLengthA = aKeyLengthB;
        } else {
            aKeyB >>>= aKeyLengthB - aKeyLengthA;
            aKeyLengthB = aKeyLengthA;
        }

        long diff = aKeyA ^ aKeyB;
        int count = 0;

        while (diff != 0) {
            diff >>>= 1;
            count += 1;
        }

        return aKeyLengthA - count;
    }

    @Override
    public T add(final PatriciaTrieKey aKey, final T aObj) {
        modify();
        myCount += 1;

        if (firstBit(aKey.myMask, aKey.myLength) == 1) {
            if (myRootOne != null) {
                return myRootOne.add(aKey.myMask, aKey.myLength, aObj);
            } else {
                myRootOne = new PTrieNode<T>(aKey.myMask, aKey.myLength, aObj);
                return null;
            }
        } else {
            if (myRootZero != null) {
                return myRootZero.add(aKey.myMask, aKey.myLength, aObj);
            } else {
                myRootZero = new PTrieNode<T>(aKey.myMask, aKey.myLength, aObj);
                return null;
            }
        }
    }

    @Override
    public void clear() {
        if (myRootOne != null) {
            myRootOne.deallocate();
            myRootOne = null;
        }

        if (myRootZero != null) {
            myRootZero.deallocate();
            myRootZero = null;
        }

        myCount = 0;
    }

    @Override
    public ArrayList<T> elements() {
        final ArrayList<T> list = new ArrayList<T>(myCount);

        fill(list, myRootZero);
        fill(list, myRootOne);

        return list;
    }

    @Override
    public T findBestMatch(final PatriciaTrieKey aKey) {
        if (firstBit(aKey.myMask, aKey.myLength) == 1) {
            if (myRootOne != null) {
                return myRootOne.findBestMatch(aKey.myMask, aKey.myLength);
            }
        } else {
            if (myRootZero != null) {
                return myRootZero.findBestMatch(aKey.myMask, aKey.myLength);
            }
        }

        return null;
    }

    @Override
    public T findExactMatch(final PatriciaTrieKey aKey) {
        if (firstBit(aKey.myMask, aKey.myLength) == 1) {
            if (myRootOne != null) {
                return myRootOne.findExactMatch(aKey.myMask, aKey.myLength);
            }
        } else {
            if (myRootZero != null) {
                return myRootZero.findExactMatch(aKey.myMask, aKey.myLength);
            }
        }

        return null;
    }

    @Override
    public Iterator<T> iterator() {
        return elements().iterator();
    }

    @Override
    public T remove(final PatriciaTrieKey aKey) {
        if (firstBit(aKey.myMask, aKey.myLength) == 1) {
            if (myRootOne != null) {
                final T obj = myRootOne.remove(aKey.myMask, aKey.myLength);

                if (obj != null) {
                    modify();
                    myCount -= 1;

                    if (myRootOne.isNotUsed()) {
                        myRootOne.deallocate();
                        myRootOne = null;
                    }
                    return obj;
                }
            }
        } else {
            if (myRootZero != null) {
                final T obj = myRootZero.remove(aKey.myMask, aKey.myLength);

                if (obj != null) {
                    modify();
                    myCount -= 1;

                    if (myRootZero.isNotUsed()) {
                        myRootZero.deallocate();
                        myRootZero = null;
                    }

                    return obj;
                }
            }
        }

        return null;
    }

    @Override
    public int size() {
        return myCount;
    }

    @Override
    public Object[] toArray() {
        return elements().toArray();
    }

    @Override
    public <E> E[] toArray(final E[] aArray) {
        return elements().toArray(aArray);
    }

    static class PTrieNode<T> extends Persistent {

        long myKey;

        int myKeyLength;

        T myObj;

        PTrieNode<T> myChildZero;

        PTrieNode<T> myChildOne;

        PTrieNode() {
        }

        PTrieNode(final long aKey, final int aKeyLength, final T aObj) {
            this.myObj = aObj;
            this.myKey = aKey;
            this.myKeyLength = aKeyLength;
        }

        T add(final long aKey, final int aKeyLength, final T aObj) {
            if ((aKey == this.myKey) && (aKeyLength == this.myKeyLength)) {
                modify();
                final T prevObj = this.myObj;
                this.myObj = aObj;
                return prevObj;
            }

            final int keyLengthCommon = getCommonPartLength(aKey, aKeyLength, this.myKey, this.myKeyLength);
            int keyLengthDiff = this.myKeyLength - keyLengthCommon;
            final long keyCommon = aKey >>> (aKeyLength - keyLengthCommon);
            long keyDiff = this.myKey - (keyCommon << keyLengthDiff);

            if (keyLengthDiff > 0) {
                modify();
                final PTrieNode<T> newNode = new PTrieNode<T>(keyDiff, keyLengthDiff, this.myObj);
                newNode.myChildZero = myChildZero;
                newNode.myChildOne = myChildOne;

                this.myKey = keyCommon;
                this.myKeyLength = keyLengthCommon;
                this.myObj = null;

                if (firstBit(keyDiff, keyLengthDiff) == 1) {
                    myChildZero = null;
                    myChildOne = newNode;
                } else {
                    myChildZero = newNode;
                    myChildOne = null;
                }
            }

            if (aKeyLength > keyLengthCommon) {
                keyLengthDiff = aKeyLength - keyLengthCommon;
                keyDiff = aKey - (keyCommon << keyLengthDiff);

                if (firstBit(keyDiff, keyLengthDiff) == 1) {
                    if (myChildOne != null) {
                        return myChildOne.add(keyDiff, keyLengthDiff, aObj);
                    } else {
                        modify();
                        myChildOne = new PTrieNode<T>(keyDiff, keyLengthDiff, aObj);
                        return null;
                    }
                } else {
                    if (myChildZero != null) {
                        return myChildZero.add(keyDiff, keyLengthDiff, aObj);
                    } else {
                        modify();
                        myChildZero = new PTrieNode<T>(keyDiff, keyLengthDiff, aObj);
                        return null;
                    }
                }
            } else {
                final T prevObj = this.myObj;
                this.myObj = aObj;
                return prevObj;
            }
        }

        @Override
        public void deallocate() {
            if (myChildOne != null) {
                myChildOne.deallocate();
            }
            if (myChildZero != null) {
                myChildZero.deallocate();
            }
            super.deallocate();
        }

        T findBestMatch(final long aKey, final int aKeyLength) {
            if (aKeyLength > this.myKeyLength) {
                final int keyLengthCommon = getCommonPartLength(aKey, aKeyLength, this.myKey, this.myKeyLength);
                final int keyLengthDiff = aKeyLength - keyLengthCommon;
                final long keyCommon = aKey >>> keyLengthDiff;
                final long keyDiff = aKey - (keyCommon << keyLengthDiff);

                if (firstBit(keyDiff, keyLengthDiff) == 1) {
                    if (myChildOne != null) {
                        return myChildOne.findBestMatch(keyDiff, keyLengthDiff);
                    }
                } else {
                    if (myChildZero != null) {
                        return myChildZero.findBestMatch(keyDiff, keyLengthDiff);
                    }
                }
            }

            return myObj;
        }

        T findExactMatch(final long aKey, final int aKeyLength) {
            if (aKeyLength >= this.myKeyLength) {
                if ((aKey == this.myKey) && (aKeyLength == this.myKeyLength)) {
                    return myObj;
                } else {
                    final int keyLengthCommon = getCommonPartLength(aKey, aKeyLength, this.myKey, this.myKeyLength);

                    if (keyLengthCommon == this.myKeyLength) {
                        final int keyLengthDiff = aKeyLength - keyLengthCommon;
                        final long keyCommon = aKey >>> keyLengthDiff;
                        final long keyDiff = aKey - (keyCommon << keyLengthDiff);

                        if (firstBit(keyDiff, keyLengthDiff) == 1) {
                            if (myChildOne != null) {
                                return myChildOne.findBestMatch(keyDiff, keyLengthDiff);
                            }
                        } else {
                            if (myChildZero != null) {
                                return myChildZero.findBestMatch(keyDiff, keyLengthDiff);
                            }
                        }
                    }
                }
            }

            return null;
        }

        boolean isNotUsed() {
            return (myObj == null) && (myChildOne == null) && (myChildZero == null);
        }

        T remove(final long aKey, final int aKeyLength) {
            if (aKeyLength >= this.myKeyLength) {
                if ((aKey == this.myKey) && (aKeyLength == this.myKeyLength)) {
                    final T obj = this.myObj;

                    this.myObj = null;

                    return obj;
                } else {
                    final int keyLengthCommon = getCommonPartLength(aKey, aKeyLength, this.myKey, this.myKeyLength);
                    final int keyLengthDiff = aKeyLength - keyLengthCommon;
                    final long keyCommon = aKey >>> keyLengthDiff;
                    final long keyDiff = aKey - (keyCommon << keyLengthDiff);

                    if (firstBit(keyDiff, keyLengthDiff) == 1) {
                        if (myChildOne != null) {
                            final T obj = myChildOne.findBestMatch(keyDiff, keyLengthDiff);

                            if (obj != null) {
                                if (myChildOne.isNotUsed()) {
                                    modify();
                                    myChildOne.deallocate();
                                    myChildOne = null;
                                }

                                return obj;
                            }
                        }
                    } else {
                        if (myChildZero != null) {
                            final T obj = myChildZero.findBestMatch(keyDiff, keyLengthDiff);

                            if (obj != null) {
                                if (myChildZero.isNotUsed()) {
                                    modify();
                                    myChildZero.deallocate();
                                    myChildZero = null;
                                }

                                return obj;
                            }
                        }
                    }
                }
            }

            return null;
        }
    }
}
