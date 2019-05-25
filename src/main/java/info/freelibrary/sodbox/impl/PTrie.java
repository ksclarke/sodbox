
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

    @Override
    public int size() {
        return myCount;
    }

    @Override
    public ArrayList<T> elements() {
        final ArrayList<T> list = new ArrayList<>(myCount);

        fill(list, myRootZero);
        fill(list, myRootOne);

        return list;
    }

    @Override
    public Object[] toArray() {
        return elements().toArray();
    }

    @Override
    public <E> E[] toArray(final E[] aArray) {
        return elements().toArray(aArray);
    }

    @Override
    public Iterator<T> iterator() {
        return elements().iterator();
    }

    private static <E> void fill(final ArrayList<E> aList, final PTrieNode<E> aNode) {
        if (aNode != null) {
            aList.add(aNode.myObject);
            fill(aList, aNode.myChildZero);
            fill(aList, aNode.myChildOne);
        }
    }

    private static int firstBit(final long aKey, final int aKeyLength) {
        return (int) (aKey >>> aKeyLength - 1) & 1;
    }

    private static int getCommonPartLength(final long aFirstKey, final int aFirstKeyLength, final long aSecondKey,
            final int aSecondKeyLength) {
        long secondKey = aSecondKey;
        long firstKey = aFirstKey;
        int firstKeyLength = aFirstKeyLength;
        int secondKeyLength = aSecondKeyLength;

        if (firstKeyLength > secondKeyLength) {
            firstKey >>>= firstKeyLength - secondKeyLength;
            firstKeyLength = secondKeyLength;
        } else {
            secondKey >>>= secondKeyLength - firstKeyLength;
            secondKeyLength = firstKeyLength;
        }

        long diff = firstKey ^ secondKey;
        int count = 0;

        while (diff != 0) {
            diff >>>= 1;
            count += 1;
        }

        return firstKeyLength - count;
    }

    @Override
    public T add(final PatriciaTrieKey aKey, final T aObject) {
        modify();
        myCount += 1;

        if (firstBit(aKey.myMask, aKey.myLength) == 1) {
            if (myRootOne != null) {
                return myRootOne.add(aKey.myMask, aKey.myLength, aObject);
            } else {
                myRootOne = new PTrieNode<>(aKey.myMask, aKey.myLength, aObject);
                return null;
            }
        } else {
            if (myRootZero != null) {
                return myRootZero.add(aKey.myMask, aKey.myLength, aObject);
            } else {
                myRootZero = new PTrieNode<>(aKey.myMask, aKey.myLength, aObject);
                return null;
            }
        }
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

    static class PTrieNode<T> extends Persistent {

        long myKey;

        int myKeyLength;

        T myObject;

        PTrieNode<T> myChildZero;

        PTrieNode<T> myChildOne;

        PTrieNode(final long aKey, final int aKeyLength, final T aObject) {
            myObject = aObject;
            myKey = aKey;
            myKeyLength = aKeyLength;
        }

        PTrieNode() {
        }

        T add(final long aKey, final int aKeyLength, final T aObj) {
            if (aKey == myKey && aKeyLength == myKeyLength) {
                modify();

                final T previousObj = myObject;

                myObject = aObj;

                return previousObj;
            }

            final int keyLengthCommon = getCommonPartLength(aKey, aKeyLength, myKey, myKeyLength);

            int keyLengthDiff = myKeyLength - keyLengthCommon;

            final long keyCommon = aKey >>> aKeyLength - keyLengthCommon;

            long keyDiff = this.myKey - (keyCommon << keyLengthDiff);

            if (keyLengthDiff > 0) {
                modify();

                final PTrieNode<T> newNode = new PTrieNode<>(keyDiff, keyLengthDiff, this.myObject);

                newNode.myChildZero = myChildZero;
                newNode.myChildOne = myChildOne;

                myKey = keyCommon;
                myKeyLength = keyLengthCommon;
                myObject = null;

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
                        myChildOne = new PTrieNode<>(keyDiff, keyLengthDiff, aObj);

                        return null;
                    }
                } else {
                    if (myChildZero != null) {
                        return myChildZero.add(keyDiff, keyLengthDiff, aObj);
                    } else {
                        modify();
                        myChildZero = new PTrieNode<>(keyDiff, keyLengthDiff, aObj);

                        return null;
                    }
                }
            } else {
                final T previousObj = myObject;

                myObject = aObj;

                return previousObj;
            }
        }

        T findBestMatch(final long aKey, final int aKeyLength) {
            if (aKeyLength > myKeyLength) {
                final int keyLengthCommon = getCommonPartLength(aKey, aKeyLength, myKey, myKeyLength);
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

            return myObject;
        }

        T findExactMatch(final long aKey, final int aKeyLength) {
            if (aKeyLength >= myKeyLength) {
                if (aKey == myKey && aKeyLength == myKeyLength) {
                    return myObject;
                } else {
                    final int keyLengthCommon = getCommonPartLength(aKey, aKeyLength, myKey, myKeyLength);

                    if (keyLengthCommon == myKeyLength) {
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
            return myObject == null && myChildOne == null && myChildZero == null;
        }

        T remove(final long aKey, final int aKeyLength) {
            if (aKeyLength >= myKeyLength) {
                if (aKey == myKey && aKeyLength == myKeyLength) {
                    final T obj = myObject;

                    myObject = null;

                    return obj;
                } else {
                    final int keyLengthCommon = getCommonPartLength(aKey, aKeyLength, myKey, myKeyLength);
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
    }

}
