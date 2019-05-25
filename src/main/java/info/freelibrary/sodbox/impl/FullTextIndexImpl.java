
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.Index;
import info.freelibrary.sodbox.Key;
import info.freelibrary.sodbox.Link;
import info.freelibrary.sodbox.Persistent;
import info.freelibrary.sodbox.PersistentResource;
import info.freelibrary.sodbox.Storage;
import info.freelibrary.sodbox.StorageError;
import info.freelibrary.sodbox.fulltext.FullTextIndex;
import info.freelibrary.sodbox.fulltext.FullTextQuery;
import info.freelibrary.sodbox.fulltext.FullTextQueryBinaryOp;
import info.freelibrary.sodbox.fulltext.FullTextQueryMatchOp;
import info.freelibrary.sodbox.fulltext.FullTextQueryUnaryOp;
import info.freelibrary.sodbox.fulltext.FullTextQueryVisitor;
import info.freelibrary.sodbox.fulltext.FullTextSearchHelper;
import info.freelibrary.sodbox.fulltext.FullTextSearchHit;
import info.freelibrary.sodbox.fulltext.FullTextSearchResult;
import info.freelibrary.sodbox.fulltext.FullTextSearchable;
import info.freelibrary.sodbox.fulltext.Occurrence;

public class FullTextIndexImpl extends PersistentResource implements FullTextIndex {

    static final int OCC_KIND_OFFSET = 24;

    static final int OCC_POSITION_MASK = (1 << OCC_KIND_OFFSET) - 1;

    static final int COMPRESSION_OVERHEAD = 8;

    protected Index myInverseIndex;

    protected Index myDocuments;

    protected FullTextSearchHelper myHelper;

    /**
     * Creates a full-text index.
     *
     * @param aStorage A database storage
     * @param aSearchHelper A full text search helper
     */
    public FullTextIndexImpl(final Storage aStorage, final FullTextSearchHelper aSearchHelper) {
        super(aStorage);

        this.myHelper = aSearchHelper;
        myInverseIndex = aStorage.createIndex(String.class, true);
        myDocuments = aStorage.createIndex(Object.class, true);
    }

    @SuppressWarnings("unused")
    private FullTextIndexImpl() {
    }

    @Override
    public Iterator<Keyword> getKeywords(final String aPrefix) {
        return new KeywordIterator(myInverseIndex.entryIterator(new Key(aPrefix), new Key(aPrefix + Character.MAX_VALUE,
                false), Index.ASCENT_ORDER));
    }

    @Override
    public FullTextSearchResult search(final FullTextQuery aQuery, final int aMaxResults, final int aTimeLimit) {
        final FullTextSearchEngine engine = new FullTextSearchEngine();

        return engine.search(aQuery, aMaxResults, aTimeLimit);
    }

    @Override
    public FullTextSearchResult searchPrefix(final String aPrefix, final int aMaxResults, final int aTimeLimit,
            final boolean aSort) {
        final FullTextSearchEngine engine = new FullTextSearchEngine();

        return engine.searchPrefix(aPrefix, aMaxResults, aTimeLimit, aSort);
    }

    @Override
    public FullTextSearchHelper getHelper() {
        return myHelper;
    }

    @Override
    public void add(final FullTextSearchable aObject) {
        add(aObject, aObject.getText(), aObject.getLanguage());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(final Object aObject, final Reader aText, final String aLanguage) {
        final Occurrence[] occurrences;

        try {
            occurrences = myHelper.parseText(aText);
        } catch (final IOException x) {
            throw new StorageError(StorageError.FULL_TEXT_INDEX_ERROR, x);
        }

        delete(aObject);

        if (occurrences.length > 0) {
            final Document doc = new Document(getStorage(), aObject);

            myDocuments.put(new Key(aObject), doc);
            Arrays.sort(occurrences);
            String word = occurrences[0].myWord;
            int i = 0;

            for (int j = 1; j < occurrences.length; j++) {
                final Occurrence occ = occurrences[j];

                if (!occ.myWord.equals(word)) {
                    addReference(doc, word, occurrences, i, j, aLanguage);
                    word = occ.myWord;
                    i = j;
                }
            }

            addReference(doc, word, occurrences, i, occurrences.length, aLanguage);
        }
    }

    @SuppressWarnings("unchecked")
    private void addReference(final Document aDoc, final String aWord, final Occurrence[] aOccurrences,
            final int aFrom, final int aTo) {
        final DocumentOccurrences d = new DocumentOccurrences();
        final int[] occ = new int[aTo - aFrom];

        d.myNumWordsInDoc = aOccurrences.length;

        for (int i = aFrom; i < aTo; i++) {
            occ[i - aFrom] = aOccurrences[i].myPosition | aOccurrences[i].myKind << OCC_KIND_OFFSET;
        }

        d.setOccurrences(occ);

        final int oid = getStorage().getOid(aDoc.myObject);
        InverseList list = (InverseList) myInverseIndex.get(aWord);

        if (list == null) {
            list = new InverseList(getStorage(), oid, d);
            myInverseIndex.put(aWord, list);
        } else {
            list.add(oid, d);
        }

        d.myInverseList = list;
        d.modify();
        aDoc.myOccurrences.add(d);
    }

    private void addReference(final Document aDoc, final String aWord, final Occurrence[] aOccurrences,
            final int aFrom, final int aTo, final String aLanguage) {
        final String[] normalForms = myHelper.getNormalForms(aWord, aLanguage);

        boolean isNormalForm = false;

        for (int i = 0; i < normalForms.length; i++) {
            if (aWord.equals(normalForms[i])) {
                isNormalForm = true;
            }

            addReference(aDoc, normalForms[i], aOccurrences, aFrom, aTo);
        }

        if (!isNormalForm) {
            addReference(aDoc, aWord, aOccurrences, aFrom, aTo);
        }
    }

    @Override
    public void delete(final Object aObject) {
        final Key key = new Key(aObject);
        final Document doc = (Document) myDocuments.get(key);

        if (doc != null) {
            for (int i = 0, n = doc.myOccurrences.size(); i < n; i++) {
                final DocumentOccurrences d = (DocumentOccurrences) doc.myOccurrences.get(i);

                d.myInverseList.remove(getStorage().getOid(aObject));
                d.deallocate();
            }

            myDocuments.remove(key);
            doc.deallocate();
        }
    }

    @Override
    public void deallocate() {
        clear();
        super.deallocate();
    }

    @Override
    public void clear() {
        myInverseIndex.deallocateMembers();
        myDocuments.deallocateMembers();
    }

    @Override
    public int getNumberOfWords() {
        return myInverseIndex.size();
    }

    @Override
    public int getNumberOfDocuments() {
        return myDocuments.size();
    }

    @Override
    public FullTextSearchResult search(final String aQuery, final String aLanguage, final int aMaxResults,
            final int aTimeLimit) {
        return search(myHelper.parseQuery(aQuery, aLanguage), aMaxResults, aTimeLimit);
    }

    static class DocumentOccurrences extends Persistent {

        InverseList myInverseList;

        int myNumWordsInDoc;

        byte[] myOccurrences;

        final void setOccurrences(final int[] aOccurrences) {
            final int len = aOccurrences.length;
            final Compressor compressor = new Compressor(new byte[len * 4 + COMPRESSION_OVERHEAD]);

            int i = 0;
            int prevOcc = -1;

            compressor.encodeStart();
            compressor.encode(len);

            do {
                final int kind = aOccurrences[i] >>> OCC_KIND_OFFSET;

                int j = i;

                while (++j < len && aOccurrences[j] >>> OCC_KIND_OFFSET == kind) {
                }

                compressor.encode(j - i);
                compressor.encode(kind + 1);

                do {
                    final int currOcc = aOccurrences[i++] & OCC_POSITION_MASK;

                    compressor.encode(currOcc - prevOcc);
                    prevOcc = currOcc;
                } while (i != j);
            } while (i != len);

            myOccurrences = compressor.encodeStop();
        }

        final int[] getOccurrences() {
            final Compressor compressor = new Compressor(myOccurrences);
            int i = 0;
            compressor.decodeStart();
            final int len = compressor.decode();
            final int[] buf = new int[len];
            int pos = -1;

            do {
                int n = compressor.decode();

                final int kind = compressor.decode() - 1 << OCC_KIND_OFFSET;

                do {
                    pos += compressor.decode();
                    buf[i++] = kind | pos;
                } while (--n != 0);
            } while (i != len);

            return buf;
        }
    }

    static class Document extends Persistent {

        Object myObject;

        Link myOccurrences;

        Document() {
        }

        Document(final Storage aStorage, final Object aObject) {
            super(aStorage);

            myObject = aObject;
            myOccurrences = aStorage.createLink();
        }
    }

    static class InverseList extends Btree {

        static final int BTREE_THRESHOLD = 500;

        int[] myOIDs;

        Link myDocs;

        @SuppressWarnings("unchecked")
        InverseList(final Storage aDB, final int aOID, final DocumentOccurrences aDoc) {
            super(int.class, true);

            myDocs = aDB.createLink(1);
            myDocs.add(aDoc);
            myOIDs = new int[1];
            myOIDs[0] = aOID;
            assignOid(aDB, 0, false);
        }

        InverseList() {
        }

        @Override
        public int size() {
            return myOIDs != null ? myOIDs.length : super.size();
        }

        int first() {
            if (myOIDs != null) {
                return myOIDs[0];
            }

            final Map.Entry entry = (Map.Entry) entryIterator(null, null, Index.ASCENT_ORDER).next();

            return ((Integer) entry.getKey()).intValue();
        }

        int last() {
            if (myOIDs != null) {
                return myOIDs[myOIDs.length - 1];
            }

            final Map.Entry entry = (Map.Entry) entryIterator(null, null, Index.DESCENT_ORDER).next();

            return ((Integer) entry.getKey()).intValue();
        }

        Iterator iterator(final int aOID) {
            final int[] os = myOIDs;

            if (os != null) {
                int l = 0;
                int r = os.length;

                while (l < r) {
                    final int m = l + r >>> 1;

                    if (os[m] < aOID) {
                        l = m + 1;
                    } else {
                        r = m;
                    }
                }

                return new InverstListIterator(r);
            } else {
                return entryIterator(new Key(aOID), null, Index.ASCENT_ORDER);
            }
        }

        @SuppressWarnings("unchecked")
        void add(final int aOID, final DocumentOccurrences aDocs) {
            int[] os = myOIDs;

            if (os == null || os.length >= BTREE_THRESHOLD) {
                if (os != null) {
                    for (int i = 0; i < os.length; i++) {
                        super.put(new Key(os[i]), myDocs.get(i));
                    }

                    myOIDs = null;
                    myDocs = null;
                }

                super.put(new Key(aOID), aDocs);
            } else {
                final int n = os.length;

                int l = 0;
                int r = n;

                while (l < r) {
                    final int m = l + r >>> 1;

                    if (os[m] < aOID) {
                        l = m + 1;
                    } else {
                        r = m;
                    }
                }

                os = new int[n + 1];

                System.arraycopy(myOIDs, 0, os, 0, r);

                os[r] = aOID;

                System.arraycopy(myOIDs, r, os, r + 1, n - r);

                myDocs.insert(r, aDocs);
                myOIDs = os;
                modify();
            }
        }

        void remove(final int aOID) {
            final int[] os = myOIDs;

            if (os != null) {
                int l = 0;

                final int n = os.length;

                int r = n;

                while (l < r) {
                    final int m = l + r >>> 1;

                    if (os[m] < aOID) {
                        l = m + 1;
                    } else {
                        r = m;
                    }
                }

                Assert.that(r < n && os[r] == aOID);

                myDocs.remove(r);
                myOIDs = new int[n - 1];

                System.arraycopy(os, 0, myOIDs, 0, r);
                System.arraycopy(os, r + 1, myOIDs, r, n - r - 1);

                modify();
            } else {
                super.remove(new Key(aOID));
            }
        }

        class InverstListIterator implements Iterator {

            int myIndex;

            InverstListIterator(final int aPosition) {
                myIndex = aPosition;
            }

            @Override
            public boolean hasNext() {
                return myIndex < myOIDs.length;
            }

            @Override
            public Object next() {
                final int index = myIndex++;

                return new Map.Entry() {

                    @Override
                    public Object getKey() {
                        return new Integer(myOIDs[index]);
                    }

                    @Override
                    public Object getValue() {
                        return myDocs.get(index);
                    }

                    @Override
                    public Object setValue(final Object aValue) {
                        return null;
                    }

                    @SuppressWarnings("unused")
                    public Object setKey(final Object aKey) {
                        return null;
                    }
                };
            }

            @Override
            public void remove() {
            }
        }
    }

    protected static class KeywordList {

        InverseList myList;

        int[] myOccurrences;

        String myWord;

        int mySameAs;

        int myKeywordLength;

        int myKeywordOffset;

        int myOccurrencePos;

        int myCurrentDoc;

        Map.Entry myCurrentEntry;

        Iterator myIterator;

        KeywordList(final String aWord) {
            myWord = aWord;
            myKeywordLength = aWord.length();
            mySameAs = -1;
        }
    }

    static class KeywordImpl implements Keyword {

        Map.Entry myEntry;

        KeywordImpl(final Map.Entry aEntry) {
            myEntry = aEntry;
        }

        @Override
        public String getNormalForm() {
            return (String) myEntry.getKey();
        }

        @Override
        public long getNumberOfOccurrences() {
            return ((InverseList) myEntry.getValue()).size();
        }

    }

    static class KeywordIterator implements Iterator<Keyword> {

        Iterator myIterator;

        KeywordIterator(final Iterator aIterator) {
            myIterator = aIterator;
        }

        @Override
        public boolean hasNext() {
            return myIterator.hasNext();
        }

        @Override
        public Keyword next() {
            return new KeywordImpl((Map.Entry) myIterator.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    static class ExpressionWeight implements Comparable {

        int myWeight;

        FullTextQuery myExpr;

        @Override
        public int compareTo(final Object aObject) {
            return myWeight - ((ExpressionWeight) aObject).myWeight;
        }
    }

    protected class FullTextSearchEngine extends FullTextQueryVisitor {

        static final int STRICT_MATCH_BONUS = 8;

        static final double DENSITY_MAGIC = 2;

        KeywordList[] myKeywords;

        ArrayList myKeywordList;

        int[] myOccurrences;

        int myNumOfOccurrences;

        float[] myOccurrenceKindWeight;

        @SuppressWarnings("unchecked")
        @Override
        public void visit(final FullTextQueryMatchOp aMatchOp) {
            aMatchOp.myWordInQueryIndex = myKeywordList.size();
            final KeywordList kwd = new KeywordList(aMatchOp.myWord);
            kwd.myList = (InverseList) myInverseIndex.get(aMatchOp.myWord);
            myKeywordList.add(kwd);
        }

        int calculateWeight(final FullTextQuery aQuery) {
            switch (aQuery.myOp) {
                case FullTextQuery.AND: {
                    return calculateWeight(((FullTextQueryBinaryOp) aQuery).myLeft);
                }
                case FullTextQuery.NEAR: {
                    int shift = STRICT_MATCH_BONUS;

                    for (FullTextQuery q = ((FullTextQueryBinaryOp) aQuery).myRight; q.myOp == FullTextQuery.NEAR; q =
                            ((FullTextQueryBinaryOp) q).myRight) {
                        shift += STRICT_MATCH_BONUS;
                    }

                    return shift >= 32 ? 0 : calculateWeight(((FullTextQueryBinaryOp) aQuery).myLeft) >> shift;
                }
                case FullTextQuery.OR: {
                    final int leftWeight = calculateWeight(((FullTextQueryBinaryOp) aQuery).myLeft);
                    final int rightWeight = calculateWeight(((FullTextQueryBinaryOp) aQuery).myRight);

                    return leftWeight > rightWeight ? leftWeight : rightWeight;
                }
                case FullTextQuery.MATCH:
                case FullTextQuery.STRICT_MATCH: {
                    final InverseList list = myKeywords[((FullTextQueryMatchOp) aQuery).myWordInQueryIndex].myList;

                    return list == null ? 0 : list.size();
                }
                default:
                    return Integer.MAX_VALUE;
            }
        }

        FullTextQuery optimize(final FullTextQuery aQuery) {
            switch (aQuery.myOp) {
                case FullTextQuery.AND:
                case FullTextQuery.NEAR: {
                    final int op = aQuery.myOp;

                    int nConjuncts = 1;
                    FullTextQuery q = aQuery;

                    while ((q = ((FullTextQueryBinaryOp) q).myRight).myOp == op) {
                        nConjuncts += 1;
                    }

                    final ExpressionWeight[] conjuncts = new ExpressionWeight[nConjuncts + 1];

                    q = aQuery;

                    for (int i = 0; i < nConjuncts; i++) {
                        final FullTextQueryBinaryOp and = (FullTextQueryBinaryOp) q;

                        conjuncts[i] = new ExpressionWeight();
                        conjuncts[i].myExpr = optimize(and.myLeft);
                        conjuncts[i].myWeight = calculateWeight(conjuncts[i].myExpr);
                        q = and.myRight;
                    }

                    conjuncts[nConjuncts] = new ExpressionWeight();
                    conjuncts[nConjuncts].myExpr = optimize(q);
                    conjuncts[nConjuncts].myWeight = calculateWeight(conjuncts[nConjuncts].myExpr);

                    Arrays.sort(conjuncts);

                    if (op == FullTextQuery.AND) { // eliminate duplicates
                        InverseList list = null;
                        int n = 0;
                        int j = -1;

                        for (int i = 0; i <= nConjuncts; i++) {
                            q = conjuncts[i].myExpr;

                            if (q instanceof FullTextQueryMatchOp) {
                                final FullTextQueryMatchOp match = (FullTextQueryMatchOp) q;

                                if (n == 0 || myKeywords[match.myWordInQueryIndex].myList != list) {
                                    j = match.myWordInQueryIndex;
                                    list = myKeywords[j].myList;
                                    conjuncts[n++] = conjuncts[i];
                                } else {
                                    myKeywords[match.myWordInQueryIndex].mySameAs = j;
                                }
                            } else {
                                conjuncts[n++] = conjuncts[i];
                            }
                        }

                        nConjuncts = n - 1;
                    } else { // calculate distance between keywords
                        int kwdPos = 0;

                        for (int i = 0; i <= nConjuncts; i++) {
                            q = conjuncts[i].myExpr;

                            if (q instanceof FullTextQueryMatchOp) {
                                final FullTextQueryMatchOp match = (FullTextQueryMatchOp) q;

                                myKeywords[match.myWordInQueryIndex].myKeywordOffset = match.myPosition - kwdPos;
                                kwdPos = match.myPosition;
                            }
                        }
                    }

                    if (nConjuncts == 0) {
                        return conjuncts[0].myExpr;
                    } else {
                        q = aQuery;
                        int i = 0;

                        while (true) {
                            final FullTextQueryBinaryOp and = (FullTextQueryBinaryOp) q;

                            and.myLeft = conjuncts[i].myExpr;

                            if (++i < nConjuncts) {
                                q = and.myRight;
                            } else {
                                and.myRight = conjuncts[i].myExpr;
                                break;
                            }
                        }
                    }

                    break;
                }
                case FullTextQuery.OR: {
                    final FullTextQueryBinaryOp or = (FullTextQueryBinaryOp) aQuery;
                    or.myLeft = optimize(or.myLeft);
                    or.myRight = optimize(or.myRight);
                    break;
                }
                case FullTextQuery.NOT: {
                    final FullTextQueryUnaryOp not = (FullTextQueryUnaryOp) aQuery;

                    not.myQuery = optimize(not.myQuery);
                    break;
                }
                default:
            }

            return aQuery;
        }

        int intersect(final int aDoc, final FullTextQuery aQuery) {
            final int right;

            int doc = aDoc;
            int left;

            switch (aQuery.myOp) {
                case FullTextQuery.AND:
                case FullTextQuery.NEAR:
                    do {
                        left = intersect(aDoc, ((FullTextQueryBinaryOp) aQuery).myLeft);

                        if (left == Integer.MAX_VALUE) {
                            return left;
                        }

                        doc = intersect(left, ((FullTextQueryBinaryOp) aQuery).myRight);
                    } while (left != doc && doc != Integer.MAX_VALUE);

                    return doc;
                case FullTextQuery.OR:
                    left = intersect(doc, ((FullTextQueryBinaryOp) aQuery).myLeft);
                    right = intersect(doc, ((FullTextQueryBinaryOp) aQuery).myRight);

                    return left < right ? left : right;
                case FullTextQuery.MATCH:
                case FullTextQuery.STRICT_MATCH: {
                    final KeywordList kwd = myKeywords[((FullTextQueryMatchOp) aQuery).myWordInQueryIndex];

                    if (kwd.myCurrentDoc >= doc) {
                        return kwd.myCurrentDoc;
                    }

                    Iterator iterator = kwd.myIterator;

                    if (iterator != null) {
                        if (iterator.hasNext()) {
                            final Map.Entry entry = (Map.Entry) iterator.next();
                            final int nextDoc = ((Integer) entry.getKey()).intValue();

                            if (nextDoc >= doc) {
                                kwd.myCurrentEntry = entry;
                                kwd.myCurrentDoc = nextDoc;

                                return nextDoc;
                            }
                        } else {
                            kwd.myCurrentEntry = null;
                            kwd.myCurrentDoc = 0;

                            return Integer.MAX_VALUE;
                        }
                    }

                    if (kwd.myList != null) {
                        kwd.myIterator = iterator = kwd.myList.iterator(doc);

                        if (iterator.hasNext()) {
                            final Map.Entry entry = (Map.Entry) iterator.next();

                            doc = ((Integer) entry.getKey()).intValue();
                            kwd.myCurrentEntry = entry;
                            kwd.myCurrentDoc = doc;

                            return doc;
                        }
                    }

                    kwd.myCurrentEntry = null;
                    kwd.myCurrentDoc = 0;

                    return Integer.MAX_VALUE;
                }
                case FullTextQuery.NOT: {
                    final int nextDoc = intersect(doc, ((FullTextQueryUnaryOp) aQuery).myQuery);

                    if (nextDoc == doc) {
                        doc += 1;
                    }

                    return doc;
                }
                default:
                    return doc;
            }
        }

        int calculateEstimation(final FullTextQuery aQuery, final int aNumOfResults) {
            switch (aQuery.myOp) {
                case FullTextQuery.AND:
                case FullTextQuery.NEAR: {
                    final int left = calculateEstimation(((FullTextQueryBinaryOp) aQuery).myLeft, aNumOfResults);
                    final int right = calculateEstimation(((FullTextQueryBinaryOp) aQuery).myRight, aNumOfResults);

                    return left < right ? left : right;
                }
                case FullTextQuery.OR: {
                    final int left = calculateEstimation(((FullTextQueryBinaryOp) aQuery).myLeft, aNumOfResults);
                    final int right = calculateEstimation(((FullTextQueryBinaryOp) aQuery).myRight, aNumOfResults);

                    return left > right ? left : right;
                }
                case FullTextQuery.MATCH:
                case FullTextQuery.STRICT_MATCH: {
                    final KeywordList kwd = myKeywords[((FullTextQueryMatchOp) aQuery).myWordInQueryIndex];

                    if (kwd.myCurrentDoc == 0) {
                        return 0;
                    } else {
                        final int curr = kwd.myCurrentDoc;
                        final int first = kwd.myList.first();
                        final int last = kwd.myList.last();

                        int estimation = aNumOfResults * (last - first + 1) / (curr - first + 1);

                        if (estimation > kwd.myList.size()) {
                            estimation = kwd.myList.size();
                        }

                        return estimation;
                    }
                }
                case FullTextQuery.NOT:
                    return myDocuments.size();
                default:
            }

            return 0;
        }

        double evaluate(final int aDocIndex, final FullTextQuery aQuery) {
            final double left;
            final double right;

            switch (aQuery.myOp) {
                case FullTextQuery.NEAR:
                case FullTextQuery.AND:
                    left = evaluate(aDocIndex, ((FullTextQueryBinaryOp) aQuery).myLeft);
                    right = evaluate(aDocIndex, ((FullTextQueryBinaryOp) aQuery).myRight);
                    myNumOfOccurrences = 0;

                    return left < 0 || right < 0 ? -1 : left + right;
                case FullTextQuery.OR:
                    left = evaluate(aDocIndex, ((FullTextQueryBinaryOp) aQuery).myLeft);
                    right = evaluate(aDocIndex, ((FullTextQueryBinaryOp) aQuery).myRight);

                    return left > right ? left : right;
                case FullTextQuery.MATCH:
                case FullTextQuery.STRICT_MATCH: {
                    final KeywordList kwd = myKeywords[((FullTextQueryMatchOp) aQuery).myWordInQueryIndex];

                    if (kwd.myCurrentDoc != aDocIndex) {
                        return -1;
                    }

                    final DocumentOccurrences d = (DocumentOccurrences) kwd.myCurrentEntry.getValue();
                    final int[] occ = d.getOccurrences();

                    kwd.myOccurrences = occ;
                    final int frequency = occ.length;

                    if (aQuery.myOp == FullTextQuery.STRICT_MATCH) {
                        if (myNumOfOccurrences == 0) {
                            myNumOfOccurrences = frequency;

                            if (myOccurrences == null || myOccurrences.length < frequency) {
                                myOccurrences = new int[frequency];
                            }

                            for (int i = 0; i < frequency; i++) {
                                myOccurrences[i] = occ[i] & OCC_POSITION_MASK;
                            }
                        } else {
                            final int offs = kwd.myKeywordOffset;
                            final int[] dst = myOccurrences;

                            int nPairs = 0;
                            int occ1 = dst[0];
                            int occ2 = occ[0] & OCC_POSITION_MASK;
                            int i = 0;
                            int j = 0;

                            while (true) {
                                if (occ1 + offs <= occ2) {
                                    if (occ1 + offs + 1 >= occ2) {
                                        dst[nPairs++] = occ2;
                                    }

                                    if (++j == myNumOfOccurrences) {
                                        break;
                                    }

                                    occ1 = dst[j];
                                } else {
                                    if (++i == frequency) {
                                        break;
                                    }

                                    occ2 = occ[i] & OCC_POSITION_MASK;
                                }
                            }

                            myNumOfOccurrences = nPairs;

                            if (nPairs == 0) {
                                return -1;
                            }
                        }
                    }

                    return calculateKwdRank(kwd.myList, d, occ);
                }
                case FullTextQuery.NOT: {
                    final double rank = evaluate(aDocIndex, ((FullTextQueryUnaryOp) aQuery).myQuery);

                    return rank >= 0 ? -1 : 0;
                }
                default:
                    return -1;
            }
        }

        final double calculateKwdRank(final InverseList aInverseList, final DocumentOccurrences aDocOccurrences,
                final int[] aOccurrences) {
            final int frequency = aOccurrences.length;
            final int totalNumberOfDocuments = myDocuments.size();
            final int nRelevantDocuments = aInverseList.size();
            final int totalNumberOfWords = myInverseIndex.size();
            final double idf = Math.log((double) totalNumberOfDocuments / nRelevantDocuments);
            final double averageWords = (double) totalNumberOfWords / totalNumberOfDocuments;
            final double density = frequency * Math.log(1 + DENSITY_MAGIC * averageWords /
                    aDocOccurrences.myNumWordsInDoc);
            final double wordWeight = density * idf;

            double wordScore = 1;

            for (int i = 0; i < frequency; i++) {
                wordScore += wordWeight * myOccurrenceKindWeight[aOccurrences[i] >>> OCC_KIND_OFFSET];
            }

            return Math.log(wordScore);
        }

        void buildOccurrenceKindWeightTable() {
            final float[] weights = myHelper.getOccurrenceKindWeights();

            myOccurrenceKindWeight = new float[256];
            myOccurrenceKindWeight[0] = 1.0f;

            for (int i = 1; i < 256; i++) {
                float weight = 0;

                for (int j = 0; j < weights.length; j++) {
                    if ((i & 1 << j) != 0) {
                        weight += weights[j];
                    }

                    myOccurrenceKindWeight[i] = weight;
                }
            }
        }

        double calculateNearness() {
            final KeywordList[] kwds = this.myKeywords;
            final int nKwds = kwds.length;

            if (nKwds < 2) {
                return 0;
            }

            for (int i = 0; i < nKwds; i++) {
                if (kwds[i].myOccurrences == null) {
                    final int j = kwds[i].mySameAs;

                    if (j >= 0 && kwds[j].myOccurrences != null) {
                        kwds[i].myOccurrences = kwds[j].myOccurrences;
                    } else {
                        return 0;
                    }
                }

                kwds[i].myOccurrencePos = 0;
            }

            final int swapPenalty = myHelper.getWordSwapPenalty();
            double maxNearness = 0;

            while (true) {
                int minPos = Integer.MAX_VALUE;
                double nearness = 0;
                KeywordList first = null;
                KeywordList prev = null;

                for (int i = 0; i < nKwds; i++) {
                    final KeywordList curr = kwds[i];

                    if (curr.myOccurrencePos < curr.myOccurrences.length) {
                        if (prev != null) {
                            int offset = curr.myOccurrences[curr.myOccurrencePos] -
                                    prev.myOccurrences[prev.myOccurrencePos];

                            if (offset < 0) {
                                offset = (-offset - curr.myKeywordLength) * swapPenalty;
                            } else {
                                offset -= prev.myKeywordLength;
                            }

                            if (offset <= 2) {
                                offset = 1;
                            }

                            nearness += 1 / Math.sqrt(offset);
                        }

                        if (curr.myOccurrences[curr.myOccurrencePos] < minPos) {
                            minPos = curr.myOccurrences[curr.myOccurrencePos];
                            first = curr;
                        }

                        prev = curr;
                    }
                }

                if (first == null) {
                    break;
                }

                first.myOccurrencePos += 1;

                if (nearness > maxNearness) {
                    maxNearness = nearness;
                }
            }

            return maxNearness;
        }

        void reset() {
            myNumOfOccurrences = 0;

            for (int i = 0; i < myKeywords.length; i++) {
                myKeywords[i].myOccurrences = null;
            }
        }

        FullTextSearchResult searchPrefix(final String aPrefix, final int aMaxResults, final int aTimeLimit,
                final boolean aSort) {
            final Iterator lists = myInverseIndex.prefixIterator(aPrefix);
            final long start = System.currentTimeMillis();

            FullTextSearchHit[] hits = new FullTextSearchHit[aMaxResults];
            int nResults = 0;
            int estimation = 0;

            JoinLists:
            while (lists.hasNext()) {
                final InverseList list = (InverseList) lists.next();
                final Iterator occurrences = list.iterator(0);

                estimation += list.size();

                while (occurrences.hasNext()) {
                    final Map.Entry entry = (Map.Entry) occurrences.next();
                    final int doc = ((Integer) entry.getKey()).intValue();

                    float rank = 1.0f;

                    if (aSort) {
                        final DocumentOccurrences d = (DocumentOccurrences) entry.getValue();
                        rank = (float) calculateKwdRank(list, d, d.getOccurrences());
                    }

                    hits[nResults] = new FullTextSearchHit(getStorage(), doc, rank);

                    if (++nResults >= aMaxResults || System.currentTimeMillis() >= start + aTimeLimit) {
                        break JoinLists;
                    }
                }
            }

            if (nResults < aMaxResults) {
                final FullTextSearchHit[] realHits = new FullTextSearchHit[nResults];

                System.arraycopy(hits, 0, realHits, 0, nResults);

                hits = realHits;
            }

            if (aSort) {
                Arrays.sort(hits);
            }

            return new FullTextSearchResult(hits, estimation);
        }

        @SuppressWarnings("unchecked")
        FullTextSearchResult search(final FullTextQuery aQuery, final int aMaxResults, final int aTimeLimit) {
            FullTextQuery query = aQuery;

            if (query == null || !query.isConstrained()) {
                return null;
            }

            final long start = System.currentTimeMillis();
            final int estimation;

            buildOccurrenceKindWeightTable();
            myKeywordList = new ArrayList();
            query.visit(this);
            myKeywords = (KeywordList[]) myKeywordList.toArray(new KeywordList[myKeywordList.size()]);
            query = optimize(query);
            // System.out.println(query.toString());
            FullTextSearchHit[] hits = new FullTextSearchHit[aMaxResults];
            int currDoc = 1;
            int nResults = 0;
            final float nearnessWeight = myHelper.getNearnessWeight();
            boolean noMoreMatches = false;

            while (nResults < aMaxResults && System.currentTimeMillis() < start + aTimeLimit) {
                currDoc = intersect(currDoc, aQuery);

                if (currDoc == Integer.MAX_VALUE) {
                    noMoreMatches = true;
                    break;
                }

                reset();

                final double kwdRank = evaluate(currDoc, query);

                if (kwdRank >= 0) {
                    final float rank = (float) (kwdRank * (1 + calculateNearness() * nearnessWeight));

                    // System.out.println("kwdRank=" + kwdRank + ", nearness=" + calculateNearness() +
                    // ", total rank=" + rank);
                    hits[nResults++] = new FullTextSearchHit(getStorage(), currDoc, rank);
                }

                currDoc += 1;
            }

            if (nResults < aMaxResults) {
                final FullTextSearchHit[] realHits = new FullTextSearchHit[nResults];

                System.arraycopy(hits, 0, realHits, 0, nResults);

                hits = realHits;
            }

            if (noMoreMatches) {
                estimation = nResults;
            } else if (aQuery instanceof FullTextQueryMatchOp) {
                estimation = myKeywords[0].myList.size();
            } else {
                estimation = calculateEstimation(aQuery, nResults);
            }

            Arrays.sort(hits);

            return new FullTextSearchResult(hits, estimation);
        }
    }

}
