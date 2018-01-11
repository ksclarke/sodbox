
package info.freelibrary.sodbox.impl;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import info.freelibrary.sodbox.Assert;
import info.freelibrary.sodbox.GenericIndex;
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

    static class Document extends Persistent {

        Object obj;

        Link occurrences;

        Document() {
        }

        Document(final Storage storage, final Object obj) {
            super(storage);
            this.obj = obj;
            occurrences = storage.createLink();
        }
    }

    static class DocumentOccurrences extends Persistent {

        InverseList list;

        int nWordsInDocument;

        byte[] occurrences;

        final int[] getOccurrences() {
            final Compressor compressor = new Compressor(occurrences);
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

        final void setOccurrences(final int[] occ) {
            int i = 0;
            int prevOcc = -1;
            final int len = occ.length;
            final Compressor compressor = new Compressor(new byte[len * 4 + COMPRESSION_OVERHEAD]);
            compressor.encodeStart();
            compressor.encode(len);
            do {
                final int kind = occ[i] >>> OCC_KIND_OFFSET;
                int j = i;
                while (++j < len && occ[j] >>> OCC_KIND_OFFSET == kind) {
                    ;
                }
                compressor.encode(j - i);
                compressor.encode(kind + 1);
                do {
                    final int currOcc = occ[i++] & OCC_POSITION_MASK;
                    compressor.encode(currOcc - prevOcc);
                    prevOcc = currOcc;
                } while (i != j);
            } while (i != len);
            occurrences = compressor.encodeStop();
        }
    }

    static class ExpressionWeight implements Comparable {

        int weight;

        FullTextQuery expr;

        @Override
        public int compareTo(final Object o) {
            return weight - ((ExpressionWeight) o).weight;
        }
    }

    protected class FullTextSearchEngine extends FullTextQueryVisitor {

        static final int STRICT_MATCH_BONUS = 8;

        static final double DENSITY_MAGIC = 2;

        KeywordList[] kwds;

        ArrayList kwdList;

        int[] occurrences;

        int nOccurrences;

        float[] occurrenceKindWeight;

        void buildOccurrenceKindWeightTable() {
            occurrenceKindWeight = new float[256];
            final float[] weights = helper.getOccurrenceKindWeights();
            occurrenceKindWeight[0] = 1.0f;
            for (int i = 1; i < 256; i++) {
                float weight = 0;
                for (int j = 0; j < weights.length; j++) {
                    if ((i & 1 << j) != 0) {
                        weight += weights[j];
                    }
                    occurrenceKindWeight[i] = weight;
                }
            }
        }

        int calculateEstimation(final FullTextQuery query, final int nResults) {
            switch (query.op) {
                case FullTextQuery.AND:
                case FullTextQuery.NEAR: {
                    final int left = calculateEstimation(((FullTextQueryBinaryOp) query).left, nResults);
                    final int right = calculateEstimation(((FullTextQueryBinaryOp) query).right, nResults);
                    return left < right ? left : right;
                }
                case FullTextQuery.OR: {
                    final int left = calculateEstimation(((FullTextQueryBinaryOp) query).left, nResults);
                    final int right = calculateEstimation(((FullTextQueryBinaryOp) query).right, nResults);
                    return left > right ? left : right;
                }
                case FullTextQuery.MATCH:
                case FullTextQuery.STRICT_MATCH: {
                    final KeywordList kwd = kwds[((FullTextQueryMatchOp) query).wno];
                    if (kwd.currDoc == 0) {
                        return 0;
                    } else {
                        final int curr = kwd.currDoc;
                        final int first = kwd.list.first();
                        final int last = kwd.list.last();
                        int estimation = nResults * (last - first + 1) / (curr - first + 1);
                        if (estimation > kwd.list.size()) {
                            estimation = kwd.list.size();
                        }
                        return estimation;
                    }
                }
                case FullTextQuery.NOT:
                    return documents.size();

            }
            return 0;
        }

        final double calculateKwdRank(final InverseList list, final DocumentOccurrences d, final int[] occ) {
            final int frequency = occ.length;
            final int totalNumberOfDocuments = documents.size();
            final int nRelevantDocuments = list.size();
            final int totalNumberOfWords = inverseIndex.size();
            final double idf = Math.log((double) totalNumberOfDocuments / nRelevantDocuments);
            final double averageWords = (double) totalNumberOfWords / totalNumberOfDocuments;
            final double density = frequency * Math.log(1 + DENSITY_MAGIC * averageWords / d.nWordsInDocument);
            final double wordWeight = density * idf;
            double wordScore = 1;
            for (int i = 0; i < frequency; i++) {
                wordScore += wordWeight * occurrenceKindWeight[occ[i] >>> OCC_KIND_OFFSET];
            }
            return Math.log(wordScore);
        }

        double calculateNearness() {
            final KeywordList[] kwds = this.kwds;
            final int nKwds = kwds.length;
            if (nKwds < 2) {
                return 0;
            }
            for (int i = 0; i < nKwds; i++) {
                if (kwds[i].occ == null) {
                    final int j = kwds[i].sameAs;
                    if (j >= 0 && kwds[j].occ != null) {
                        kwds[i].occ = kwds[j].occ;
                    } else {
                        return 0;
                    }
                }
                kwds[i].occPos = 0;
            }
            double maxNearness = 0;
            final int swapPenalty = helper.getWordSwapPenalty();
            while (true) {
                int minPos = Integer.MAX_VALUE;
                double nearness = 0;
                KeywordList first = null;
                KeywordList prev = null;
                for (int i = 0; i < nKwds; i++) {
                    final KeywordList curr = kwds[i];
                    if (curr.occPos < curr.occ.length) {
                        if (prev != null) {
                            int offset = curr.occ[curr.occPos] - prev.occ[prev.occPos];
                            if (offset < 0) {
                                offset = (-offset - curr.kwdLen) * swapPenalty;
                            } else {
                                offset -= prev.kwdLen;
                            }
                            if (offset <= 2) {
                                offset = 1;
                            }
                            nearness += 1 / Math.sqrt(offset);
                        }
                        if (curr.occ[curr.occPos] < minPos) {
                            minPos = curr.occ[curr.occPos];
                            first = curr;
                        }
                        prev = curr;
                    }
                }
                if (first == null) {
                    break;
                }
                first.occPos += 1;

                if (nearness > maxNearness) {
                    maxNearness = nearness;
                }
            }
            return maxNearness;
        }

        int calculateWeight(final FullTextQuery query) {
            switch (query.op) {
                case FullTextQuery.AND: {
                    return calculateWeight(((FullTextQueryBinaryOp) query).left);
                }
                case FullTextQuery.NEAR: {
                    int shift = STRICT_MATCH_BONUS;
                    for (FullTextQuery q = ((FullTextQueryBinaryOp) query).right; q.op == FullTextQuery.NEAR; q =
                            ((FullTextQueryBinaryOp) q).right) {
                        shift += STRICT_MATCH_BONUS;
                    }
                    return shift >= 32 ? 0 : calculateWeight(((FullTextQueryBinaryOp) query).left) >> shift;
                }
                case FullTextQuery.OR: {
                    final int leftWeight = calculateWeight(((FullTextQueryBinaryOp) query).left);
                    final int rightWeight = calculateWeight(((FullTextQueryBinaryOp) query).right);
                    return leftWeight > rightWeight ? leftWeight : rightWeight;
                }
                case FullTextQuery.MATCH:
                case FullTextQuery.STRICT_MATCH: {
                    final InverseList list = kwds[((FullTextQueryMatchOp) query).wno].list;
                    return list == null ? 0 : list.size();
                }
                default:
                    return Integer.MAX_VALUE;
            }
        }

        double evaluate(final int doc, final FullTextQuery query) {
            double left, right;
            switch (query.op) {
                case FullTextQuery.NEAR:
                case FullTextQuery.AND:
                    left = evaluate(doc, ((FullTextQueryBinaryOp) query).left);
                    right = evaluate(doc, ((FullTextQueryBinaryOp) query).right);
                    nOccurrences = 0;
                    return left < 0 || right < 0 ? -1 : left + right;
                case FullTextQuery.OR:
                    left = evaluate(doc, ((FullTextQueryBinaryOp) query).left);
                    right = evaluate(doc, ((FullTextQueryBinaryOp) query).right);
                    return left > right ? left : right;
                case FullTextQuery.MATCH:
                case FullTextQuery.STRICT_MATCH: {
                    final KeywordList kwd = kwds[((FullTextQueryMatchOp) query).wno];
                    if (kwd.currDoc != doc) {
                        return -1;
                    }
                    final DocumentOccurrences d = (DocumentOccurrences) kwd.currEntry.getValue();
                    final int[] occ = d.getOccurrences();
                    kwd.occ = occ;
                    final int frequency = occ.length;
                    if (query.op == FullTextQuery.STRICT_MATCH) {
                        if (nOccurrences == 0) {
                            nOccurrences = frequency;
                            if (occurrences == null || occurrences.length < frequency) {
                                occurrences = new int[frequency];
                            }
                            for (int i = 0; i < frequency; i++) {
                                occurrences[i] = occ[i] & OCC_POSITION_MASK;
                            }
                        } else {
                            int nPairs = 0;
                            final int[] dst = occurrences;
                            int occ1 = dst[0];
                            int occ2 = occ[0] & OCC_POSITION_MASK;
                            int i = 0, j = 0;
                            final int offs = kwd.kwdOffset;
                            while (true) {
                                if (occ1 + offs <= occ2) {
                                    if (occ1 + offs + 1 >= occ2) {
                                        dst[nPairs++] = occ2;
                                    }
                                    if (++j == nOccurrences) {
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
                            nOccurrences = nPairs;
                            if (nPairs == 0) {
                                return -1;
                            }
                        }
                    }
                    return calculateKwdRank(kwd.list, d, occ);
                }
                case FullTextQuery.NOT: {
                    final double rank = evaluate(doc, ((FullTextQueryUnaryOp) query).opd);
                    return rank >= 0 ? -1 : 0;
                }
                default:
                    return -1;
            }
        }

        int intersect(int doc, final FullTextQuery query) {
            int left, right;

            switch (query.op) {
                case FullTextQuery.AND:
                case FullTextQuery.NEAR:
                    do {
                        left = intersect(doc, ((FullTextQueryBinaryOp) query).left);
                        if (left == Integer.MAX_VALUE) {
                            return left;
                        }
                        doc = intersect(left, ((FullTextQueryBinaryOp) query).right);
                    } while (left != doc && doc != Integer.MAX_VALUE);
                    return doc;
                case FullTextQuery.OR:
                    left = intersect(doc, ((FullTextQueryBinaryOp) query).left);
                    right = intersect(doc, ((FullTextQueryBinaryOp) query).right);
                    return left < right ? left : right;
                case FullTextQuery.MATCH:
                case FullTextQuery.STRICT_MATCH: {
                    final KeywordList kwd = kwds[((FullTextQueryMatchOp) query).wno];
                    if (kwd.currDoc >= doc) {
                        return kwd.currDoc;
                    }
                    Iterator iterator = kwd.iterator;
                    if (iterator != null) {
                        if (iterator.hasNext()) {
                            final Map.Entry entry = (Map.Entry) iterator.next();
                            final int nextDoc = ((Integer) entry.getKey()).intValue();
                            if (nextDoc >= doc) {
                                kwd.currEntry = entry;
                                kwd.currDoc = nextDoc;
                                return nextDoc;
                            }
                        } else {
                            kwd.currEntry = null;
                            kwd.currDoc = 0;
                            return Integer.MAX_VALUE;
                        }
                    }
                    if (kwd.list != null) {
                        kwd.iterator = iterator = kwd.list.iterator(doc);
                        if (iterator.hasNext()) {
                            final Map.Entry entry = (Map.Entry) iterator.next();
                            doc = ((Integer) entry.getKey()).intValue();
                            kwd.currEntry = entry;
                            kwd.currDoc = doc;
                            return doc;
                        }
                    }
                    kwd.currEntry = null;
                    kwd.currDoc = 0;
                    return Integer.MAX_VALUE;
                }
                case FullTextQuery.NOT: {
                    final int nextDoc = intersect(doc, ((FullTextQueryUnaryOp) query).opd);
                    if (nextDoc == doc) {
                        doc += 1;
                    }
                    return doc;
                }
                default:
                    return doc;
            }
        }

        FullTextQuery optimize(final FullTextQuery query) {
            switch (query.op) {
                case FullTextQuery.AND:
                case FullTextQuery.NEAR: {
                    final int op = query.op;
                    int nConjuncts = 1;
                    FullTextQuery q = query;
                    while ((q = ((FullTextQueryBinaryOp) q).right).op == op) {
                        nConjuncts += 1;
                    }
                    final ExpressionWeight[] conjuncts = new ExpressionWeight[nConjuncts + 1];
                    q = query;
                    for (int i = 0; i < nConjuncts; i++) {
                        final FullTextQueryBinaryOp and = (FullTextQueryBinaryOp) q;
                        conjuncts[i] = new ExpressionWeight();
                        conjuncts[i].expr = optimize(and.left);
                        conjuncts[i].weight = calculateWeight(conjuncts[i].expr);
                        q = and.right;
                    }
                    conjuncts[nConjuncts] = new ExpressionWeight();
                    conjuncts[nConjuncts].expr = optimize(q);
                    conjuncts[nConjuncts].weight = calculateWeight(conjuncts[nConjuncts].expr);
                    Arrays.sort(conjuncts);
                    if (op == FullTextQuery.AND) { // eliminate duplicates
                        int n = 0, j = -1;
                        InverseList list = null;
                        for (int i = 0; i <= nConjuncts; i++) {
                            q = conjuncts[i].expr;
                            if (q instanceof FullTextQueryMatchOp) {
                                final FullTextQueryMatchOp match = (FullTextQueryMatchOp) q;
                                if (n == 0 || kwds[match.wno].list != list) {
                                    j = match.wno;
                                    list = kwds[j].list;
                                    conjuncts[n++] = conjuncts[i];
                                } else {
                                    kwds[match.wno].sameAs = j;
                                }
                            } else {
                                conjuncts[n++] = conjuncts[i];
                            }
                        }
                        nConjuncts = n - 1;
                    } else { // calculate distance between keywords
                        int kwdPos = 0;
                        for (int i = 0; i <= nConjuncts; i++) {
                            q = conjuncts[i].expr;
                            if (q instanceof FullTextQueryMatchOp) {
                                final FullTextQueryMatchOp match = (FullTextQueryMatchOp) q;
                                kwds[match.wno].kwdOffset = match.pos - kwdPos;
                                kwdPos = match.pos;
                            }
                        }
                    }
                    if (nConjuncts == 0) {
                        return conjuncts[0].expr;
                    } else {
                        q = query;
                        int i = 0;
                        while (true) {
                            final FullTextQueryBinaryOp and = (FullTextQueryBinaryOp) q;
                            and.left = conjuncts[i].expr;
                            if (++i < nConjuncts) {
                                q = and.right;
                            } else {
                                and.right = conjuncts[i].expr;
                                break;
                            }
                        }
                    }
                    break;
                }
                case FullTextQuery.OR: {
                    final FullTextQueryBinaryOp or = (FullTextQueryBinaryOp) query;
                    or.left = optimize(or.left);
                    or.right = optimize(or.right);
                    break;
                }
                case FullTextQuery.NOT: {
                    final FullTextQueryUnaryOp not = (FullTextQueryUnaryOp) query;
                    not.opd = optimize(not.opd);
                }
                    break;
                default:;
            }
            return query;
        }

        void reset() {
            nOccurrences = 0;
            for (int i = 0; i < kwds.length; i++) {
                kwds[i].occ = null;
            }
        }

        FullTextSearchResult search(FullTextQuery query, final int maxResults, final int timeLimit) {
            if (query == null || !query.isConstrained()) {
                return null;
            }
            final long start = System.currentTimeMillis();
            buildOccurrenceKindWeightTable();
            kwdList = new ArrayList();
            query.visit(this);
            kwds = (KeywordList[]) kwdList.toArray(new KeywordList[kwdList.size()]);
            query = optimize(query);
            // System.out.println(query.toString());
            FullTextSearchHit[] hits = new FullTextSearchHit[maxResults];
            int currDoc = 1;
            int nResults = 0;
            final float nearnessWeight = helper.getNearnessWeight();
            boolean noMoreMatches = false;
            while (nResults < maxResults && System.currentTimeMillis() < start + timeLimit) {
                currDoc = intersect(currDoc, query);
                if (currDoc == Integer.MAX_VALUE) {
                    noMoreMatches = true;
                    break;
                }
                reset();
                final double kwdRank = evaluate(currDoc, query);
                if (kwdRank >= 0) {
                    calculateNearness();
                    final float rank = (float) (kwdRank * (1 + calculateNearness() * nearnessWeight));
                    // System.out.println("kwdRank=" + kwdRank + ", nearness=" + nearness + ", total rank=" + rank);
                    hits[nResults++] = new FullTextSearchHit(getStorage(), currDoc, rank);
                }
                currDoc += 1;
            }
            int estimation;
            if (nResults < maxResults) {
                final FullTextSearchHit[] realHits = new FullTextSearchHit[nResults];
                System.arraycopy(hits, 0, realHits, 0, nResults);
                hits = realHits;
            }
            if (noMoreMatches) {
                estimation = nResults;
            } else if (query instanceof FullTextQueryMatchOp) {
                estimation = kwds[0].list.size();
            } else {
                estimation = calculateEstimation(query, nResults);
            }
            Arrays.sort(hits);
            return new FullTextSearchResult(hits, estimation);
        }

        FullTextSearchResult searchPrefix(final String prefix, final int maxResults, final int timeLimit,
                final boolean sort) {
            final Iterator lists = inverseIndex.prefixIterator(prefix);
            FullTextSearchHit[] hits = new FullTextSearchHit[maxResults];
            int nResults = 0;
            int estimation = 0;
            final long start = System.currentTimeMillis();

            JoinLists:
            while (lists.hasNext()) {
                final InverseList list = (InverseList) lists.next();
                final Iterator occurrences = list.iterator(0);
                estimation += list.size();
                while (occurrences.hasNext()) {
                    final Map.Entry entry = (Map.Entry) occurrences.next();
                    final int doc = ((Integer) entry.getKey()).intValue();
                    float rank = 1.0f;
                    if (sort) {
                        final DocumentOccurrences d = (DocumentOccurrences) entry.getValue();
                        rank = (float) calculateKwdRank(list, d, d.getOccurrences());
                    }
                    hits[nResults] = new FullTextSearchHit(getStorage(), doc, rank);
                    if (++nResults >= maxResults || System.currentTimeMillis() >= start + timeLimit) {
                        break JoinLists;
                    }
                }
            }
            if (nResults < maxResults) {
                final FullTextSearchHit[] realHits = new FullTextSearchHit[nResults];
                System.arraycopy(hits, 0, realHits, 0, nResults);
                hits = realHits;
            }
            if (sort) {
                Arrays.sort(hits);
            }
            return new FullTextSearchResult(hits, estimation);
        }

        @Override
        public void visit(final FullTextQueryMatchOp q) {
            q.wno = kwdList.size();
            final KeywordList kwd = new KeywordList(q.word);
            kwd.list = (InverseList) inverseIndex.get(q.word);
            kwdList.add(kwd);
        }
    }

    static class InverseList extends Btree {

        class InverstListIterator implements Iterator {

            int i;

            InverstListIterator(final int pos) {
                i = pos;
            }

            @Override
            public boolean hasNext() {
                return i < oids.length;
            }

            @Override
            public Object next() {
                final int j = i++;
                return new Map.Entry() {

                    @Override
                    public Object getKey() {
                        return new Integer(oids[j]);
                    }

                    @Override
                    public Object getValue() {
                        return docs.get(j);
                    }

                    @Override
                    public Object setValue(final Object value) {
                        return null;
                    }
                };
            }

            @Override
            public void remove() {
            }
        }

        static final int BTREE_THRESHOLD = 500;

        int[] oids;

        Link docs;

        InverseList() {
        }

        InverseList(final Storage db, final int oid, final DocumentOccurrences doc) {
            super(int.class, true);
            docs = db.createLink(1);
            docs.add(doc);
            oids = new int[1];
            oids[0] = oid;
            assignOid(db, 0, false);
        }

        void add(final int oid, final DocumentOccurrences doc) {
            int[] os = oids;
            if (os == null || os.length >= BTREE_THRESHOLD) {
                if (os != null) {
                    for (int i = 0; i < os.length; i++) {
                        super.put(new Key(os[i]), docs.get(i));
                    }
                    oids = null;
                    docs = null;
                }
                super.put(new Key(oid), doc);
            } else {
                int l = 0;
                final int n = os.length;
                int r = n;
                while (l < r) {
                    final int m = l + r >>> 1;
                    if (os[m] < oid) {
                        l = m + 1;
                    } else {
                        r = m;
                    }
                }
                os = new int[n + 1];
                System.arraycopy(oids, 0, os, 0, r);
                os[r] = oid;
                System.arraycopy(oids, r, os, r + 1, n - r);
                docs.insert(r, doc);
                oids = os;
                modify();
            }
        }

        int first() {
            if (oids != null) {
                return oids[0];
            }
            final Map.Entry entry = (Map.Entry) entryIterator(null, null, GenericIndex.ASCENT_ORDER).next();
            return ((Integer) entry.getKey()).intValue();
        }

        Iterator iterator(final int oid) {
            final int[] os = oids;
            if (os != null) {
                int l = 0, r = os.length;
                while (l < r) {
                    final int m = l + r >>> 1;
                    if (os[m] < oid) {
                        l = m + 1;
                    } else {
                        r = m;
                    }
                }
                return new InverstListIterator(r);
            } else {
                return entryIterator(new Key(oid), null, GenericIndex.ASCENT_ORDER);
            }
        }

        int last() {
            if (oids != null) {
                return oids[oids.length - 1];
            }
            final Map.Entry entry = (Map.Entry) entryIterator(null, null, GenericIndex.DESCENT_ORDER).next();
            return ((Integer) entry.getKey()).intValue();
        }

        void remove(final int oid) {
            final int[] os = oids;
            if (os != null) {
                int l = 0;
                final int n = os.length;
                int r = n;
                while (l < r) {
                    final int m = l + r >>> 1;
                    if (os[m] < oid) {
                        l = m + 1;
                    } else {
                        r = m;
                    }
                }
                Assert.that(r < n && os[r] == oid);
                docs.remove(r);
                oids = new int[n - 1];
                System.arraycopy(os, 0, oids, 0, r);
                System.arraycopy(os, r + 1, oids, r, n - r - 1);
                modify();
            } else {
                super.remove(new Key(oid));
            }
        }

        @Override
        public int size() {
            return oids != null ? oids.length : super.size();
        }
    }

    static class KeywordImpl implements Keyword {

        Map.Entry entry;

        KeywordImpl(final Map.Entry entry) {
            this.entry = entry;
        }

        @Override
        public String getNormalForm() {
            return (String) entry.getKey();
        }

        @Override
        public long getNumberOfOccurrences() {
            return ((InverseList) entry.getValue()).size();
        }
    }

    static class KeywordIterator implements Iterator<Keyword> {

        Iterator iterator;

        KeywordIterator(final Iterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Keyword next() {
            return new KeywordImpl((Map.Entry) iterator.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    protected static class KeywordList {

        InverseList list;

        int[] occ;

        String word;

        int sameAs;

        int kwdLen;

        int kwdOffset;

        int occPos;

        int currDoc;

        Map.Entry currEntry;

        Iterator iterator;

        KeywordList(final String word) {
            this.word = word;
            kwdLen = word.length();
            sameAs = -1;
        }
    }

    static final int OCC_KIND_OFFSET = 24;

    static final int OCC_POSITION_MASK = (1 << OCC_KIND_OFFSET) - 1;

    static final int COMPRESSION_OVERHEAD = 8;

    protected Index inverseIndex;

    protected Index documents;

    protected FullTextSearchHelper helper;

    public FullTextIndexImpl(final Storage storage, final FullTextSearchHelper helper) {
        super(storage);
        this.helper = helper;
        inverseIndex = storage.createIndex(String.class, true);
        documents = storage.createIndex(Object.class, true);
    }

    @Override
    public void add(final FullTextSearchable obj) {
        add(obj, obj.getText(), obj.getLanguage());
    }

    @Override
    public void add(final Object obj, final Reader text, final String language) {
        Occurrence[] occurrences;
        try {
            occurrences = helper.parseText(text);
        } catch (final IOException x) {
            throw new StorageError(StorageError.FULL_TEXT_INDEX_ERROR, x);
        }
        delete(obj);
        if (occurrences.length > 0) {
            final Document doc = new Document(getStorage(), obj);
            documents.put(new Key(obj), doc);
            Arrays.sort(occurrences);
            String word = occurrences[0].word;
            int i = 0;
            for (int j = 1; j < occurrences.length; j++) {
                final Occurrence occ = occurrences[j];
                if (!occ.word.equals(word)) {
                    addReference(doc, word, occurrences, i, j, language);
                    word = occ.word;
                    i = j;
                }
            }
            addReference(doc, word, occurrences, i, occurrences.length, language);
        }
    }

    private final void addReference(final Document doc, final String word, final Occurrence[] occurrences,
            final int from, final int till) {
        final DocumentOccurrences d = new DocumentOccurrences();
        final int[] occ = new int[till - from];
        d.nWordsInDocument = occurrences.length;
        for (int i = from; i < till; i++) {
            occ[i - from] = occurrences[i].position | occurrences[i].kind << OCC_KIND_OFFSET;
        }
        d.setOccurrences(occ);
        final int oid = getStorage().getOid(doc.obj);
        InverseList list = (InverseList) inverseIndex.get(word);
        if (list == null) {
            list = new InverseList(getStorage(), oid, d);
            inverseIndex.put(word, list);
        } else {
            list.add(oid, d);
        }
        d.list = list;
        d.modify();
        doc.occurrences.add(d);
    }

    private final void addReference(final Document doc, final String word, final Occurrence[] occurrences,
            final int from, final int till, final String language) {
        final String[] normalForms = helper.getNormalForms(word, language);
        boolean isNormalForm = false;
        for (final String normalForm : normalForms) {
            if (word.equals(normalForm)) {
                isNormalForm = true;
            }
            addReference(doc, normalForm, occurrences, from, till);
        }
        if (!isNormalForm) {
            addReference(doc, word, occurrences, from, till);
        }
    }

    @Override
    public void clear() {
        inverseIndex.deallocateMembers();
        documents.deallocateMembers();
    }

    @Override
    public void deallocate() {
        clear();
        super.deallocate();
    }

    @Override
    public void delete(final Object obj) {
        final Key key = new Key(obj);
        final Document doc = (Document) documents.get(key);
        if (doc != null) {
            for (int i = 0, n = doc.occurrences.size(); i < n; i++) {
                final DocumentOccurrences d = (DocumentOccurrences) doc.occurrences.get(i);
                d.list.remove(getStorage().getOid(obj));
                d.deallocate();
            }
            documents.remove(key);
            doc.deallocate();
        }
    }

    @Override
    public FullTextSearchHelper getHelper() {
        return helper;
    }

    @Override
    public Iterator<Keyword> getKeywords(final String prefix) {
        return new KeywordIterator(inverseIndex.entryIterator(new Key(prefix), new Key(prefix + Character.MAX_VALUE,
                false), GenericIndex.ASCENT_ORDER));
    }

    @Override
    public int getNumberOfDocuments() {
        return documents.size();
    }

    @Override
    public int getNumberOfWords() {
        return inverseIndex.size();
    }

    @Override
    public FullTextSearchResult search(final FullTextQuery query, final int maxResults, final int timeLimit) {
        final FullTextSearchEngine engine = new FullTextSearchEngine();
        return engine.search(query, maxResults, timeLimit);
    }

    @Override
    public FullTextSearchResult search(final String query, final String language, final int maxResults,
            final int timeLimit) {
        return search(helper.parseQuery(query, language), maxResults, timeLimit);
    }

    @Override
    public FullTextSearchResult searchPrefix(final String prefix, final int maxResults, final int timeLimit,
            final boolean sort) {
        final FullTextSearchEngine engine = new FullTextSearchEngine();
        return engine.searchPrefix(prefix, maxResults, timeLimit, sort);
    }
}