package io.anserini.search;

import io.anserini.index.IndexPlainText;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.kohsuke.args4j.ParserProperties;

import java.nio.file.Paths;
import java.util.*;

/**
 * Created by youngbinkim on 6/2/16.
 */
public class SearchPlainText {
    private static final Logger LOG = LogManager.getLogger(SearchPlainText.class);

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws Exception {
        SearchArgs searchArgs = new SearchArgs();
        CmdLineParser parser = new CmdLineParser(searchArgs, ParserProperties.defaults().withUsageWidth(90));

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.err.println("Example: SearchPlainText" + parser.printExample(OptionHandlerFilter.REQUIRED));
            return;
        }

        LOG.info("Reading index at " + searchArgs.index);
        Directory dir;
        if (searchArgs.inmem) {
            LOG.info("Using MMapDirectory with preload");
            dir = new MMapDirectory(Paths.get(searchArgs.index));
            ((MMapDirectory) dir).setPreload(true);
        } else {
            LOG.info("Using default FSDirectory");
            dir = FSDirectory.open(Paths.get(searchArgs.index));
        }

// Creating a RAMDirectory (memory) object, to be able to create index in memory.
        RAMDirectory rdir = new RAMDirectory();

        String indexPath = searchArgs.index;

        DirectoryReader ireader = DirectoryReader.open(dir);
        IndexSearcher isearcher = new IndexSearcher(ireader);

        Analyzer analyzer = new StandardAnalyzer();

        // Parse a simple query that searches for "text":
        QueryParser queryParser = new QueryParser(IndexPlainText.FIELD_BODY, analyzer);
        Query query = queryParser.parse(searchArgs.topics);

        ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
        System.out.println("hit length " + hits.length);
        DefaultSimilarity similarity = new DefaultSimilarity();
        int docnum = ireader.numDocs();
        Set<String> seen = new HashSet();
        int counter = 0;
        int i = 0;
        // Iterate through the results:
        while (counter < 10) {

            int docId = hits[i++].doc;
            Document hitDoc = isearcher.doc(docId);

            String url = hitDoc.get("url");
            if (url.endsWith("index.html")) {
                url = url.substring(0, url.lastIndexOf("index.html"));
            }
            if (seen.contains(url)){
                continue;
            } else {
                seen.add(url);
                counter++;
            }
            /*
            Fields res = ireader.getTermVectors(docId);
            Iterator<String> it = res.iterator();
            while(it.hasNext()){
                System.out.println(it.next());
            }*/
            System.out.println(url);
            TermsEnum terms = ireader.getTermVector(docId, IndexPlainText.FIELD_BODY).iterator();
            System.out.println("fu " + terms.term().utf8ToString());
            BytesRef text = null;
            PriorityQueue<SearchTerm> minHeap = new PriorityQueue(new SearchTermComparator());

            // for (count = 0; count < 5 && ((text = terms.next()) != null); count++) {
            while ((text = terms.next()) != null) {
                //double idf = similarity.idf(terms.docFreq(), docnum);
                //System.out.println("idf ? " + idf);
                // System.out.println("total" + terms.doc);
                //System.out.println(terms.term().utf8ToString());
                String term = text.utf8ToString();
                if (isInterger(term)) {
                    continue;
                }


                // System.out.println("term: " + term);
                Long freq = terms.totalTermFreq();
                int df = ireader.docFreq(new Term(IndexPlainText.FIELD_BODY, term));
                float idf = similarity.idf(df, docnum);
                float tf = similarity.tf(freq);
                float score =  tf * idf;
                //System.out.println("tf-idf: " + tf * idf);

                if (minHeap.size() < 5) {
                    minHeap.add(new SearchTerm(score, term));
                }
                else {
                    Float cur = minHeap.peek().getScore();
                    if (cur < score) {
                        minHeap.poll();
                        minHeap.add(new SearchTerm(score, term));
                    }
                }

                        /*
                System.out.println("doc freq ? " + ireader.docFreq(new Term(IndexPlainText.FIELD_BODY, term)));
                System.out.println(term + " *** " + freq);
                System.out.println("df " + terms.docFreq());
                if (terms.docFreq() > 1) {
                    System.out.println("-----------------------------");
                }
                System.out.println("tf idf " + freq / terms.docFreq());
                */
            }

            while(!minHeap.isEmpty()) {
                SearchTerm cur = minHeap.poll();
                System.out.println("term " + cur.getTerm() + ":" + cur.getScore());
            }
            System.out.println("");
        }
        rdir.close();
        ireader.close();
        dir.close();
    }

    public static boolean isInterger(String str){
        return str.matches("^[+-]?\\d+$");
    }
}

