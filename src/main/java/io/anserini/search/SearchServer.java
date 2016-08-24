package io.anserini.search;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.anserini.index.IndexPlainText;
import io.anserini.index.ThumbnailIndexer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * This server class provides
 * Query API +
 * Thumbnail API
 *
 * Used by nodejs Search server
 */
public class SearchServer {
    public static void main(String[] args) throws Exception {
        Server server = new Server(8888);
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HelloServlet.class, "/query");//Set the servlet to run.
        handler.addServletWithMapping(ThumbnailServlet.class, "/thumbnail");//Set the servlet to run.
        server.setHandler(handler);
        server.start();
        server.join();
    }

    @SuppressWarnings("serial")
    public static class HelloServlet extends HttpServlet {

        private Directory dir;
        private IndexSearcher isearcher;
        private DirectoryReader ireader;
        private Analyzer analyzer = new StandardAnalyzer();
        final private int docnum;
        final int MAX_ENTRIES = 70000;
        private ConcurrentMap<String, Integer> dfMap = new ConcurrentLinkedHashMap.Builder<String, Integer>()
                .maximumWeightedCapacity(MAX_ENTRIES)
                .build();
        final int MAX_RESULT_ARR_SIZE = 20;
        private ConcurrentMap<String, ScoreDoc[]> resMap = new ConcurrentLinkedHashMap.Builder<String, ScoreDoc[]>()
                .maximumWeightedCapacity(MAX_RESULT_ARR_SIZE)
                .build();

        public HelloServlet() throws IOException {
            dir = FSDirectory.open(Paths.get("../index-enchanted-forest"));

            ireader = DirectoryReader.open(dir);
            docnum = ireader.numDocs();
            isearcher = new IndexSearcher(ireader);

            analyzer = new StandardAnalyzer();
        }

        private ScoreDoc[] getQueryResults(String queryString) throws IOException, ParseException {
            System.out.println("NEW QUERY " + queryString);
            QueryParser queryParser = new QueryParser(IndexPlainText.FIELD_BODY, analyzer);
            Query query = queryParser.parse(queryString);
            ScoreDoc[] res = isearcher.search(query, 500).scoreDocs;
            if (res.length > 0) {
                resMap.put(queryString, res);
                System.out.println("INSERTING QUERY " + queryString);
                System.out.println(resMap.containsKey(queryString));
            }
            return res;
        }
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            // Parse a simple query that searches for "text":
            String queryString;
            String go;
            int startId;
            ScoreDoc[] hits;

            try {
                queryString = request.getParameter("query");
                go = request.getParameter("terms");
                System.out.println(queryString);

                String startIdStr = request.getParameter("startId");
                System.out.println("start : " + startIdStr);
                startId = (startIdStr == null) ? 0 : Integer.parseInt(startIdStr);
                hits = (resMap.containsKey(queryString)) ? resMap.get(queryString) : getQueryResults(queryString);
                System.out.println("ALWAYS TRUE ? = " + resMap.containsKey(queryString));
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            System.out.println("hit length " + hits.length);
            ArrayList<SearchUrl> list = new ArrayList<>();
            // Iterate through the results:

            DefaultSimilarity similarity = new DefaultSimilarity();
            Set<String> seen = new HashSet();
            int counter = 0;
            int i = startId;
            // Iterate through the results:
            while (counter < 10 && i < hits.length) {

                int docId = hits[i++].doc;
                Document hitDoc = isearcher.doc(docId);

                String url = hitDoc.get("url");
                String urlKey = url;
                if (url.endsWith("index.html") || url.endsWith("index.html/") ||
                        url.endsWith("index.htm") || url.endsWith("index.htm/")) {
                    urlKey = url.substring(0, url.lastIndexOf("index.htm"));
                }
                if (seen.contains(urlKey)){
                    continue;
                } else {
                    seen.add(urlKey);
                    counter++;
                }

                System.out.println(url);
                TermsEnum terms = ireader.getTermVector(docId, IndexPlainText.FIELD_BODY).iterator();
                BytesRef text = null;
                PriorityQueue<SearchTerm> minHeap = new PriorityQueue(new SearchTermComparator());

                if ("yes".equals(go)) {
                    while ((text = terms.next()) != null) {
                        String term = text.utf8ToString();
                        if (isInterger(term)) {
                            continue;
                        }

                        Long freq = terms.totalTermFreq();
                        // caching for optimization
                        int df = (dfMap.containsKey(term)) ? dfMap.get(term) : ireader.docFreq(new Term(IndexPlainText.FIELD_BODY, term));
                        dfMap.putIfAbsent(term, df);

                        float idf = similarity.idf(df, docnum);
                        float tf = similarity.tf(freq);
                        float score = tf * idf;

                        // to keep track of 5 top tf-idf terms.
                        if (minHeap.size() < 5) {
                            minHeap.add(new SearchTerm(score, term));
                        } else {
                            Float cur = minHeap.peek().getScore();
                            if (cur < score) {
                                minHeap.poll();
                                minHeap.add(new SearchTerm(score, term));
                            }
                        }
                    }
                }
                List<String> termsList = new ArrayList<>(5);

                while(!minHeap.isEmpty()) {
                    SearchTerm cur = minHeap.poll();
                    termsList.add(0, "\"" + cur.getTerm() + "\"");
                }
                list.add(new SearchUrl(url, termsList));
                System.out.println("");
            }

            if (hits.length == i) {
                i = -1;
            }

            response.setHeader("Access-Control-Allow-Origin", "*");
            response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("{ \"res\":" + list.toString() + ", \"nextDocID\": " + i  +
                    ", \"total\": " + hits.length + "}");
        }
    }

    @SuppressWarnings("serial")
    public static class ThumbnailServlet extends HttpServlet {
        private Directory dir;

        public IndexSearcher getIsearcher() {
            return isearcher;
        }

        private IndexSearcher isearcher;

        public ThumbnailServlet() throws IOException {
            this.dir = FSDirectory.open(Paths.get("../thumbnails_forestTmpTmp"));
            DirectoryReader ireader = DirectoryReader.open(dir);
            isearcher = new IndexSearcher(ireader);
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            IndexSearcher isearcher = getIsearcher();

            String queryString = "";
            try {
                queryString = request.getParameter("query");
                System.out.println(queryString);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String res = "";

            TermQuery tq= new TermQuery(new Term("url", queryString));
// BooleanClauses Enum SHOULD says Use this operator for clauses that should appear in the matching documents.
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            BooleanQuery bq = builder.add(tq, BooleanClause.Occur.SHOULD).build();

            ScoreDoc[] hits = isearcher.search(bq, 1).scoreDocs;
            System.out.println("hit length " + hits.length);
            // Iterate through the results:
            if (hits.length > 0) {
                int docId = hits[0].doc;
                Document hitDoc = isearcher.doc(docId);
                res =  hitDoc.get(ThumbnailIndexer.FIELD_BODY);
            }

            response.setHeader("Access-Control-Allow-Origin", "*");
            response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("{ \"res\":\"" + res + "\"}");
        }
    }

    public static boolean isInterger(String str){
        return str.matches("^[+-]?\\d+$");
    }
}

class SearchUrl {
    String url;
    List<String> terms;

    public SearchUrl(String url, List<String> terms) {
        this.url = url;
        this.terms = terms;
    }

    @Override
    public String toString() {
        return "{" +
                "\"url\":\"" + url + '\"' +
                ", \"terms\":" + terms +
                '}';
    }
}
