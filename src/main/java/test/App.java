package test;


import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.sun.net.httpserver.HttpServer;
import io.anserini.index.IndexPlainText;
import io.anserini.index.ThumbnailIndexer;
import io.anserini.search.SearchTerm;
import io.anserini.search.SearchTermComparator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by youngbinkim on 6/17/16.
 */
public class App {
    public static void main(String[] args) throws Exception {
        Server server = new Server(8888);
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HelloServlet.class, "/hello");//Set the servlet to run.
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
        final int MAX_ENTRIES = 70000;
        private ConcurrentMap<String, Integer> dfMap = new ConcurrentLinkedHashMap.Builder<String, Integer>()
                .maximumWeightedCapacity(MAX_ENTRIES)
                .build();

        public HelloServlet() throws IOException {
            dir = FSDirectory.open(Paths.get("../index-enchanted-forest"));

            ireader = DirectoryReader.open(dir);
            isearcher = new IndexSearcher(ireader);

            analyzer = new StandardAnalyzer();
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            // Parse a simple query that searches for "text":
            QueryParser queryParser = new QueryParser(IndexPlainText.FIELD_BODY, analyzer);
            Query query;
            String go;
            int startId;
            try {
                String queryString = request.getParameter("query");
                go = request.getParameter("terms");
                System.out.println(queryString);
                query = queryParser.parse(queryString);
                String startIdStr = request.getParameter("startId");
                System.out.println("start : " + startIdStr);
                startId = (startIdStr == null) ? 0 : Integer.parseInt(startIdStr);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
            System.out.println("hit length " + hits.length);
            ArrayList<SearchUrl> list = new ArrayList<>();
            // Iterate through the results:


            DefaultSimilarity similarity = new DefaultSimilarity();
            int docnum = ireader.numDocs();
            Set<String> seen = new HashSet();
            int counter = 0;
            int i = startId;
            // Iterate through the results:
            while (counter < 10 && i < hits.length) {

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

                        // System.out.println("term: " + term);
                        Long freq = terms.totalTermFreq();
                        int df = dfMap.getOrDefault(term, ireader.docFreq(new Term(IndexPlainText.FIELD_BODY, term)));
                        dfMap.putIfAbsent(term, df);
                        System.out.println(dfMap.size());

                        float idf = similarity.idf(df, docnum);
                        float tf = similarity.tf(freq);
                        float score = tf * idf;
                        //System.out.println("tf-idf: " + tf * idf);

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
                    System.out.println("term " + cur.getTerm() + ":" + cur.getScore());
                    termsList.add(0, "\"" + cur.getTerm() + "\"");
                }
                list.add(new SearchUrl(url, termsList));
                System.out.println("");
            }

            if (hits.length == i) {
                i = -1;
            }
            /*
            for (int i = 0; i < hits.length; i++) {
                Document hitDoc = isearcher.doc(hits[i].doc);
                list.add("\"https://web.archive.org/web/" + hitDoc.get("url") + "\"");
            }
            */
            //ireader.close();
            //dir.close();
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

        private int counter = 0;
        private int getCounter() {
            return counter;
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            counter++;
            System.out.println(counter);
            IndexSearcher isearcher = getIsearcher();

            String queryString = "";
            try {
                queryString = request.getParameter("query");
                System.out.println(queryString);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String res = "";
            //ArrayList<String> list = new ArrayList<>();
            // Iterate through the results:

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
                //System.out.println(hitDoc.get(ThumbnailIndexer.FIELD_URL));
                // System.out.println(hitDoc.get(ThumbnailIndexer.FIELD_BODY));
            }

            /*
            for (int i = 0; i < hits.length; i++) {
                Document hitDoc = isearcher.doc(hits[i].doc);
                list.add("\"https://web.archive.org/web/" + hitDoc.get("url") + "\"");
            }
            */
            response.setHeader("Access-Control-Allow-Origin", "*");
            response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("{ \"res\":\"" + res + "\"}");
        }
    }
    /*
    @SuppressWarnings("serial")
    public static class HelloServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("<h1>Hello SimpleServlet</h1>");
        }
    }*/
    /*
    public static void main(String[] args) throws Exception {
        Server server = new Server(9090);
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HelloServlet.class, "/hello");//Set the servlet to run.
        handler.addServletWithMapping(Resource.class, "/home");
        server.setHandler(handler);
        server.start();
        server.join();
    }

    @SuppressWarnings("serial")
    public static class HelloServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("<h1>Hello SimpleServlet</h1>");
        }
    }
    /*
    public static void main(String[] args) throws Exception {


        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");

        Server jettyServer = new Server(9999);
        jettyServer.setHandler(context);

        ServletHolder jerseyServlet = context.addServlet(ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);

        // Tells the Jersey Servlet which REST service/class to load.
        jerseyServlet.setInitParameter("jersey.config.server.provider.packages", "com.sangupta.keepwalking.rest");

        try {
            jettyServer.start();
            jettyServer.join();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jettyServer.destroy();
        }
        /*
        Server jettyServer = new Server(9191);
        try {
            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
            context.setContextPath("/");


            jettyServer.setHandler(context);

            ServletHolder jerseyServlet = context.addServlet(
                    org.glassfish.jersey.servlet.ServletContainer.class, "/*");
            jerseyServlet.setInitOrder(0);

            // Tells the Jersey Servlet which REST service/class to load.
            jerseyServlet.setInitParameter(
                    "jersey.config.server.provider.classnames",
                    Resource.class.getCanonicalName());
            jettyServer.start();
            jettyServer.join();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            jettyServer.stop();
            jettyServer.destroy();
        }
    }*/
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
