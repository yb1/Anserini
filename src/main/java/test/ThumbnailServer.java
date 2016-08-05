package test;

import io.anserini.index.IndexPlainText;
import io.anserini.index.ThumbnailIndexer;
import io.anserini.search.SearchArgs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.servlet.ServletHandler;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.kohsuke.args4j.ParserProperties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by youngbinkim on 7/24/16.
 */
public class ThumbnailServer {
    private static final Logger LOG = LogManager.getLogger(ThumbnailServer.class);
    public static void main(String[] args) throws Exception {
        org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server(8889);
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HelloServlet.class, "/thumbnail");//Set the servlet to run.
        server.setHandler(handler);
        server.start();
        server.join();
    }

    @SuppressWarnings("serial")
    public static class HelloServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            String index = "";
            String queryString = "";
            try {
                queryString = request.getParameter("query");
                index = request.getParameter("index");
                System.out.println(queryString);
            } catch (Exception e) {
                e.printStackTrace();
            }

            LOG.info("Reading index at " + index);
            Directory  dir = FSDirectory.open(Paths.get(index));

// Creating a RAMDirectory (memory) object, to be able to create index in memory.
            RAMDirectory rdir = new RAMDirectory();

            DirectoryReader ireader = DirectoryReader.open(dir);
            IndexSearcher isearcher = new IndexSearcher(ireader);

            TermQuery tq= new TermQuery(new Term("url", queryString));
// BooleanClauses Enum SHOULD says Use this operator for clauses that should appear in the matching documents.
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            BooleanQuery bq = builder.add(tq, BooleanClause.Occur.SHOULD).build();

            ScoreDoc[] hits = isearcher.search(bq, 1000).scoreDocs;
            System.out.println("hit length " + hits.length);
            // Iterate through the results:
            String res = "";
            if (hits.length > 0) {
                int docId = hits[0].doc;
                Document hitDoc = isearcher.doc(docId);
                res = hitDoc.get(ThumbnailIndexer.FIELD_BODY);
                //System.out.println(hitDoc.get(ThumbnailIndexer.FIELD_URL));
                System.out.println(res);
            }
            rdir.close();
            ireader.close();
            dir.close();

            List<String> list = new ArrayList<>();
            response.setHeader("Access-Control-Allow-Origin", "*");
            response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("{ \"res\":" + res + "}");
        }
    }
}
