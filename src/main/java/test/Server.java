package test;

import io.anserini.index.IndexPlainText;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.eclipse.jetty.servlet.ServletHandler;
import io.anserini.index.IndexPlainText;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.*;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.kohsuke.args4j.ParserProperties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * Created by youngbinkim on 6/19/16.
 */
public class Server {
    public static void main(String[] args) throws Exception {
        org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server(8888);
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(HelloServlet.class, "/search");//Set the servlet to run.
        server.setHandler(handler);
        server.start();
        server.join();
    }

    @SuppressWarnings("serial")
    public static class HelloServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            Configuration conf = new Configuration();
            conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

            String indexPath = "lucene-index-enchanted"; // request.getParameter("index");
            System.out.println("index :" + indexPath);
            HdfsDirectory hdfsDirectory = new HdfsDirectory(new Path(indexPath), conf);

            DirectoryReader ireader = DirectoryReader.open(hdfsDirectory);
            IndexSearcher isearcher = new IndexSearcher(ireader);

            Analyzer analyzer = new StandardAnalyzer();

            // Parse a simple query that searches for "text":
            QueryParser queryParser = new QueryParser(IndexPlainText.FIELD_BODY, analyzer);
            Query query = null;

            try {
                String queryString = request.getParameter("query");
                System.out.println(queryString);
                query = queryParser.parse(queryString);
            } catch (Exception e) {
                e.printStackTrace();
            }

            ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
            System.out.println("hit length " + hits.length);
            ArrayList<String> list = new ArrayList<>();
            // Iterate through the results:
            for (int i = 0; i < hits.length; i++) {
                Document hitDoc = isearcher.doc(hits[i].doc);
                list.add("\"https://web.archive.org/web/" + hitDoc.get("url") + "\"");
            }
            ireader.close();
            response.setHeader("Access-Control-Allow-Origin", "*");
            response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("{ \"res\":" + list.toString() + "}");
        }
    }
}

