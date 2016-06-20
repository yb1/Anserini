package io.anserini.search;

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
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import rest.Example;


import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Paths;

/**
 * Created by youngbinkim on 6/17/16.
 */
public class SearchServer {
    public static void main(String[] args) throws Exception {
        URI baseUri = UriBuilder.fromUri("http://localhost/").port(9998).build();
        ResourceConfig config = new ResourceConfig(Example.class);
        Server server = JettyHttpContainerFactory.createServer(baseUri, config);
        server.start();
        server.join();
    }

    @SuppressWarnings("serial")
    public static class HelloServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            Directory dir = FSDirectory.open(Paths.get("../lucene-index-enchanted/lucene-index-enchanted"));

            DirectoryReader ireader = DirectoryReader.open(dir);
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
            PrintWriter out = new PrintWriter("../output.txt");
            // Iterate through the results:
            for (int i = 0; i < hits.length; i++) {
                Document hitDoc = isearcher.doc(hits[i].doc);
                out.println("https://web.archive.org/web/" + hitDoc.get("url"));
            }
            out.close();
            ireader.close();
            dir.close();
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("<h1>Hello SimpleServlet</h1>");
        }
    }
}
