package io.anserini.search;

import io.anserini.index.IndexPlainText;
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.kohsuke.args4j.ParserProperties;

import javax.servlet.http.HttpServlet;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.PrintWriter;
import java.nio.file.Paths;

/**
 * Created by youngbinkim on 6/2/16.
 */
@Path("/search")
public class SearchPlainText extends HttpServlet {
    private static final Logger LOG = LogManager.getLogger(SearchPlainText.class);

    @GET
    @Path("/getEmployee")
    @Produces(MediaType.APPLICATION_JSON)
    public void search(String topics) throws Exception {
        Directory dir = FSDirectory.open(Paths.get("../lucene-index-enchanted/lucene-index-enchanted"));

        DirectoryReader ireader = DirectoryReader.open(dir);
        IndexSearcher isearcher = new IndexSearcher(ireader);

        Analyzer analyzer = new StandardAnalyzer();

        // Parse a simple query that searches for "text":
        QueryParser queryParser = new QueryParser(IndexPlainText.FIELD_BODY, analyzer);
        Query query = queryParser.parse(topics);

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
    }

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

        Directory dir = FSDirectory.open(Paths.get("../lucene-index-enchanted/lucene-index-enchanted"));

        DirectoryReader ireader = DirectoryReader.open(dir);
        IndexSearcher isearcher = new IndexSearcher(ireader);

        Analyzer analyzer = new StandardAnalyzer();

        // Parse a simple query that searches for "text":
        QueryParser queryParser = new QueryParser(IndexPlainText.FIELD_BODY, analyzer);
        Query query = queryParser.parse(searchArgs.topics);

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
    }
}
