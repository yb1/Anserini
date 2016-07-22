package io.anserini.search;

import io.anserini.index.IndexPlainText;
import io.anserini.index.ThumbnailIndexer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.kohsuke.args4j.ParserProperties;

import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Created by youngbinkim on 7/21/16.
 */
public class SearchThumbnail  {
    private static final Logger LOG = LogManager.getLogger(SearchThumbnail.class);

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
        QueryParser queryParser = new QueryParser(ThumbnailIndexer.FIELD_URL, analyzer);
        Query query = queryParser.parse(URLEncoder.encode(searchArgs.topics, "UTF-8"));

        TermQuery tq= new TermQuery(new Term("url", searchArgs.topics));
// BooleanClauses Enum SHOULD says Use this operator for clauses that should appear in the matching documents.
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        BooleanQuery bq = builder.add(tq, BooleanClause.Occur.SHOULD).build();

        ScoreDoc[] hits = isearcher.search(bq, 1000).scoreDocs;
        System.out.println("hit length " + hits.length);
        // Iterate through the results:
        for (int i = 0; i < hits.length; i++) {
            int docId = hits[i].doc;
            Document hitDoc = isearcher.doc(docId);

            System.out.println(hitDoc.get(ThumbnailIndexer.FIELD_URL));
            System.out.println(hitDoc.get(ThumbnailIndexer.FIELD_BODY));
        }
        rdir.close();
        ireader.close();
        dir.close();
    }
}
