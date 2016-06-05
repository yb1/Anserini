package io.anserini.search;

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
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.kohsuke.args4j.ParserProperties;

import java.nio.file.Paths;

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

        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        FileSystem dfs = FileSystem.get(conf);

// Creating a RAMDirectory (memory) object, to be able to create index in memory.
        RAMDirectory rdir = new RAMDirectory();

        String indexPath = searchArgs.index;

        FileStatus[] status = dfs.listStatus(new Path(indexPath));

        FSDataInputStream filereader = null;
        for (int i = 0; i < status.length; i++)
        {
// Reading data from index files on HDFS directory into filereader object.
            String fileName = status[i].getPath().getName();
            filereader = dfs.open(new Path(dfs.getWorkingDirectory() + "/" + indexPath + "/" + fileName));

            int size = filereader.available();

            // Reading data from file into a byte array.
            byte[] bytarr = new byte[size];
            filereader.read(bytarr, 0, size);

// Creating file in RAM directory with names same as that of
//index files present in HDFS directory.
            IndexOutput indxout = rdir.createOutput(fileName, null);

            // Writing data from byte array to the file in RAM directory
            indxout.writeBytes(bytarr,bytarr.length);
            indxout.close();
        }
        filereader.close();

        DirectoryReader ireader = DirectoryReader.open(rdir);
        IndexSearcher isearcher = new IndexSearcher(ireader);

        Analyzer analyzer = new StandardAnalyzer();

        // Parse a simple query that searches for "text":
        QueryParser queryParser = new QueryParser(IndexPlainText.FIELD_BODY, analyzer);
        Query query = queryParser.parse(searchArgs.topics);

        ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
        System.out.println("hit length " + hits.length);
        // Iterate through the results:
        for (int i = 0; i < hits.length; i++) {
            Document hitDoc = isearcher.doc(hits[i].doc);
            System.out.println(hitDoc.get("url"));
        }
        rdir.close();
        ireader.close();
        dir.close();
    }
}
