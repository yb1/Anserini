package io.anserini.index;

import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jsoup.Jsoup;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Paths;

/**
 * Created by youngbinkim on 5/31/16.
 */
public class IndexPlainText {
    private static final Logger LOG = LogManager.getLogger(IndexPlainText.class);


    private IndexPlainText() {}

    private static final String HELP_OPTION = "h";
    private static final String INPUT_OPTION = "input";
    private static final String INDEX_OPTION = "index";
    public static final String FIELD_BODY = "body";
    public static final String FIELD_URL = "url";

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(new Option(HELP_OPTION, "show help"));

        options.addOption(OptionBuilder.withArgName("dir").hasArg()
                .withDescription("source collection directory").create(INPUT_OPTION));
        options.addOption(OptionBuilder.withArgName("dir").hasArg()
                .withDescription("index location").create(INDEX_OPTION));

        CommandLine cmdline = null;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            System.exit(-1);
        }

        if (cmdline.hasOption(HELP_OPTION) || !cmdline.hasOption(INPUT_OPTION)
                || !cmdline.hasOption(INDEX_OPTION)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(IndexTweets.class.getName(), options);
            System.exit(-1);
        }

        String inputPath = cmdline.getOptionValue(INPUT_OPTION);
        String indexPath = cmdline.getOptionValue(INDEX_OPTION);

        LOG.info("input: " + inputPath);
        LOG.info("index: " + indexPath);


        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));


        Directory dir = FSDirectory.open(Paths.get(indexPath));
        final IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());

        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        IndexWriter writer = new IndexWriter(dir, config);
        Path pt = new Path(inputPath);//"hdfs://cleon1.umiacs.umd.edu:8020/user/jrwiebe/EnchantedForest/part-08791");
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream reader = fs.open(pt);
        System.out.println(reader.available());
        //fs.close();
        //BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));
        //BufferedReader reader= new BufferedReader(new FileReader("hdfs://" + collectionPath));

        String row;

        while ((row=reader.readLine())!= null)
        {
            String tokens[] = row.split("\\s", 2);

            String url = tokens[0];
            String body = tokens[1];

            if (url == null || body == null) {
                continue;
            }

            Document document = new Document();

            document.add(new StringField(FIELD_URL, url, Field.Store.YES));
            document.add(new TextField(FIELD_BODY, body, Field.Store.NO));
            writer.addDocument(document);
        }
        writer.close();
        reader.close();
    }
}
