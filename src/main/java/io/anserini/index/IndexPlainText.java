package io.anserini.index;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RAMDirectory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

    public static void writeIndex(FileSystem fs, Path pt, IndexWriter writer) {
        String url;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));

            String row;

            while ((row=reader.readLine())!= null)
            {
                String tokens[] = row.split("\\s", 2);

                if (tokens.length < 2) {
                    LOG.error("Cannot tokenize for url {} from path {} ", tokens[0], pt.getName());
                    continue;
                }

                url = tokens[0];
                String body = tokens[1];

                if (url == null || body == null) {
                    continue;
                }

                Document document = new Document();

                document.add(new StringField(FIELD_URL, url, Field.Store.YES));
                document.add(new TextField(FIELD_BODY, body, Field.Store.NO));
                writer.addDocument(document);
            }
            reader.close();
        } catch (Exception e) {
            LOG.error("CANNOT write index for url {} from path {} ", pt.getName(), e);
        }
    }

    public static void iterateFiles(FileSystem fs, IndexWriter writer, FileStatus[] status) {
        for (int i = 0; i < status.length; i++) {
            FileStatus file = status[i];
            Path pt = file.getPath();

            if (file.isDirectory()) {
                try {
                    iterateFiles(fs, writer, fs.listStatus(pt));
                } catch (Exception e) {
                    LOG.error("CANNOT access subdirectory for a directory {} ", pt.getName(), e);
                }

            } else {
                writeIndex(fs, pt, writer);
            }
        }
    }

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


        RAMDirectory rdir = new RAMDirectory();
        //Directory dir = FSDirectory.open(Paths.get(indexPath));
        final IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());

        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        IndexWriter writer = new IndexWriter(rdir, config);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(inputPath));
        iterateFiles(fs, writer, status);

        writer.close();

        // Getting files present in memory into an array.
        String fileList[]=rdir.listAll();

// Reading index files from memory and storing them to HDFS.
        for (int i = 0; i < fileList.length; i++)
        {
            IndexInput indxfile = rdir.openInput(fileList[i].trim(), null);
            long len = indxfile.length();
            int len1 = (int) len;

            // Reading data from file into a byte array.
            byte[] bytarr = new byte[len1];
            indxfile.readBytes(bytarr, 0, len1);

// Creating file in HDFS directory with name same as that of
//index file
            Path src = new Path(fs.getWorkingDirectory() + "/" + indexPath + "/" + fileList[i].trim());
            fs.createNewFile(src);

            // Writing data from byte array to the file in HDFS
            FSDataOutputStream ofs = fs.create(new Path(fs.getWorkingDirectory() + "/" + indexPath + "/" + fileList[i].trim()),true);
            ofs.write(bytarr);
            ofs.close();
        }
        fs.closeAll();
    }
}