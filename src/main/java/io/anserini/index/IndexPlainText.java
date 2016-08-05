package io.anserini.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.ParserProperties;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    private final class IndexerThread extends Thread {
        final private Path inputWarcFile;
        final private FileSystem fs;
        final private IndexWriter writer;

        public IndexerThread(IndexWriter writer, Path inputWarcFile, FileSystem fs) throws IOException {
            this.writer = writer;
            this.inputWarcFile = inputWarcFile;
            this.fs = fs;
            setName(inputWarcFile.getName().toString());
        }

        @Override
        public void run() {
            String url;
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(inputWarcFile)));

                String row;

                while ((row=reader.readLine())!= null)
                {
                    String tokens[] = row.split("\\s", 2);

                    if (tokens.length < 2) {
                        LOG.error("Cannot tokenize for url {} from path {} ", tokens[0], inputWarcFile.getName());
                        continue;
                    }

                    url = tokens[0];
                    String body = tokens[1];

                    if (url == null || body == null) {
                        continue;
                    }

                    Document document = new Document();
                    System.out.println("Wrote " + url);
                    document.add(new StringField(FIELD_URL, url, Field.Store.YES));
                    document.add(new TermVectorsTextField(FIELD_BODY, body, Field.Store.YES));
                    // document.add(new TextField(FIELD_BODY, body, Field.Store.NO));
                    writer.addDocument(document);
                }
                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("CANNOT write index for url {} from path {} ", inputWarcFile.getName(), e);
            }
        }
    }

    public int indexWithThreads(int numThreads, Deque<Path> warcFiles, IndexWriter writer, FileSystem fs) throws IOException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        int numIndexed = 0;

        try {
            int i = 0;
            while (!warcFiles.isEmpty()){
                if (!warcFiles.isEmpty())
                    executor.execute(new IndexerThread(writer, warcFiles.removeFirst(), fs));
                else {
                    if (!executor.isShutdown()) {
                        Thread.sleep(30000);
                        executor.shutdown();
                    }
                    break;
                }
                i++;
            }

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.DAYS);

            writer.commit();
            numIndexed = writer.maxDoc();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("CANNOT index with threads.. ", e);
        } finally {
            writer.close();
        }

        return numIndexed;
    }

    public static Deque<Path> iterateFiles(FileSystem fs, FileStatus[] status) {
        final Deque<Path> fileStack = new ArrayDeque<>();

        for (int i = 0; i < status.length; i++) {
            FileStatus file = status[i];
            Path pt = file.getPath();

            if (file.isDirectory()) {
                try {
                    iterateFiles(fs, fs.listStatus(pt));
                } catch (Exception e) {
                    LOG.error("CANNOT access subdirectory for a directory {} ", pt.getName(), e);
                }

            } else {
                fileStack.add(pt);
                // writeIndex(fs, pt, writer);
            }
        }
        return fileStack;
    }

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws Exception {
        IndexArgs indexArgs = new IndexArgs();

        CmdLineParser parser = new CmdLineParser(indexArgs, ParserProperties.defaults().withUsageWidth(90));
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println("Error parsing command line: " + e.getMessage());
            System.exit(-1);
        }

        String inputPath = indexArgs.input;
        String indexPath = indexArgs.index;

        LOG.info("input: " + inputPath);
        LOG.info("index: " + indexPath);

        int numThreads = indexArgs.threads;

        Directory dir = FSDirectory.open(Paths.get(indexPath));
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        final IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());

        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        IndexWriter writer = new IndexWriter(dir, config);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(inputPath));
        final Deque<Path> fileStack = iterateFiles(fs, status);
        new IndexPlainText().indexWithThreads(numThreads, fileStack, writer, fs);
    }
}
