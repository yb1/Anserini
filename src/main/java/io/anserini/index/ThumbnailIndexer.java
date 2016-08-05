package io.anserini.index;


import com.sun.jna.Library;
import com.sun.jna.Native;
import org.apache.commons.cli.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
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
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.openqa.selenium.Dimension;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by youngbinkim on 7/5/16.
 */
public class  ThumbnailIndexer {
    private static final Logger LOG = LogManager.getLogger(ThumbnailIndexer.class);

    private ThumbnailIndexer() {}

    private static final String WAYBACK_PREFIX = "https://web.archive.org/web/";
    private static final String PHANTOM_OPTION = "phantom";
    private static final String PHANTOM_DEFAULT = "../phantomjs-2.1.1-linux-x86_64/bin/phantomjs";
    private static final AtomicInteger numSuccess = new AtomicInteger(0);
    private static final AtomicInteger numFailure = new AtomicInteger(0);
    private static final String HELP_OPTION = "h";
    private static final String INPUT_OPTION = "input";
    private static final String OUTPUT_OPTION = "output";
    private static final String THREAD_OPTION = "threads";
    public static final String FIELD_BODY = "body";
    public static final String FIELD_URL = "url";
    private static String phantomjs;

    private final class ScreenshotDriver {
        final private Path inputWarcFile;
        final private FileSystem fs;
        final private String output;
        final private IndexWriter writer;
        private Image src;
        private BufferedImage dst;
        private PhantomJSDriver driver;
        final private DesiredCapabilities caps;
        private int counter;
        private int numLocalFailures = 0;

        public ScreenshotDriver(String output, Path inputWarcFile, FileSystem fs, IndexWriter writer) throws IOException {
            this.output = output;
            this.inputWarcFile = inputWarcFile;
            this.fs = fs;
            this.caps = DesiredCapabilities.phantomjs();
            this.writer = writer;
            caps.setJavascriptEnabled(true);
            caps.setCapability("takesScreenshot", true);
            caps.setCapability(
                    PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY,
                    phantomjs
            );
            String[] phantomArgs = new  String[] {
                    "--webdriver-loglevel=NONE"
            };
            caps.setCapability(PhantomJSDriverService.PHANTOMJS_CLI_ARGS, phantomArgs);
            startDriver();
        }

        private void takeScreenshot(String url, int numTrial) {
            File screenshot = null;
            try {
                PhantomJSDriver driver = getDriver();
                //System.out.println(driver.manage().window().getSize().getHeight());
                driver.get(WAYBACK_PREFIX + url);
                driver.executeScript("if (document.getElementById('wm-ipp')) document.getElementById('wm-ipp').remove();");
                //driver.executeScript("alert('hello')");
                //System.out.println(driver.executeScript("return window.screen.width"));
                screenshot = driver.getScreenshotAs(OutputType.FILE);

                // Long start = System.currentTimeMillis();

                src = ImageIO.read(screenshot);

                int x = 0, y = 0, w = 640, h = 480;

                dst = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
                dst.getGraphics().drawImage(src, 0, 0, w, h, x, y, x + w, y + h, null);
                // ImageIO.write(dst, "png", new File(output + "/" + fileName + "ss.png"));

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ImageIO.write(dst, "png", baos );
                baos.flush();
                byte[] imageInBytes = baos.toByteArray();
                baos.close();
                // System.out.println("test " + (System.currentTimeMillis() - start));


                String encoded = Base64.encodeBase64String(imageInBytes);
                Document document = new Document();
                document.add(new StringField(FIELD_URL, url, Field.Store.YES));
                document.add(new TextField(FIELD_BODY, encoded, Field.Store.YES));

                writer.addDocument(document);
                LOG.debug("Indexed url {} ", url);
            } catch (Exception e) {
                resetDriver();
                if (numTrial < 3) {
                    LOG.warn("Trying {} for {}th time -- counter: {}", url, numTrial, counter);
                    takeScreenshot(url, numTrial + 1);
                } else {
                    LOG.error("CANNOT open url {} ", url, e);
                    e.printStackTrace();
                    numLocalFailures++;
                }
            } finally {
                if (screenshot != null) {
                    screenshot.delete();
                }
                dst.flush();
                src.flush();
            }
        }

        private void startDriver() {
            this.driver = new PhantomJSDriver(caps);
            this.driver.manage().timeouts()
                    .implicitlyWait(30, TimeUnit.SECONDS)
                    .pageLoadTimeout(30, TimeUnit.SECONDS);
            this.driver.manage().window().setSize(new Dimension(30, 30));
            counter = 0;
        }

        private void stopDriver(boolean completed) {
            if (this.driver == null)
                return;
            try {
                //this.driver.close();
                this.driver.quit();
                this.driver = null;
            } catch (Exception e) {
                LOG.error("ERROR: while stopping driver for path {} status {} ", inputWarcFile.getName(), e, completed);
            }
        }

        private void resetDriver(){
            stopDriver(false);
            startDriver();
        }

        public int run() {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(inputWarcFile)));

                String url;

                while ((url=reader.readLine())!= null)
                {
                    try {
                        takeScreenshot(url, 0);
                    } catch (Exception e) {
                        LOG.error("ERROR occurred while taking screenshot of url {}", url, e);
                    }

                    counter++;
                }
                reader.close();
            } catch (Exception e) {
                LOG.error("CANNOT read urls from path {} ", inputWarcFile.getName(), e);
            } finally {
                LOG.debug("Driver has been completed.. Stop the driver..");
                stopDriver(true);
            }
            return numLocalFailures;
        }

        public PhantomJSDriver getDriver() {
            return driver;
        }
    }

    private final class ThumbnailThread extends Thread {
        final private LinkedBlockingQueue<Path> warcFiles;
        final private FileSystem fs;
        final private String output;
        final private int threadNum;
        final private IndexWriter writer;
        private int numFailuresThread = 0;

        public ThumbnailThread(String output, LinkedBlockingQueue<Path> warcFiles, FileSystem fs, int num, IndexWriter writer) throws IOException {
            this.output = output;
            this.warcFiles = warcFiles;
            this.fs = fs;
            this.writer = writer;
            threadNum = num;
        }

        public boolean processFile(Path cur, int numTrial) {
            ScreenshotDriver driver = null;
            Boolean retry = false;
            try {
                driver = new ScreenshotDriver(output, cur, fs, writer);
                driver.run();
            } catch (OutOfMemoryError e) {
                LOG.error("ERROR: unable to create a new native thread.. ");
                // return false;
            } catch (Exception e) {
                LOG.error("CANNOT run ThumbnailThread {} for path {} numTrial {} ",
                        this.threadNum, cur.getName(), numTrial, e);
                if (numTrial < 3)
                    retry = true;
            } finally {
                if (driver != null)
                    driver.stopDriver(true);
            }

            if (retry) {
                return processFile(cur, numTrial+1);
            }
            return true;
        }

        @Override
        public void run() {
            try {
                while(!warcFiles.isEmpty()) {
                    if (threadNum == 1) {
                        Process p = Runtime.getRuntime().exec("./cleanPhantomProcess");
                        p.waitFor();

                        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

                        try {
                            String line;
                            while ((line = reader.readLine())!= null) {
                                System.out.println(line);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        p.destroy();
                    }
                    Path cur = warcFiles.poll();
                    System.out.println(cur.getName());
                    if (!cur.getName().contains("_SUCCESS")) {
                        if (!processFile(cur, 0)) {
                            //warcFiles.put(cur); // put it back in
                            LOG.info("THREAD {} completed (unexpectedly)", threadNum);
                            return;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("CANNOT run ThumbnailThread {} ", threadNum, e);
            }
            LOG.info("THREAD {} completed ", threadNum);
            numFailure.addAndGet(numFailuresThread);
        }
    }

    public int screenshotWithThreads(int numThreads, LinkedBlockingQueue<Path> warcFiles, String output, FileSystem fs, IndexWriter writer) throws IOException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads * 10);

        long start = System.currentTimeMillis() / 1000;
        int numIndexed = 0;

        try {
            for (int i = 0; i < numThreads; i++) {
                //System.out.println(" **** THREAD " + i);
                executor.execute(new ThumbnailThread(output, warcFiles, fs, i+1, writer));
            }

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.DAYS);
            writer.commit();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("CANNOT index with threads.. ", e);
        }

        long end = System.currentTimeMillis() / 1000;
        System.out.println("Took " + (end - start) + " seconds.");
        // System.out.println("# of success : " + numSuccess.get());
        // System.out.println("# of failure : " + numFailure.get());
        return numIndexed;
    }

    public static LinkedBlockingQueue<Path> iterateFiles(FileSystem fs, FileStatus[] status) {
        final LinkedBlockingQueue<Path> fileStack = new LinkedBlockingQueue<Path>();

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

        Options options = new Options();

        options.addOption(new Option(HELP_OPTION, "show help"));

        options.addOption(OptionBuilder.withArgName("input").hasArg()
                .withDescription("input").create(INPUT_OPTION));
        options.addOption(OptionBuilder.withArgName("output").hasArg()
                .withDescription("output").create(OUTPUT_OPTION));
        options.addOption(OptionBuilder.withArgName("threads").hasArg()
                .withDescription("# of threads ").create(THREAD_OPTION));


        CommandLine cmdline = null;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            System.exit(-1);
        }

        if (cmdline.hasOption(HELP_OPTION) || !cmdline.hasOption(INPUT_OPTION)
                || !cmdline.hasOption(OUTPUT_OPTION) || !cmdline.hasOption(THREAD_OPTION)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(ThumbnailIndexer.class.getName(), options);
            System.exit(-1);
        }

        String inputPath = cmdline.getOptionValue(INPUT_OPTION);
        String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
        phantomjs = (cmdline.hasOption(PHANTOM_OPTION)) ? cmdline.getOptionValue(PHANTOM_OPTION) : PHANTOM_DEFAULT;
        System.out.println("# of threads : " + cmdline.getOptionValue(THREAD_OPTION));
        int numThreads = Integer.parseInt(cmdline.getOptionValue(THREAD_OPTION));

        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        Directory dir = FSDirectory.open(Paths.get(outputPath));
        final IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        ConcurrentMergeScheduler scheduler = new ConcurrentMergeScheduler();
        scheduler.setMaxMergesAndThreads((int) (numThreads * 1.1), (int) (numThreads * 1.1));
        config.setMergeScheduler(scheduler);
        // scheduler.
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        //config.getMergeScheduler().
        //IndexWriterManager writerManager = new IndexWriterManager(config, dir);
        final IndexWriter writer = new IndexWriter(dir, config);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    System.out.println("Shutting down the program..");
                    if (writer.isOpen()) {
                        writer.commit();
                        writer.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        new File(outputPath).mkdir();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(inputPath));
        final LinkedBlockingQueue<Path> fileStack = iterateFiles(fs, status);
        String userName = System.getProperty("user.name");
        File scriptFile = new File("cleanPhantomProcess");
        FileUtils.writeStringToFile(scriptFile, "#!/bin/bash\n"
                + "echo 'Checking phantom processes..'\n"
                + "if [ $(ps -u " + userName + "| grep -c 'phantomjs') -ge " + (numThreads + 3)
                + " ];\n  then\n    echo 'Status: Abort'\n    kill -9 `ps -u " + userName
                + " | grep 'phantomjs' | awk '{print $1}'`;\n  else echo 'Status: Normal' ; \nfi\n");
        scriptFile.setExecutable(true);
        new ThumbnailIndexer().screenshotWithThreads(numThreads, fileStack, outputPath, fs, writer);
        scriptFile.delete();
    }
    private interface CLibrary extends Library {
        CLibrary INSTANCE = (CLibrary) Native.loadLibrary("c", CLibrary.class);
        int getpid ();
    }
}

class IndexWriterManager {
    private static final Logger LOG = LogManager.getLogger(IndexWriterManager.class);

    private IndexWriter writer;
    final private IndexWriterConfig config;
    final Directory dir;

    IndexWriterManager(IndexWriterConfig config, Directory dir) {
        this.config = config;
        this.dir = dir;
        init();
    }
    private void init() {
        try {
            writer = new IndexWriter(this.dir, config);
        } catch (Exception e) {
            LOG.error("CANNOT create indexWriterManager " , e);
        }
    }

    public synchronized void resetIfNeeded() {
        if (!writer.isOpen()) {
            init();
        }
    }


    public IndexWriter getWriter() {
        return writer;
    }
}

