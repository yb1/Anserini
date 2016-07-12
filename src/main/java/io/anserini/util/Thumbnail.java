package io.anserini.util;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openqa.selenium.Dimension;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;

import java.io.*;
import java.net.URLEncoder;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by youngbinkim on 7/5/16.
 */
public class  Thumbnail {
    private static final Logger LOG = LogManager.getLogger(Thumbnail.class);

    private Thumbnail() {}

    private static final String WAYBACK_PREFIX = "https://web.archive.org/web/";
    private static final String PHANTOM_OPTION = "phantom";
    private static final String PHANTOM_DEFAULT = "../phantomjs-2.1.1-linux-x86_64/bin/phantomjs";
    private static final AtomicInteger numSuccess = new AtomicInteger(0);
    private static final AtomicInteger numFailure = new AtomicInteger(0);
    private static final String HELP_OPTION = "h";
    private static final String INPUT_OPTION = "input";
    private static final String OUTPUT_OPTION = "output";
    private static final String THREAD_OPTION = "threads";
    private static String phantomjs;

    private final class ScreenshotDriver {
        final private Path inputWarcFile;
        final private FileSystem fs;
        final private String output;
        private PhantomJSDriver driver;
        final private DesiredCapabilities caps;
        private int counter;
        private int numLocalFailures = 0;

        public ScreenshotDriver(String output, Path inputWarcFile, FileSystem fs) throws IOException {
            this.output = output;
            this.inputWarcFile = inputWarcFile;
            this.fs = fs;
            this.caps = new DesiredCapabilities();
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
                driver.get(WAYBACK_PREFIX + url);
                screenshot = driver.getScreenshotAs(OutputType.FILE);
                String fileName = URLEncoder.encode(url, "UTF-8");
                fileName = (fileName.length() > 255) ? fileName.substring(0, 255) : fileName;
                FileUtils.copyFile(screenshot, new File(output + "/" + fileName + ".png"));

            } catch (Exception e) {
                resetDriver();
                if (numTrial < 3) {
                    LOG.error("Trying {} for {}th time -- counter: {}", url, numTrial, counter);
                    takeScreenshot(url, numTrial + 1);
                } else {
                    LOG.error("CANNOT open url {} ", url, e);
                    numLocalFailures++;
                }
            } finally {
                if (screenshot != null) {
                    screenshot.delete();
                }
            }
        }

        private void startDriver() {
            this.driver = new PhantomJSDriver(caps);
            driver.manage().timeouts().pageLoadTimeout(30, TimeUnit.SECONDS);
            driver.manage().window().setSize(new Dimension(640, 480));
            counter = 0;
        }

        private void stopDriver() {
            try {
                //this.driver.close();
                this.driver.quit();
            } catch (Exception e) {
                LOG.error("ERROR: while stopping driver for path {} ", inputWarcFile.getName(), e);
            }
        }

        private void resetDriver(){
            stopDriver();
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
                stopDriver();
            }
            return numLocalFailures;
        }
    }

    private final class ThumbnailThread extends Thread {
        final private LinkedBlockingQueue<Path> warcFiles;
        final private FileSystem fs;
        final private String output;
        final private int threadNum;
        private int numFailuresThread = 0;

        public ThumbnailThread(String output, LinkedBlockingQueue<Path> warcFiles, FileSystem fs, int num) throws IOException {
            this.output = output;
            this.warcFiles = warcFiles;
            this.fs = fs;
            threadNum = num;
        }

        public void processFile(Path cur, int numTrial) {
            ScreenshotDriver driver = null;
            Boolean retry = false;
            try {
                driver = new ScreenshotDriver(output, cur, fs);
                numFailuresThread += driver.run();
            } catch (Exception e) {
                LOG.error("CANNOT run ThumbnailThread {} for path {} numTrial {} ",
                        this.threadNum, cur.getName(), numTrial, e);
                if (numTrial < 3)
                    retry = true;
            } finally {
                if (driver != null)
                    driver.stopDriver();
            }

            if (retry) {
                processFile(cur, numTrial+1);
            }
        }

        @Override
        public void run() {
            try {
                while(!warcFiles.isEmpty()) {
                    Path cur = warcFiles.poll();
                    System.out.println(cur.getName());
                    if (!cur.getName().contains("_SUCCESS"))
                        processFile(cur, 0);
                }
            } catch (Exception e) {
                LOG.error("CANNOT run ThumbnailThread {} ", threadNum, e);
            }
            LOG.info("THREAD {} completed ", threadNum);
            numFailure.addAndGet(numFailuresThread);
        }
    }

    public int screenshotWithThreads(int numThreads, LinkedBlockingQueue<Path> warcFiles, String output, FileSystem fs) throws IOException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads * 10);

        long start = System.currentTimeMillis() / 1000;
        int numIndexed = 0;

        try {
            for (int i = 0; i < numThreads; i++) {
                //System.out.println(" **** THREAD " + i);
                executor.execute(new ThumbnailThread(output, warcFiles, fs, i));
            }

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.DAYS);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("CANNOT index with threads.. ", e);
        }

        long end = System.currentTimeMillis() / 1000;
        System.out.println("Took " + (end - start) + " seconds.");
        // System.out.println("# of success : " + numSuccess.get());
        System.out.println("# of failure : " + numFailure.get());
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
            formatter.printHelp(Thumbnail.class.getName(), options);
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

        new File(outputPath).mkdir();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(inputPath));
        final LinkedBlockingQueue<Path> fileStack = iterateFiles(fs, status);
        new Thumbnail().screenshotWithThreads(numThreads, fileStack, outputPath, fs);
    }
}

