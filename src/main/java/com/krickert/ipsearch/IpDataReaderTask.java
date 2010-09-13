package com.krickert.ipsearch;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.constraint.LMinMax;
import org.supercsv.cellprocessor.constraint.StrMinMax;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import com.krickert.ipsearch.city.IpSearchCityBean;

/**
 * This portion of the application will take in the indexer and start outputting
 * the data into a concurrent queue for processing by multiple threads into a
 * single index writer. <br>
 * This reader will read the data in a Zip format and use the SuperCSV api to
 * read the data "fast" and put it into a bean for reading <br>
 * The data is expected to be in the table_full format from the csv offered on
 * the website. As of the time of this writing this file is over 400 megabytes
 * long and has over 4047599 entries in them. This means an average of 103.6
 * bytes per entry. <br>
 * The data file is in this format:
 * 
 * <pre>
 * "ip_start";"country_code";"country_name";"region_code";"region_name";"city";"zipcode";"latitude";"longitude";"metrocode"
 * </pre>
 * 
 * Sample data:
 * 
 * <pre>
 * "3523140760";"US";"United States";"17";"Illinois";"Chicago";"60611";"41.9288";"-87.6315";"602"
 * "3523140848";"US";"United States";"17";"Illinois";"Chicago";"60657";"41.9373";"-87.6551";"602"
 * </pre>
 * 
 * <br>
 * You can find out minimums and maximums by looking at the data below and
 * matching it to the processor array. <br>
 * 
 * 
 * @author krickert
 * 
 */
public class IpDataReaderTask {
  private static final Log log = LogFactory.getLog(IpDataReaderTask.class);
  /* the processors were figured out by analyzing the data within */
  private static final CellProcessor[] processors = { new LMinMax(0l, 4278190080l), new StrMinMax(2l, 2l),
      new Optional(new StrMinMax(4l, 50l)), new StrMinMax(0l, 2l), new StrMinMax(0l, 41l), new StrMinMax(0l, 34l), new StrMinMax(0l, 6l),
      new ParseDouble(), new ParseDouble(), new StrMinMax(0l, 3l) };

  /*
   * the column mapping file used to reflect between the processors above and
   * the data in the file.
   */
  private static final String[] columnMapping = { "ipStart", "countryCode", "countryName", "regionCode", "regionName", "city", "zipCode",
      "lat", "lon", "metroCode" };

  public final String zipFileName;
  public final String fileInZip;
  public final BlockingQueue<IpSearchCityBean> queue;

  /**
   * This is a thread that's meant to be run on a single queue and a single file
   * per thread. In other words, right now it should only have a single thread
   * running while multiple other threads are reading from the concurrent queue.
   * 
   * @param zipFileName
   *          the name of the file that's zipp'ed up from the internet that has
   *          the ip spatial data
   * @param fileInZip
   *          the path and name of the file in the ZIP file
   * @param queue
   *          the blocking queue that this thread will send all the parsed data
   *          from the file off to
   */
  public IpDataReaderTask(String zipFileName, String fileInZip, BlockingQueue<IpSearchCityBean> queue) {
    super();
    this.zipFileName = checkNotNull(zipFileName);
    this.fileInZip = checkNotNull(fileInZip);
    this.queue = checkNotNull(queue);
  }

  public ExecutorService fireAndForget() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    IpDataReaderThread runner = new IpDataReaderThread();
    executor.execute(runner);
    return executor;
  }

  private class IpDataReaderThread implements Runnable {
    @Override
    public void run() {
      this.queueIpEntries();
    }

    /**
     * The action function that takes in a file input stream, converts the data
     * via a CSV parser, and sends it off to a queue for processing by multiple
     * threads.
     */
    public void queueIpEntries() {
      InputStream fileStream = null;

      try {
        fileStream = new FileInputStream(zipFileName);
      } catch (FileNotFoundException e) {
        log.fatal("The file we were supposed to download does not exist: [" + zipFileName + "]", e);
        throw new IllegalStateException("File check occurred after downloading/starting application and is no longer there.", e);
      }

      ZipInputStream zip = new ZipInputStream(fileStream);
      try {
        boolean searchingForFile = true;
        while (searchingForFile) {
          ZipEntry entry = zip.getNextEntry();
          if (entry == null) {
            log.error("The zip file is valid but does not match the ${ipsearch.fileinzip} entry from the project.properties file");
            zip.close();
            throw new IllegalArgumentException("Couldn't find file " + fileInZip + " in zip archive " + zipFileName);
          } else if (entry.getName().equals(fileInZip)) {
            searchingForFile = false;
          }
        }
      } catch (IOException e) {
        log.fatal("zip file appears to be empty.", e);
        throw new IllegalArgumentException("problem opening up the zip file");
      }
      Reader fr = null;
      ICsvBeanReader inFile = null;
      try {
        fr = new InputStreamReader(zip);
        inFile = new CsvBeanReader(fr, CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE);

        final String[] header = inFile.getCSVHeader(true);
        log.info("The following header was parsed: " + Arrays.toString(header));

        IpSearchCityBean ipRow;
        int counter = 0;
        while ((ipRow = inFile.read(IpSearchCityBean.class, columnMapping, processors)) != null) {
          if (counter++ % 50000 == 0 && counter > 0) {
            log.info(counter + " number of records parsed.");
          }
          queue.put(ipRow);
        }
      } catch (IOException e) {
        throw new IllegalStateException("The zip file opened but an IO exception was thrown while reading the zip file.", e);
      } catch (Exception e) {
        // from the queue offering
        log.error("queue offering interrupted.");
      } finally {
        if (inFile != null) {
          try {
            inFile.close();
          } catch (IOException e) {
            log.fatal(e);
          }
        }// infile null end
        if (fr != null) {
          try {
            fr.close();
          } catch (IOException e) {
            log.warn("failed to close file reader from zip file.", e);
          }
        }
      }
      log.info("*******************\n** IpData all in queue.  Terminating process\n**\n******************");

    }

  }
}