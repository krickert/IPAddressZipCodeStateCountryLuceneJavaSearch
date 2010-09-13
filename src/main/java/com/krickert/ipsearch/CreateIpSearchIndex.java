package com.krickert.ipsearch;

import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * <b>Simple ip address spatial search indexer.</b> <br>
 * With this application, we will automatically download the latest version of
 * the ip address database and create a searchable index for it. <br>
 * 
 * The steps to do this are very procedural:
 * <ol>
 * <li>Download the index file
 * <li>Steam the file into the lucene index
 * <li>Close the index and optimize
 * <li>You can now search with the spatial encoding of IP and location data
 * </ol>
 * 
 * <br>
 * <b>TODO:</b> add options to add to a GIS postgresql <br>
 * <b>TODO:</b> add options to add to a mysql database too <br>
 * <i>Note:</i> This file was downloaded from the <a
 * href=http://ipinfodb.com/ip_database.php>ipinfodb</a>.
 * 
 * <br>
 * <b>Usage</b><br>
 * All of the settings to run this application are contained in the
 * project.properties file. Just use that file and the options should be fairly
 * obvious. If they're not feel free to email me and I'll be glad to upload
 * them.<br>
 * 
 * <b>TODO:</b> Provide some example search queries.<br>
 * 
 * @author krickert
 * 
 */
public class CreateIpSearchIndex {
  private static final Log log = LogFactory.getLog(CreateIpSearchIndex.class);
  private final ApplicationContext context;

  public CreateIpSearchIndex(final ClassPathXmlApplicationContext classPathXmlApplicationContext) {
    this.context = classPathXmlApplicationContext;
  }

  /**
   * 
   * Creates the application context and starts the indexing
   * 
   * @param args
   *          command line options (ignored)
   */
  public static void main(String[] args) {
    CreateIpSearchIndex cisi = new CreateIpSearchIndex(new ClassPathXmlApplicationContext("application-context.xml"));
    cisi.start();
  }

  /**
   * Runs all the external classes to perform the indexing
   * 
   */
  private void start() {
    downloadIpDataFiles();
    // since the parsing occurs in it's own thread, returns the executor so we
    // can shut it down on completion
    ExecutorService execution = parseIpData();
    // time to index
    IndexIpAddressTask task = createLuceneIndex();
    log.info("Completed index.  Committing and flushing to disk.");
    execution.shutdownNow();
    task.commitAndFinish();
  }

  /**
   * Creates the lucene index fron the {@link IndexIpAddressTask}
   * 
   * @return the task object so we can shutdown the parsing thread before
   *         committing
   */
  private IndexIpAddressTask createLuceneIndex() {
    IndexIpAddressTask task = context.getBean(IndexIpAddressTask.class);
    task.insertIntoIndex();
    return task;
  }

  /**
   * Calls on the {@link IpDataReaderTask} to parse the IP Address data from the
   * files that were just downloaded and puts them into the lucene index
   * 
   * @return the IP data
   */
  private ExecutorService parseIpData() {
    log.info("Getting the data into the queue");
    IpDataReaderTask dataReader = context.getBean(IpDataReaderTask.class);
    ExecutorService execution = dataReader.fireAndForget();
    execution.shutdown();
    return execution;
  }

  /**
   * Calls the {@link IpDataDownloader} to download the GIS IP data files.
   */
  private void downloadIpDataFiles() {
    IpDataDownloader dataDownloader = context.getBean(IpDataDownloader.class);
    dataDownloader.downloadFile();
  }
}