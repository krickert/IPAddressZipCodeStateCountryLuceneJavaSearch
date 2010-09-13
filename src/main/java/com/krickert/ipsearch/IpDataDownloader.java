package com.krickert.ipsearch;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.krickert.wget.Wget.wGet;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.krickert.wget.WgetStatus;

/**
 * Simple data downloader that does a wget on the ip address file using the <a
 * href="http://github.com/krickert/java-wget"/>krickert wget lib</a> <br>
 * This file requires that two properties in the project property file exist:<br>
 * <ol>
 * <li><i>(required)</i> ipsearch.file.zip = the name of the file that we're
 * going to save to. Include the full path if needed otherwise it'll just
 * download in the same directory
 * 
 * <li><i>(optional, required if ipsearch.download.zip exists)</i>
 * ipsearch.url.csv = the name of the url to download the ipinfo from in csv
 * format. Please see {@link IpDataReaderTask} for the expected file format
 * 
 * <li><i>(required)</i> ipsearch.should.download = (true | false). True if
 * attempted to download. False if it assumes it's already downloaded and we
 * want do index. If the file was already there, it'll overwrite it.
 * </ol>
 * 
 * @author krickert
 * 
 */
public class IpDataDownloader {
  private static Log log = LogFactory.getLog(IpDataDownloader.class);

  private final String urlOfZip;
  private final String fileName;
  private final Boolean shouldDownlaod;

  public IpDataDownloader(String urlOfZip, String fileName, Boolean shouldDownlaod) {
    super();
    this.urlOfZip = urlOfZip;
    this.fileName = checkNotNull(fileName);
    this.shouldDownlaod = checkNotNull(shouldDownlaod);
  }

  /**
   * Downloads the ip search index file from the <i>ip.url.csv</i> in the
   * project.properties file.
   */
  public void downloadFile() {
    if (!shouldDownlaod) {
      checkIpFileExists();
      log.info("Download skipped from user request.  File is there so we're good to go.");
      return;
    }
    WgetStatus status = wGet(fileName, urlOfZip);
    if (status != WgetStatus.Success) {
      String error = "We couldn't get the ip address information because " + status;
      log.error(error);
      throw new IllegalStateException(error);
    }
    checkIpFileExists();
  }

  /**
   * Checks to see if the ip geo file is on the server after downloading or if
   * the user suggests that they dont want to download it
   */
  private boolean checkIpFileExists() {
    File f = new File(fileName);
    if (!f.exists()) {
      String error = "File " + fileName + " specified does not exist.  Please make  that either the file " + fileName
          + " is there or set the URL in the property file to be there.  Try using "
          + "\"http://mirrors.ipinfodb.com/ipinfodb/ip_database/current/ipinfodb_one_table_full.csv.zip\" as the "
          + "property \"ipsearch.url.csv\" (no quotes) and \"ipinfodb_one_table_full.csv.zip\" as the \"ipsearch.file.zip\" "
          + "(no quotes) in the project.properties file in your classpath. ";
      log.error(error);
      throw new IllegalStateException(error);
    }
    return true;
  }

}
