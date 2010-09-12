package com.krickert.ipsearch;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.CorruptIndexException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * <b>Simple ip address spatial search indexer.</b> <br>
 * With this application, we will automatically download the latest version of
 * the ip address database and create a searchable index for it. <br>
 * The steps to do this are very procedural:
 * <ol>
 * <li>Download the index file
 * <li>Steam the file into the lucene index
 * <li>Close the index and optimize
 * <li>You can now search with the spatial encoding
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

  /**
   * This file should just run over ant. It'll pickup the config via spring.
   * 
   * @param args
   */
  public static void main(String[] args) {
    log.info("Loading the initial application context");
    ApplicationContext context = new ClassPathXmlApplicationContext("application-context.xml");
    log.info("Getting the ipsearch index manager");
    IpSearchIndexManager manager = context.getBean(IpSearchIndexManager.class);
    log.info("Downloading the latest index file");
    manager.downloadFile();
    // file is downloaded .. lets' start some threads
    manager.startExecutors();
    while (manager.isTimeToCommit()) {
      try {
        Thread.sleep(10000);
        log.info("Some task to do");
      } catch (InterruptedException e) {
        log.info(e);
      }

    }

    try {
      manager.cleanUp();
    } catch (CorruptIndexException e) {
      log.fatal("didn't work out..", e);
    } catch (IOException e) {
      log.fatal("check the disk.", e);
    }
  }

}
