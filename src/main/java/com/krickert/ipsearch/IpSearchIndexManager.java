package com.krickert.ipsearch;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.CorruptIndexException;
import org.springframework.beans.factory.annotation.Autowired;

import com.krickert.ipsearch.city.IpSearchCityBean;
import com.krickert.lucene.IndexWriterManager;

/**
 * The IpSearchIndex manager is a spring managed file made to download the data
 * and create the lucene index
 * 
 * @author krickert
 * 
 */
public class IpSearchIndexManager {

  private static final Log log = LogFactory.getLog(IpSearchIndexManager.class);

  private final IpDataDownloader downloader;
  private final IpDataReaderTask dataReaderThread;
  private final IndexIpAddressTask indexer;
  private final AtomicBoolean lastDocInQueue;
  private final BlockingQueue<IpSearchCityBean> queue;
  private final IndexWriterManager writer;

  @Autowired
  public IpSearchIndexManager(IpDataDownloader downloader, IpDataReaderTask dataReaderThread, IndexIpAddressTask indexer,
      AtomicBoolean lastDocInQueue, BlockingQueue<IpSearchCityBean> queue, IndexWriterManager writer) {
    this.downloader = checkNotNull(downloader);
    this.dataReaderThread = checkNotNull(dataReaderThread);
    this.indexer = checkNotNull(indexer);
    this.lastDocInQueue = checkNotNull(lastDocInQueue);
    this.queue = checkNotNull(queue);
    this.writer = checkNotNull(writer);
  }

  public void downloadFile() {
    downloader.downloadFile();
    log.info("Successfully downloaded file");
  }

  public void cleanUp() throws CorruptIndexException, IOException {
    writer.finishIndex();
  }

  public ExecutorService[] startExecutors() {
    ExecutorService[] executors = new ExecutorService[2];
    executors[0] = dataReaderThread.executeCallback();
    executors[1] = indexer.executeCallback();
    return executors;
  }

  public boolean isTimeToCommit() {
    if (this.lastDocInQueue.get() && queue.size() == 0) {
      return true;
    } else {
      return false;
    }
  }
}
