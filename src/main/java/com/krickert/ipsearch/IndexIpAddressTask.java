package com.krickert.ipsearch;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.spatial.tier.projections.CartesianTierPlotter;
import org.apache.lucene.spatial.tier.projections.IProjector;
import org.apache.lucene.spatial.tier.projections.SinusoidalProjector;
import org.apache.lucene.util.NumericUtils;

import com.krickert.ipsearch.city.IpSearchCityBean;
import com.krickert.lucene.IndexWriterManager;

public class IndexIpAddressTask {
  private static final Log log = LogFactory.getLog(IndexIpAddressTask.class);

  private final IndexWriter writer;
  private final BlockingQueue<IpSearchCityBean> queue;
  private final int timeOut;
  private final AtomicBoolean lastEntryInQueue;
  private final int numThreads;

  private static final String latField = "lat";
  private static final String lngField = "lon";
  private static final String tierPrefix = "_localTier";

  /**
   * The task of this indexer is to create the spatial ip address index. There
   * is an executor that creates a configurable number of threads and will all
   * hit the same index writer index.
   * 
   * The data will come from the concurrent queue which will be concurrently
   * read by the reader thread as this is being indexed. Once the reader thread
   * is complete, it will set the lastEntryInQueue boolean to notify the other
   * threads that it's time to exit and that no more messages will be coming
   * into the queue.
   * 
   * @param writer
   *          a wrapper that created the writer object to create the index
   * @param queue
   *          the concurrent queue that the reader is sending objects into for
   *          lucene to write
   * @param lastEntryInQueue
   *          boolean to signifiy that there is no more writing needed to go on
   *          in the index. From this point once the thread sees that this task
   *          has ended, it will exit when the queue size drains to 0
   * @param timeOut
   *          the timeout, in seconds, that a single thread will wait before the
   *          concurrent queue will return null.
   * @param numThreads
   *          the number of threads to spawn off
   * @throws IOException
   *           if something goes wrong with the indexer, the thread exits,
   *           violently
   */
  public IndexIpAddressTask(IndexWriterManager writer, BlockingQueue<IpSearchCityBean> queue, AtomicBoolean lastEntryInQueue, int timeOut,
      int numThreads) throws IOException {
    this.writer = checkNotNull(writer.getWriter());
    this.lastEntryInQueue = checkNotNull(lastEntryInQueue);
    this.queue = checkNotNull(queue);
    this.timeOut = timeOut;
    this.numThreads = numThreads;
  }

  /**
   * Starts the writers up in an executor service and issues the shutdown to the
   * executor.
   * 
   * @return the executor service that started and executed the threads
   */
  public ExecutorService executeCallback() {
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executor.execute(new IndexIpAddressThread());
    }
    executor.shutdown();
    return executor;
  }

  /**
   * This thread is going to take the current writer and try to slam it as fast
   * as it humanly can with as many docs as possible. The number of threads are
   * set in the project.properties file as ${ipsearch.writer.num.threads}. <br>
   * 
   * @author krickert
   * 
   */
  private class IndexIpAddressThread implements Runnable {

    @Override
    /**
     *
     * Three cases:
     * 1) Last entry is in the queue
     * 2) After that, queue has drained to nothing
     * 3) Once queue has drained to nothing wait for all threads to complete
     * 4) Commit.
     */
    public void run() {
      IpSearchCityBean bean = null;
      while (!lastEntryInQueue.get() && queue.size() != 0) {
        try {
          bean = queue.poll(timeOut, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          log.info("Thread interrupted while polling for a new index.", e);
          break;
        }
        if (bean != null) {
          try {
            addLocation(bean);
          } catch (IOException e) {
            throw new IllegalStateException("an IO exception occurred while making the index.  No more disk space?", e);
          }
        }// bean not null
      }// while loop continues
    }// end run

    /**
     * Adds IPSearch data to the index.
     * 
     * @param bean
     *          the bean it needs to index
     * @throws IOException
     */
    private void addLocation(IpSearchCityBean bean) throws IOException {
      Document doc = new Document();
      doc.add(new NumericField("ip_start", Field.Store.YES, true).setLongValue(bean.getIpStart()));
      doc.add(new NumericField("ip_start_a", Field.Store.NO, true).setLongValue((bean.getIpStart() / 16777216l) % 256));
      doc.add(new NumericField("ip_start_b", Field.Store.NO, true).setLongValue((bean.getIpStart() / 65536) % 256));
      doc.add(new NumericField("ip_start_c", Field.Store.NO, true).setLongValue((bean.getIpStart() / 256) % 256));
      doc.add(new NumericField("ip_start_d", Field.Store.NO, true).setLongValue((bean.getIpStart()) % 256));
      doc.add(new Field(latField, NumericUtils.doubleToPrefixCoded(bean.getLat()), Field.Store.YES, Field.Index.NOT_ANALYZED));
      doc.add(new Field(lngField, NumericUtils.doubleToPrefixCoded(bean.getLon()), Field.Store.YES, Field.Index.NOT_ANALYZED));
      doc.add(new Field("city", bean.getCity(), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("zip_code", bean.getZipCode(), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("country_code", bean.getCountryCode(), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("country_name", bean.getCountryName(), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("metro_code", bean.getMetroCode(), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("region_code", bean.getRegionCode(), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("region_name", bean.getRegionName(), Field.Store.YES, Field.Index.ANALYZED));

      IProjector projector = new SinusoidalProjector();
      int startTier = 5;
      int endTier = 15;
      for (; startTier <= endTier; startTier++) {
        CartesianTierPlotter ctp;
        ctp = new CartesianTierPlotter(startTier, projector, tierPrefix);
        double boxId = ctp.getTierBoxId(bean.getLat(), bean.getLon());
        doc.add(new Field(ctp.getTierFieldName(), NumericUtils.doubleToPrefixCoded(boxId), Field.Store.YES,
            Field.Index.NOT_ANALYZED_NO_NORMS));
      }
      writer.addDocument(doc);
    }

  }// end private class

}
