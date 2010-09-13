package com.krickert.ipsearch;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.spatial.tier.projections.CartesianTierPlotter;
import org.apache.lucene.spatial.tier.projections.IProjector;
import org.apache.lucene.spatial.tier.projections.SinusoidalProjector;
import org.apache.lucene.util.NumericUtils;

import com.krickert.ipsearch.city.IpSearchCityBean;
import com.krickert.lucene.IndexWriterManager;

/**
 * Continually takes data from a queue and puts it into an index. The indexer
 * itself is already multi threaded so this only runs on a single thread.
 * 
 * @author krickert
 * 
 */
public class IndexIpAddressTask {
  private static final Log log = LogFactory.getLog(IndexIpAddressTask.class);

  private final IndexWriter writer;
  private final BlockingQueue<IpSearchCityBean> queue;
  private final int timeout;

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
   * @throws IOException
   *           if something goes wrong with the indexer, the thread exits,
   *           violently
   */
  public IndexIpAddressTask(IndexWriterManager writer, BlockingQueue<IpSearchCityBean> queue, int timeout) {
    this.writer = checkNotNull(writer.getWriter());
    this.queue = checkNotNull(queue);
    this.timeout = timeout;
  }

  public void insertIntoIndex() {
    log.info("Starting to insert into the indexer");
    try {
      IpSearchCityBean bean;
      boolean done = false;
      while (!done) {
        bean = queue.poll(timeout, TimeUnit.SECONDS);
        if (bean != null) {
          addLocation(bean);
        } else {
          log.info("Marking as complete.");
          done = true;
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }// while loop continues

  /**
   * Adds IPSearch data to the index.
   * 
   * @param bean
   *          the bean it needs to index
   * @throws IOException
   */
  public void addLocation(IpSearchCityBean bean) throws IOException {
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
      doc.add(new Field(ctp.getTierFieldName(), NumericUtils.doubleToPrefixCoded(boxId), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
    }
    writer.addDocument(doc);
  }

  public void commit() {
    log.info("committing..");
    try {
      writer.commit();
    } catch (CorruptIndexException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);

    }
  }

  public void commitAndFinish() {
    log.info("committing..");
    try {
      writer.commit();
      log.info("closing..");
      writer.close();
      log.info("Write complete");
    } catch (CorruptIndexException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);

    }
  }

}// end private class

