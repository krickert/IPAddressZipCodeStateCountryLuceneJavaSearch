package com.krickert.ipsearch;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import junit.framework.TestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.tier.DistanceFieldComparatorSource;
import org.apache.lucene.spatial.tier.DistanceQueryBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import com.krickert.ipsearch.city.IpSearchCityBean;
import com.krickert.lucene.IndexWriterManager;

public class IndexIpAddressTaskTest extends TestCase {
  String[] expectedFields = { "zip_code", "metro_code", "ip_start", "country_name", "city", "region_name", "ip_start_d", "ip_start_c",
      "ip_start_b", "lat", "ip_start_a", "lon", "_localTier13", "country_code", "_localTier14", "_localTier15", "_localTier10",
      "_localTier12", "_localTier5", "_localTier11", "region_code", "_localTier6", "_localTier7", "_localTier8", "_localTier9" };

  private IndexWriterManager writerManager;
  private Directory directory;
  private IndexWriter writer;
  private IndexReader reader;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    BlockingQueue<IpSearchCityBean> queue = new ArrayBlockingQueue<IpSearchCityBean>(1820);
    IpDataReaderTask task = new IpDataReaderTask("src/test/resources/ipsearch_test.zip", "ip_group_city.csv", queue);
    task.fire();
    RAMDirectory directory = new RAMDirectory();
    this.directory = directory;
    WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer();
    int numThreads = 2;
    int queueSize = 2000;// over 1800 for faster testing
    int ramBufferSizeMb = 20;
    int mergeFactor = 10;
    this.writerManager = new IndexWriterManager(directory, analyzer, numThreads, queueSize, ramBufferSizeMb, mergeFactor);
    IndexIpAddressTask indexTask = new IndexIpAddressTask(writerManager, queue, 2);
    indexTask.insertIntoIndex();
    indexTask.commit();
    this.writer = writerManager.getWriter();
    this.reader = writer.getReader();

  }

  public void testFieldNames() throws IOException {
    int i = 0;
    for (String field : reader.getFieldNames(FieldOption.ALL)) {
      assertEquals(expectedFields[i], field);
      i++;
    }
  }

  public void testDistanceQuery() throws CorruptIndexException, IOException {
    DistanceQueryBuilder dq;
    dq = new DistanceQueryBuilder(34.0285, -118.318, 10, "lat", "lon", "_localTier", true);
    Query tq;
    tq = new TermQuery(new Term("region_name", "California"));
    DistanceFieldComparatorSource dsort;
    dsort = new DistanceFieldComparatorSource(dq.getDistanceFilter());
    Sort sort = new Sort(new SortField("city", dsort));
    IndexSearcher searcher = new IndexSearcher(directory);
    TopDocs hits = searcher.search(tq, dq.getFilter(), 10, sort);
    int numResults = hits.totalHits;
    assertEquals(numResults, 15);

  }
}
