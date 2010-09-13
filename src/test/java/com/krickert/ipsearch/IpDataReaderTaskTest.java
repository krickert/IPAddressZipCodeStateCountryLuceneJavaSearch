package com.krickert.ipsearch;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import junit.framework.TestCase;

import com.krickert.ipsearch.city.IpSearchCityBean;

public class IpDataReaderTaskTest extends TestCase {

  private BlockingQueue<IpSearchCityBean> queue;

  public IpDataReaderTaskTest(String name) {
    super(name);
  }

  public void testIpDataReaderParseLines() {
    assertEquals(1819, queue.size());// minus one for the heade
  }

  public void testReferencePoint() throws InterruptedException {
    IpSearchCityBean bean = queue.poll();
    assertEquals(0.0d, bean.getLat());
    assertEquals(0.0d, bean.getLon());
    assertEquals((Long) 0l, bean.getIpStart());
    assertEquals("Reserved", bean.getCountryName());
    assertEquals("RD", bean.getCountryCode());
    assertEquals("", bean.getCity());
    assertEquals("", bean.getZipCode());
  }

  public void testNoNullIpAddresses() {
    int numZeros = 0;
    for (IpSearchCityBean bean : queue) {
      assertNotNull(bean.getIpStart());
      if (bean.getIpStart() == 0l) {
        numZeros++;
      }
      // first entry is a reference point, should be the only 0
      assertEquals(1, numZeros);
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    BlockingQueue<IpSearchCityBean> queue = new ArrayBlockingQueue<IpSearchCityBean>(1820);
    IpDataReaderTask task = new IpDataReaderTask("src/test/resources/ipsearch_test.zip", "ip_group_city.csv", queue);
    task.fire();
    this.queue = queue;
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

}
