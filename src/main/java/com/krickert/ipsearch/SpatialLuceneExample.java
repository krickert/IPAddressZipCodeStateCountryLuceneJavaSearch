package com.krickert.ipsearch;

import java.io.IOException;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.spatial.tier.projections.CartesianTierPlotter;
import org.apache.lucene.spatial.tier.projections.IProjector;
import org.apache.lucene.spatial.tier.projections.SinusoidalProjector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.NumericUtils;

import com.krickert.ipsearch.city.IpSearchCityBean;

public class SpatialLuceneExample {
  String latField = "lat";
  String lngField = "lon";
  String tierPrefix = "_localTier";

  private final Directory directory;
  private final IndexWriter writer;

  SpatialLuceneExample() throws IOException {
    directory = new RAMDirectory();
    writer = new IndexWriter(directory, new WhitespaceAnalyzer(), MaxFieldLength.UNLIMITED);
  }

  private void addLocation(IndexWriter writer, IpSearchCityBean bean) throws IOException {
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

      System.out.println("Adding field " + ctp.getTierFieldName() + ":" + boxId);
      doc.add(new Field(ctp.getTierFieldName(), NumericUtils.doubleToPrefixCoded(boxId), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
    }
    writer.addDocument(doc);
    System.out.println("===== Added Doc to index ====");
  }
}