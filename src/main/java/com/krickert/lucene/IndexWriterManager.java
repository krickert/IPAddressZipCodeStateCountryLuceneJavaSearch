package com.krickert.lucene;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

public class IndexWriterManager {
  private final IndexWriter writer;

  public IndexWriterManager() throws CorruptIndexException, LockObtainFailedException, IOException {
    Directory luceneDirectory = FSDirectory.open(new File("index"));
    this.writer = new IndexWriter(luceneDirectory, new WhitespaceAnalyzer(), MaxFieldLength.UNLIMITED);
  }

  public IndexWriter getWriter() {
    return writer;
  }

  public void finishIndex() throws CorruptIndexException, IOException {
    this.writer.commit();
    this.writer.close();
  }

}
