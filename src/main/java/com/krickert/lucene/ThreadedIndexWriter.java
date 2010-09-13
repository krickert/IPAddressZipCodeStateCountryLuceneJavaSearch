package com.krickert.lucene;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;

public class ThreadedIndexWriter extends IndexWriter {
  private final ExecutorService threadPool;
  private final Analyzer defaultAnalyzer;

  private class Job implements Runnable {
    Document doc;
    Analyzer analyzer;
    Term delTerm;

    public Job(Document doc, Term delTerm, Analyzer analyzer) {
      this.doc = doc;
      this.analyzer = analyzer;
      this.delTerm = delTerm;
    }

    @Override
    public void run() {
      try {
        if (delTerm != null) {
          ThreadedIndexWriter.super.updateDocument(delTerm, doc, analyzer);
        } else {
          ThreadedIndexWriter.super.addDocument(doc, analyzer);
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  public ThreadedIndexWriter(Directory dir, Analyzer a, boolean create, int numThreads, int maxQueueSize, IndexWriter.MaxFieldLength mfl)
      throws CorruptIndexException, IOException {
    super(dir, a, create, mfl);
    defaultAnalyzer = a;
    threadPool = new ThreadPoolExecutor(numThreads, numThreads, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(maxQueueSize, false),
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @Override
  public void addDocument(Document doc) {
    threadPool.execute(new Job(doc, null, defaultAnalyzer));
  }

  @Override
  public void addDocument(Document doc, Analyzer a) {
    threadPool.execute(new Job(doc, null, a));
  }

  @Override
  public void updateDocument(Term term, Document doc) {
    threadPool.execute(new Job(doc, term, defaultAnalyzer));
  }

  @Override
  public void updateDocument(Term term, Document doc, Analyzer a) {

    threadPool.execute(new Job(doc, term, a));
  }

  @Override
  public void close() throws CorruptIndexException, IOException {
    finish();
    super.close();
  }

  @Override
  public void close(boolean doWait) throws CorruptIndexException, IOException {
    finish();
    super.close(doWait);
  }

  @Override
  public void rollback() throws CorruptIndexException, IOException {
    finish();
    super.rollback();
  }

  private void finish() {
    threadPool.shutdown();
    while (true) {
      try {
        if (threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
          break;
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }
    }
  }
}
