package com.krickert.lucene;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;

/**
 * This is a scaffolding to make it easier to inject the index writer.
 * 
 * Since the index writer has all these set methods to set the ram buffer sizes,
 * merge factor, or any other index methods
 * 
 * @author krickert
 * 
 */
public class IndexWriterManager {
  private final IndexWriter writer;

  /**
   * Constructor that's made to create a new index writer to be used by the
   * application
   * 
   * @param directory
   *          the directory object for the indexer
   * @param analyzer
   *          the analyzer type
   * @param numThreads
   *          the number of threads to create to concurrently run in the indexer
   * @param queueSize
   *          the size of the queue for new documents to add
   * @param ramBufferSizeMb
   *          the size, in MB of the ram buffer to use before flushing to disk
   * @param mergeFactor
   *          the marge factor - don't make this too big or you'll run out of
   *          file handles
   * @throws CorruptIndexException
   *           when things go nuts
   * @throws LockObtainFailedException
   *           there's another indexer running if you see this
   * @throws IOException
   *           when something is wrong with the FS - permissions of the file or
   *           you run out of space
   */
  public IndexWriterManager(Directory directory, Analyzer analyzer, int numThreads, int queueSize, int ramBufferSizeMb, int mergeFactor)
      throws CorruptIndexException, LockObtainFailedException, IOException {

    checkNotNull(directory);

    this.writer = new ThreadedIndexWriter(directory, analyzer, true, numThreads, queueSize, MaxFieldLength.UNLIMITED);
    // NOTE: max buffered docs is going to get set to DISABLE_AUTO_FLUSH because
    // it will help maximize the performance for indexing
    writer.setMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
    // the size of the ram buffer before flushing everything to disk. Makes
    // stuff faster for indexing for the cost of memory.
    writer.setRAMBufferSizeMB(ramBufferSizeMb);
    // The number of segments that are merged by add document. Don't go too
    // crazy.. them inodes get mad
    writer.setMergeFactor(mergeFactor);
  }

  public IndexWriter getWriter() {
    return writer;
  }

  public void finishIndex() throws CorruptIndexException, IOException {
    this.writer.commit();
    this.writer.close();
  }

}
