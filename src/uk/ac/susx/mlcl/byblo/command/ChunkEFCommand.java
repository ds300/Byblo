/*
 * Copyright (c) 2010-2011, University of Sussex
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * 
 *  * Redistributions of source code must retain the above copyright notice, 
 *    this list of conditions and the following disclaimer.
 * 
 *  * Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 *  * Neither the name of the University of Sussex nor the names of its 
 *    contributors may be used to endorse or promote products derived from this 
 *    software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */
package uk.ac.susx.mlcl.byblo.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.Closeable;
import java.io.IOException;
import uk.ac.susx.mlcl.lib.Checks;
import uk.ac.susx.mlcl.lib.io.FileFactory;
import uk.ac.susx.mlcl.lib.io.IOUtil;
import uk.ac.susx.mlcl.lib.io.Sink;
import uk.ac.susx.mlcl.lib.io.TempFileFactory;
import java.io.File;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.susx.mlcl.byblo.io.EntryFeature;
import uk.ac.susx.mlcl.byblo.io.EntryFeatureSink;
import uk.ac.susx.mlcl.byblo.io.EntryFeatureSource;
import uk.ac.susx.mlcl.lib.ObjectIndex;
import uk.ac.susx.mlcl.lib.command.AbstractCommand;
import uk.ac.susx.mlcl.lib.io.SinkFactory;
import uk.ac.susx.mlcl.lib.io.Source;
import uk.ac.susx.mlcl.lib.io.TempFileFactoryConverter;
import uk.ac.susx.mlcl.lib.tasks.ChunkTask;

/**
 *
 * @author Hamish Morgan &lt;hamish.morgan@sussex.ac.uk%gt;
 */
@Parameters(commandDescription = "Split a large file into a number of smaller files.")
public class ChunkEFCommand extends AbstractCommand {

    private static final Log LOG = LogFactory.getLog(ChunkEFCommand.class);

    public static final int DEFAULT_MAX_CHUNK_SIZE = 5000000;

    @Parameter(names = {"-T", "--temporary-directory"},
               description = "Directory used for holding temporary files.",
               converter = TempFileFactoryConverter.class)
    private FileFactory chunkFileFactory =
            new TempFileFactory();

    @Parameter(names = {"-C", "--max-chunk-size"},
               description = "Number of lines that will be read into RAM at one time (per thread). Larger values increase memory usage and performace.")
    private int maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;

    @Parameter(names = {"-i", "--input-file"},
               description = "Source file. If this argument is not given, or if it is \"-\", then stdin will be read.")
    private File sourceFile;

    private BlockingQueue<File> dstFileQueue = new LinkedBlockingDeque<File>();

    @Parameter(names = {"-c", "--charset"},
               description = "Character encoding to use.")
    private Charset charset = IOUtil.DEFAULT_CHARSET;

    public ChunkEFCommand() {
    }

    public ChunkEFCommand(File srcFile, Charset charset) {
        setSrcFile(srcFile);
        setCharset(charset);
    }

    public ChunkEFCommand(File srcFile, Charset charset, int maxChunkSize) {
        setSrcFile(srcFile);
        setMaxChunkSize(maxChunkSize);
        setCharset(charset);
    }

    public final void setMaxChunkSize(int maxChunkSize) {
        Checks.checkRangeIncl(maxChunkSize, 1, Integer.MAX_VALUE);
        this.maxChunkSize = maxChunkSize;
    }

    public final Charset getCharset() {
        return charset;
    }

    public final void setCharset(Charset charset) {
        Checks.checkNotNull(charset);
        if(!charset.canEncode())
            throw new IllegalArgumentException("Charset " + charset + " cannot encode.");
        this.charset = charset;
    }

    public int getMaxChunkSize() {
        return maxChunkSize;
    }

    public final void setSrcFile(File sourceFile) {
        if (sourceFile == null)
            throw new NullPointerException("sourceFile is null");
        this.sourceFile = sourceFile;
    }

    public File getSrcFile() {
        return sourceFile;
    }

    public Collection<File> getDestFiles() {
        return Collections.unmodifiableCollection(dstFileQueue);
    }

    public FileFactory getChunkFileFactory() {
        return chunkFileFactory;
    }

    public void setChunkFileFactory(FileFactory chunkFileFactory) {
        Checks.checkNotNull("chunkFileFactory", chunkFileFactory);
        this.chunkFileFactory = chunkFileFactory;
    }

//    public BlockingQueue<File> getDstFileQueue() {
//        return dstFileQueue;
//    }
//
//    public void setDstFileQueue(BlockingQueue<File> dstFileQueue) {
//        if (dstFileQueue == null)
//            throw new NullPointerException("dstFileQueue is null");
//        this.dstFileQueue = dstFileQueue;
//    }
//
    @Override
    public void run() throws Exception {
        if (LOG.isInfoEnabled())
            LOG.info("Chunking from file \"" + sourceFile
                    + "\" to " + chunkFileFactory
                    + "; max-chunk-size=" + getMaxChunkSize() 
                    + ".");

        final ObjectIndex<String> entryIndex = new ObjectIndex<String>();
        final ObjectIndex<String> featureIndex = new ObjectIndex<String>();
        final Source<EntryFeature> src = new EntryFeatureSource(
                sourceFile, charset, entryIndex, featureIndex);

        final SinkFactory<EntryFeature> sinkFactory = new SinkFactory<EntryFeature>() {

            @Override
            public Sink<EntryFeature> getSink() throws IOException {
                File sinkFile = chunkFileFactory.createFile();
                dstFileQueue.add(sinkFile);
                Sink<EntryFeature> sink = new EntryFeatureSink(
                        sinkFile, charset, entryIndex, featureIndex);
                return sink;
            }
        };

        ChunkTask<EntryFeature> chunkTask = new ChunkTask<EntryFeature>(
                src, sinkFactory);
        chunkTask.setMaxChunkSize(maxChunkSize);

        chunkTask.run();

        if (src instanceof Closeable)
            ((Closeable) src).close();
        
    }
    
}
