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
package uk.ac.susx.mlcl.lib.tasks;

import uk.ac.susx.mlcl.lib.io.SinkFactory;
import com.beust.jcommander.Parameters;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import uk.ac.susx.mlcl.lib.Checks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.susx.mlcl.lib.io.Sink;
import uk.ac.susx.mlcl.lib.io.Source;

/**
 *
 * @author Hamish Morgan &lt;hamish.morgan@sussex.ac.uk%gt;
 */
@Parameters(commandDescription = "Split a large file into a number of smaller files.")
public class ChunkTask<T> extends AbstractTask {

    private static final Log LOG = LogFactory.getLog(ChunkTask.class);

    public static final int DEFAULT_MAX_CHUNK_SIZE = 5000000;

    private Source<T> source;

    private SinkFactory<T> sinkFactory;

    private int maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;

    public ChunkTask(Source<T> source, SinkFactory<T> sinkFactory) {
        setSource(source);
        setSinkFactory(sinkFactory);
    }

    public ChunkTask() {
    }

    public final Source<T> getSource() {
        return source;
    }

    public final void setSource(Source<T> source) {
        Checks.checkNotNull("source", source);
        this.source = source;
    }

    public final SinkFactory<T> getSinkFactory() {
        return sinkFactory;
    }

    public final void setSinkFactory(SinkFactory<T> sinkFactory) {
        Checks.checkNotNull("sinkFactory", sinkFactory);
        this.sinkFactory = sinkFactory;
    }

    public int getMaxChunkSize() {
        return maxChunkSize;
    }

    public void setMaxChunkSize(int maxChunkSize) {
        Checks.checkRangeIncl(maxChunkSize, 1, Integer.MAX_VALUE);
        this.maxChunkSize = maxChunkSize;
    }

    @Override
    protected void runTask() throws Exception {
        if (LOG.isDebugEnabled())
            LOG.debug("Chunking from file \"" + getSource() + "\".");


        Sink<T> currentSink = null;

        int chunkSize = 0;
        while (source.hasNext()) {
            if (currentSink == null || chunkSize > maxChunkSize) {
                closeSink(currentSink);
                currentSink = sinkFactory.getSink();
                chunkSize = 0;
            }

            currentSink.write(source.read());
            ++chunkSize;
        }

        closeSink(currentSink);
    }

    private void closeSink(Sink<T> sink) throws IOException {
        if (sink != null) {
            if (sink instanceof Flushable)
                ((Flushable) sink).flush();
            if (sink instanceof Closeable)
                ((Closeable) sink).close();
        }

    }
}
