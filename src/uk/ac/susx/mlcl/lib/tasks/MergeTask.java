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

import java.io.Closeable;
import uk.ac.susx.mlcl.lib.Checks;
import uk.ac.susx.mlcl.lib.tasks.AbstractTask;
import java.io.Flushable;
import java.nio.charset.Charset;
import java.util.Comparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.susx.mlcl.lib.io.IOUtil;
import uk.ac.susx.mlcl.lib.io.Sink;
import uk.ac.susx.mlcl.lib.io.Source;

/**
 * Merges the contents of two sorted source files, line by line, into a
 * destination file.
 *
 * The source files are assumed to already be ordered according to the
 * comparator.
 * 
 * Any file denoted by the name string "-" is assumed to be standard-in in the
 * case of source files, and standard out in the case of destination files..
 *
 * @param <T> 
 * @author Hamish Morgan &lt;hamish.morgan@sussex.ac.uk%gt;
 */
public class MergeTask<T extends Comparable<? super T>>
        extends AbstractTask {

    private static final Log LOG = LogFactory.getLog(MergeTask.class);

    private Source<T> sourceA;

    private Source<T> sourceB;

    private Sink<T> sink;

    private Comparator<T> comparator = null;

    public MergeTask(Source<T> srcA, Source<T> srcB, Sink<T> sink,
                     Comparator<T> comparator) {
        setComparator(comparator);
        setSourceA(sourceA);
        setSourceB(sourceB);
        setSink(sink);
    }

    public MergeTask(Source<T> sourceA,
                     Source<T> sourceB,
                     Sink<T> sink) {
        setSourceA(sourceA);
        setSourceB(sourceB);
        setSink(sink);
    }

    public MergeTask() {
    }

    public final Comparator<T> getComparator() {
        return comparator;
    }

    /**
     * If set to null the use the natural ordering of the items.
     * @param comparator 
     */
    public final void setComparator(Comparator<T> comparator) {
        this.comparator = comparator;
    }

    public final Sink<T> getSink() {
        return sink;
    }

    public final void setSink(Sink<T> sink) {
        Checks.checkNotNull(sink);
        this.sink = sink;
    }

    public final Source<T> getSourceA() {
        return sourceA;
    }

    public final void setSourceA(Source<T> sourceA) {
        Checks.checkNotNull(sourceA);
        this.sourceA = sourceA;
    }

    public final Source<T> getSourceB() {
        return sourceB;
    }

    public final void setSourceB(Source<T> sourceB) {
        Checks.checkNotNull(sourceB);
        this.sourceB = sourceB;
    }

    @Override
    protected void runTask() throws Exception {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Merging from files \"" + getSourceA()
                        + "\" and \"" + getSourceB() + "\" to \""
                        + getSink() + "\".");
            }

            T nextA = sourceA.hasNext() ? sourceA.read() : null;
            T nextB = sourceB.hasNext() ? sourceB.read() : null;
            while (nextA != null && nextB != null) {

                final int comp = comparator == null
                        ? nextA.compareTo(nextB)
                        : comparator.compare(nextA, nextB);

                if (comp < 0) {
                    sink.write(nextA);
                    nextA = sourceA.hasNext() ? sourceA.read() : null;
                } else if (comp > 0) {
                    sink.write(nextB);
                    nextB = sourceB.hasNext() ? sourceB.read() : null;
                } else {
                    sink.write(nextA);
                    sink.write(nextB);
                    nextA = sourceA.hasNext() ? sourceA.read() : null;
                    nextB = sourceB.hasNext() ? sourceB.read() : null;
                }
            }
            while (nextA != null) {
                sink.write(nextA);
                nextA = sourceA.hasNext() ? sourceA.read() : null;
            }
            while (nextB != null) {
                sink.write(nextB);
                nextB = sourceB.hasNext() ? sourceB.read() : null;
            }
        } finally {
            if (sink instanceof Flushable)
                ((Flushable) sink).flush();
            if (sink instanceof Closeable)
                ((Closeable) sink).close();
        }
    }
}
