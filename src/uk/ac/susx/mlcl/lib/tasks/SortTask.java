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

import uk.ac.susx.mlcl.lib.Checks;
import uk.ac.susx.mlcl.lib.io.IOUtil;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.susx.mlcl.lib.io.Sink;
import uk.ac.susx.mlcl.lib.io.Source;
import uk.ac.susx.mlcl.lib.tasks.AbstractTask;

/**
 * Task that takes a single input file and sorts it according to some comparator,
 * then writes the results to an output file.
 * 
 * @author Hamish Morgan &lt;hamish.morgan@sussex.ac.uk%gt;
 */
public class SortTask<T> extends AbstractTask {

    private static final Log LOG = LogFactory.getLog(SortTask.class);

    private Source<T> source;

    private Sink<T> sink;

    private Comparator<T> comparator = null;

    private Charset charset = IOUtil.DEFAULT_CHARSET;

    public SortTask(Source<T> source, Sink<T> sink,
            Comparator<T> comparator, Charset charset) {
        setCharset(charset);
        setComparator(comparator);
        setSource(source);
        setSink(sink);
    }

    public SortTask(Source<T> source, Sink<T> sink) {
        setSource(source);
        setSink(sink);
    }

    public SortTask() {
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

    public final Source<T> getSource() {
        return source;
    }

    public final void setSource(Source<T> sourceA) {
        Checks.checkNotNull(sourceA);
        this.source = sourceA;
    }

    public final Charset getCharset() {
        return charset;
    }

    public final void setCharset(Charset charset) {
        Checks.checkNotNull(charset);
        this.charset = charset;
    }

    @Override
    protected void runTask() throws Exception {
        if (LOG.isInfoEnabled())
            LOG.info("Sorting file in memory, from \"" + getSource()
                    + "\" to \"" + getSink() + "\".");

        List<T> list = IOUtil.readAll(getSource());
        Collections.sort(list, getComparator());
        IOUtil.writeAll(list, getSink());
    }

}
