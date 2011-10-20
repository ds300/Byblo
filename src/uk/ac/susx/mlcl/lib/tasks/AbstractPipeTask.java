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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.susx.mlcl.lib.io.Sink;
import uk.ac.susx.mlcl.lib.io.Source;

/**
 * Task that takes a single input file and sorts it according to some comparator,
 * then writes the results to an output file.
 * 
 * @param <T> 
 * @author Hamish Morgan &lt;hamish.morgan@sussex.ac.uk%gt;
 */
public abstract class AbstractPipeTask<T> extends AbstractTask {

    private static final Log LOG = LogFactory.getLog(AbstractPipeTask.class);

    private Source<T> source;

    private Sink<T> sink;

    public AbstractPipeTask(Source<T> source, Sink<T> sink) {
        setSource(source);
        setSink(sink);
    }

    public AbstractPipeTask() {
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

    public final void setSource(Source<T> source) {
        Checks.checkNotNull(source);
        this.source = source;
    }

    @Override
    protected abstract void runTask() throws Exception;
}