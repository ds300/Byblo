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
package uk.ac.susx.mlcl.byblo.tasks;

import uk.ac.susx.mlcl.lib.tasks.SortTask;
import java.nio.charset.Charset;
import java.util.Comparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.susx.mlcl.byblo.io.WeightedEntryPairRecord;
import uk.ac.susx.mlcl.lib.Checks;
import uk.ac.susx.mlcl.lib.io.Sink;
import uk.ac.susx.mlcl.lib.io.Source;

/**
 * Task that read in a file and produces the k-nearest-neighbors for each base 
 * entry. Assumes the file is composed of entry, entry, weight triples that are
 * delimited by tabs.
 * 
 * @author Hamish Morgan &lt;hamish.morgan@sussex.ac.uk%gt;
 */
public class KnnTask extends SortTask<WeightedEntryPairRecord> {

    private static final Log LOG = LogFactory.getLog(KnnTask.class);

    public static final int DEFAULT_K = 100;

    private int k = DEFAULT_K;

    public KnnTask(Source<WeightedEntryPairRecord> source,
            Sink<WeightedEntryPairRecord> sink,
            Comparator<WeightedEntryPairRecord> comparator,
            Charset charset, int k) {
        setCharset(charset);
        setComparator(comparator);
        setSource(source);
        setSink(sink);
        setK(k);
    }

    public KnnTask(Source<WeightedEntryPairRecord> source,
            Sink<WeightedEntryPairRecord> sink) {
        setSource(source);
        setSink(sink);
    }

    public KnnTask() {
    }

    public final int getK() {
        return k;
    }

    public final void setK(int k) {
        Checks.checkRangeIncl(k, 1, Integer.MAX_VALUE);
        this.k = k;
    }

    @Override
    protected void runTask() throws Exception {
        if (LOG.isInfoEnabled())
            LOG.info("Running K-Nearest-Neighbours from \"" + getSource()
                    + "\" to \"" + getSink() + "\".");


        int currentBaseEntry = -1;
        int currentCount = -1;
        while (getSource().hasNext()) {
            WeightedEntryPairRecord record = getSource().read();
            if (record.getEntry1Id() != currentBaseEntry) {
                currentBaseEntry = record.getEntry1Id();
                currentCount = 1;
            } else {
                currentCount++;
            }

            if (currentCount <= getK()) {
                getSink().write(record);
            }
        }
    }
}
