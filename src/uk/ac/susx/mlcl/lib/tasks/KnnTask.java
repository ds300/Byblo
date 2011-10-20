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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.susx.mlcl.lib.Checks;
import uk.ac.susx.mlcl.lib.io.IOUtil;
import uk.ac.susx.mlcl.lib.io.Sink;
import uk.ac.susx.mlcl.lib.io.Source;

/**
 * Task that read in a file and produces the k-nearest-neighbors for each base 
 * entry. Assumes the file is composed of entry, entry, weight triples that are
 * delimited by tabs.
 * 
 * @param <T> 
 * @author Hamish Morgan &lt;hamish.morgan@sussex.ac.uk%gt;
 */
public class KnnTask<T> extends AbstractPipeTask<T> {

    private static final Log LOG = LogFactory.getLog(KnnTask.class);

    public static final int DEFAULT_K = 100;

    private int k = DEFAULT_K;

    private Comparator<T> boundryComparator;

    private Comparator<T> distanceComparator;

    public KnnTask(Source<T> source, Sink<T> sink,
                   int k, Comparator<T> distanceComparator,
                   Comparator<T> boundryComparator) {
        super(source, sink);
        setDistanceComparator(distanceComparator);
        setBoundryComparator(boundryComparator);
        setSource(source);
        setSink(sink);
        setK(k);
    }

    public KnnTask(Source<T> source, Sink<T> sink) {
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

    public final Comparator<T> getBoundryComparator() {
        return boundryComparator;
    }

    public final void setBoundryComparator(Comparator<T> boundryComparator) {
        this.boundryComparator = boundryComparator;
    }

    public final Comparator<T> getDistanceComparator() {
        return distanceComparator;
    }

    public final void setDistanceComparator(Comparator<T> distanceComparator) {
        this.distanceComparator = distanceComparator;
    }

    @Override
    protected void runTask() throws Exception {
        if (LOG.isInfoEnabled())
            LOG.info("Running K-Nearest-Neighbours from \"" 
                    + getSource() + "\" to \"" + getSink() + "\".");

        final List<T> list = IOUtil.readAll(getSource());


        Collections.sort(list, combinedComparator(getBoundryComparator(),
                                                  getDistanceComparator()));
        T cluster = null;
        int count = 0;

        for (T record : list) {
            if (cluster == null || boundryComparator.compare(record, cluster) != 0) {
                cluster = record;
                count = 0;
            }
            ++count;

            if (count <= getK()) {
                getSink().write(record);
            }
        }

        IOUtil.writeAll(list, getSink());
    }

    protected static <T> Comparator<T> combinedComparator(
            final Comparator<T> a, final Comparator<T> b) {
        return new Comparator<T>() {

            @Override
            public int compare(T x, T y) {
                int c = a.compare(x, y);
                return c != 0 ? c : b.compare(x, y);
            }
        };
    }
}
