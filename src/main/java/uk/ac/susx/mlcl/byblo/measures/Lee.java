/*
 * Copyright (c) 2010-2012, University of Sussex
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
package uk.ac.susx.mlcl.byblo.measures;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.susx.mlcl.lib.collect.SparseDoubleVector;

/**
 * The alpha-skew divergence measure as defined in Lee (2001)
 *
 * A weighted measure of "information gain". Not a true metric,  but effectively
 * a distance measure.
 *
 * Lillian Lee (2001) "On the Effectiveness of the Skew Divergence for
 * Statistical Language Analysis" Artificial Intelligence and Statistics 2001,
 * pp. 65--72, 2001
 *
 * @author Hamish I A Morgan &lt;hamish.morgan@sussex.ac.uk&gt;
 */
public class Lee extends AbstractProximity {

    private static final Log LOG = LogFactory.getLog(Lee.class);

    public static final double DEFAULT_ALPHA = 0.99;

    private double alpha;

    public Lee() {
        this.alpha = DEFAULT_ALPHA;
        if (LOG.isWarnEnabled())
            LOG.warn("The Lee proximity measure has been thoughoughly test and "
                    + "is likely to contain bugs.");
    }

    public double getAlpha() {
        return alpha;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    @Override
    public double shared(SparseDoubleVector A, SparseDoubleVector B) {
        double sim = 0;
        int i = 0, j = 0;
        while (i < A.size && j < B.size) {
            if (A.keys[i] < B.keys[j]) {
                i++;
            } else if (A.keys[i] > B.keys[j]) {
                j++;
            } else if (isFiltered(A.keys[i])) {
                i++;
                j++;
            } else {
                final double pA = A.values[i] / A.sum;
                final double pB = (B.values[j] / B.sum);
                sim += pA * (2 * Math.log(pA)
                             - Math.log(pB * alpha + pA * (1 - alpha))
                             + Math.log((1.0 - alpha)));
                i++;
                j++;
            }
        }
        return sim;
    }

    @Override
    public double left(SparseDoubleVector A) {
        double left = 0;
        for (int i = 0; i < A.size; i++) {
            final double pA = A.values[i] / A.sum;
            left += pA * (Math.log(pA)
                          - Math.log(pA * (1.0 - alpha)));
        }
        return left;
    }

    @Override
    public double right(SparseDoubleVector B) {
        return 0;
    }

    @Override
    public double combine(double shared, double left, double right) {
        // Low values indicate similarity so invert it.
        return 1d / (shared + left);
    }

    @Override
    public boolean isSymmetric() {
        return false;
    }

    @Override
    public String toString() {
        return "Lee{" + "alpha=" + alpha + '}';
    }
}
