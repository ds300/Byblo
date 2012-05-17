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
 * POSSIBILITY OF SUCH DAMAGE.To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.susx.mlcl.byblo.enumerators;

import java.io.File;
import java.io.IOException;

/**
 *
 * @author Hamish I A Morgan &lt;hamish.morgan@sussex.ac.uk&gt;
 */
public interface DoubleEnumerating extends Enumerating {

    Enumerator<String> getEntryEnumerator() throws IOException;

    Enumerator<String> getFeatureEnumerator() throws IOException;

    File getEntryEnumeratorFile();

    File getFeatureEnumeratorFile();

    void setEntryEnumeratorFile(File entryEnumeratorFile);

    void setFeatureEnumeratorFile(File featureEnumeratorFile);

    void openEntriesEnumerator() throws IOException;

    void saveEntriesEnumerator() throws IOException;

    void closeEntriesEnumerator() throws IOException;

    void openFeaturesEnumerator() throws IOException;

    void saveFeaturesEnumerator() throws IOException;

    void closeFeaturesEnumerator() throws IOException;

    SingleEnumerating getEntriesEnumeratorCarriar();

    SingleEnumerating getFeaturesEnumeratorCarriar();

    boolean isEnumeratedEntries();

    void setEnumeratedEntries(boolean enumeratedEntries);

    boolean isEnumeratedFeatures();

    void setEnumeratedFeatures(boolean enumeratedFeatures);

}