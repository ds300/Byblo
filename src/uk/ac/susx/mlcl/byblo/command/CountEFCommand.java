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
import uk.ac.susx.mlcl.byblo.io.EntryFeature;
import uk.ac.susx.mlcl.byblo.io.EntryFeatureSource;
import uk.ac.susx.mlcl.lib.Checks;
import uk.ac.susx.mlcl.lib.ObjectIndex;
import uk.ac.susx.mlcl.lib.io.IOUtil;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.susx.mlcl.byblo.io.WeightedEntryFeatureSink;
import uk.ac.susx.mlcl.lib.collect.Weighted;
import uk.ac.susx.mlcl.lib.io.Sink;
import uk.ac.susx.mlcl.lib.io.Source;
import uk.ac.susx.mlcl.lib.command.AbstractCommand;
import uk.ac.susx.mlcl.lib.tasks.CountUniqueTask;

/**
 * <p>Read in a raw feature instances file, to produce three frequency files:
 * entries, features, and entry-feature pairs.</p>
 *
 * @author Hamish Morgan &lt;hamish.morgan@sussex.ac.uk%gt;
 */
@Parameters(commandDescription = "Read in a raw feature instances file, to "
+ "produce three frequency files: entries, contexts, and features.")
public class CountEFCommand extends AbstractCommand {

    private static final Log LOG = LogFactory.getLog(CountEFCommand.class);

    @Parameter(names = {"-i", "--input"},
               required = true,
               description = "Source entry/feature instances file")
    private File inputFile;

    @Parameter(names = {"-o", "--output"},
               required = true,
               description = "entry/feature frequencies destination file")
    private File outputFile = null;

    @Parameter(names = {"-c", "--charset"},
               description = "Character encoding to use for input and output.")
    private Charset charset = IOUtil.DEFAULT_CHARSET;

    /**
     * Dependency injection constructor with all fields parameterised.
     *
     * @param instancesFile input file containing entry/context instances
     * @param entryFeaturesFile  output file for entry/context/frequency triples
     * @param charset       character set to use for all file I/O
     * @throws NullPointerException if any argument is null
     */
    public CountEFCommand(final File instancesFile,
                          final File entryFeaturesFile,
                          final Charset charset) throws NullPointerException {
        this(instancesFile, entryFeaturesFile);
        setCharset(charset);
    }

    /**
     * Minimal parameterisation constructor, with all fields that must be set
     * for the task to be functional. Character set will be set to software
     * default from {@link IOUtil#DEFAULT_CHARSET}.
     *
     * @param instancesFile input file containing entry/context instances
     * @param entryFeaturesFile  output file for entry/context/frequency triples
     * @throws NullPointerException if any argument is null
     */
    public CountEFCommand(
            final File instancesFile, final File entryFeaturesFile)
            throws NullPointerException {
        setInputFile(instancesFile);
        setOutputFile(entryFeaturesFile);
    }

    /**
     * Default constructor used by serialisation and JCommander instantiation.
     * All files will initially be set to null. Character set will be set to
     * software default from {@link IOUtil#DEFAULT_CHARSET}.
     */
    public CountEFCommand() {
    }

    @Override
    public void run() throws Exception {

        if (LOG.isInfoEnabled())
            LOG.info("Running " + this + ".");

        checkState();

        final Object2IntMap<EntryFeature> entryFeatureFreq =
                new Object2IntOpenHashMap<EntryFeature>();
        entryFeatureFreq.defaultReturnValue(0);

        final ObjectIndex<String> entryIndex = new ObjectIndex<String>();
        final ObjectIndex<String> featureIndex = new ObjectIndex<String>();

        final Source<EntryFeature> efSource = new EntryFeatureSource(
                inputFile, charset, entryIndex, featureIndex);

        final Sink<Weighted<EntryFeature>> efSink = new WeightedEntryFeatureSink(
                outputFile, charset, entryIndex, featureIndex);

        final CountUniqueTask<EntryFeature> cuTask =
                new CountUniqueTask<EntryFeature>(efSource, efSink);
        cuTask.run();

        if (LOG.isInfoEnabled())
            LOG.info("Completed " + this + ".");
    }

    public final File getOutputFile() {
        return outputFile;
    }

    public final void setOutputFile(final File entryFeaturesFile)
            throws NullPointerException {
        if (entryFeaturesFile == null)
            throw new NullPointerException("entryFeaturesFile is null");
        this.outputFile = entryFeaturesFile;
    }

    public File getInputFile() {
        return inputFile;
    }

    public final void setInputFile(final File inputFile)
            throws NullPointerException {
        if (inputFile == null)
            throw new NullPointerException("inputFile is null");
        this.inputFile = inputFile;
    }

    public final Charset getCharset() {
        return charset;
    }

    public final void setCharset(Charset charset) {
        Checks.checkNotNull(charset);
        this.charset = charset;
    }

    /**
     * Method that performance a number of sanity checks on the parameterisation
     * of this class. It is necessary to do this because the the class can be
     * instantiated via a null constructor when run from the command line.
     *
     * @throws NullPointerException
     * @throws IllegalStateException
     * @throws FileNotFoundException
     */
    private void checkState() throws NullPointerException, IllegalStateException, FileNotFoundException {
        // Check non of the parameters are null
        if (inputFile == null)
            throw new NullPointerException("inputFile is null");
        if (outputFile == null)
            throw new NullPointerException("entryFeaturesFile is null");
        if (charset == null)
            throw new NullPointerException("charset is null");

        // Check that no two files are the same
        if (inputFile.equals(outputFile))
            throw new IllegalStateException("inputFile == featuresFile");


        // Check that the instances file exists and is readable
        if (!inputFile.exists())
            throw new FileNotFoundException(
                    "instances file does not exist: " + inputFile);
        if (!inputFile.isFile())
            throw new IllegalStateException(
                    "instances file is not a normal data file: " + inputFile);
        if (!inputFile.canRead())
            throw new IllegalStateException(
                    "instances file is not readable: " + inputFile);

        // For each output file, check that either it exists and it writeable,
        // or that it does not exist but is creatable
        if (outputFile.exists() && (!outputFile.isFile() || !outputFile.canWrite()))
            throw new IllegalStateException(
                    "entry-features file exists but is not writable: " + outputFile);
        if (!outputFile.exists() && !outputFile.getAbsoluteFile().
                getParentFile().
                canWrite()) {
            throw new IllegalStateException(
                    "entry-features file does not exists and can not be reated: " + outputFile);
        }
    }

    @Override
    public String toString() {
        return "CountTask{"
                + "instancesFile=" + inputFile
                + ", entryFeaturesFile=" + outputFile
                + ", charset=" + charset
                + '}';
    }
}
