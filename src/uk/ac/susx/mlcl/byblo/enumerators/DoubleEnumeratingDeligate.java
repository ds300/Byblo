/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.susx.mlcl.byblo.enumerators;

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import uk.ac.susx.mlcl.lib.commands.InputFileValidator;

/**
 *
 * @author hiam20
 */
public class DoubleEnumeratingDeligate
        extends EnumeratingDeligate
        implements Serializable, DoubleEnumerating {

    private static final long serialVersionUID = 1L;

    @Parameter(names = {"-Ee", "--enumerated-entries"},
    description = "Whether tokens in the first column of the input file are indexed.")
    private boolean enumeratedEntries = DEFAULT_IS_ENUMERATED;

    @Parameter(names = {"-Ef", "--enumerated-features"},
    description = "Whether entries in the second column of the input file are indexed.")
    private boolean enumeratedFeatures = DEFAULT_IS_ENUMERATED;

    @Parameter(names = {"-Xe", "--entries-index-file"},
    description = "Index file for enumerating entries.",
    validateWith = InputFileValidator.class)
    private File entriesIndexFile = null;

    @Parameter(names = {"-Xf", "--features-index-file"},
    description = "Index file for enumerating features.",
    validateWith = InputFileValidator.class)
    private File featuresIndexFile = null;

    private Enumerator<String> entryEnumerator = null;

    private Enumerator<String> featureEnumerator = null;

    protected DoubleEnumeratingDeligate(
            EnumeratorType type, boolean enumeratedEntries, boolean enumeratedFeatures,
            File entryIndexFile, File featureIndexFile,
            Enumerator<String> entryEnumerator, Enumerator<String> featureEnumerator,
            boolean skipindexed1, boolean skipindexed2) {
        super(type, skipindexed1, skipindexed2);
        this.enumeratedEntries = enumeratedEntries;
        this.enumeratedFeatures = enumeratedFeatures;
        this.entriesIndexFile = entryIndexFile;
        this.featuresIndexFile = featureIndexFile;
        this.entryEnumerator = entryEnumerator;
        this.featureEnumerator = featureEnumerator;
    }

    public DoubleEnumeratingDeligate(
            EnumeratorType type, boolean enumeratedEntries, boolean enumeratedFeatures,
            File entryIndexFile, File featureIndexFile,
            boolean skipIndexed1, boolean skipIndexed2) {
        this(type, enumeratedEntries, enumeratedFeatures, entryIndexFile, featureIndexFile,
             null, null, skipIndexed1, skipIndexed2);
    }
//
//    public DoubleEnumeratingDeligate(
//            EnumeratorType type, boolean enumeratedEntries, boolean enumeratedFeatures,
//            Enumerator<String> entryEnumerator, Enumerator<String> featureEnumerator,
//            boolean skipIndexed1, boolean skipIndexed2) {
//        this(type, enumeratedEntries, enumeratedFeatures, null, null,
//             entryEnumerator, featureEnumerator, skipIndexed1, skipIndexed2);
//    }
//
//    public DoubleEnumeratingDeligate(
//            EnumeratorType type, boolean enumeratedEntries,
//            boolean enumeratedFeatures,
//            boolean skipIndexed1,
//            boolean skipIndexed2) {
//        this(type, enumeratedEntries, enumeratedFeatures, null, null,
//             null, null, skipIndexed1, skipIndexed2);
//    }
//
//    public DoubleEnumeratingDeligate(
//            EnumeratorType type, boolean enumeratedEntries,
//            boolean enumeratedFeatures) {
//        this(type, enumeratedEntries, enumeratedFeatures, null, null, null, null,
//             DEFAULT_SKIP_INDEXING, DEFAULT_SKIP_INDEXING);
//    }
//
//    public DoubleEnumeratingDeligate(
//            EnumeratorType type, boolean enumeratedEntries,
//                                     boolean enumeratedFeatures,
//                                     Enumerator<String> entryEnumerator,
//                                     Enumerator<String> featureEnumerator) {
//        this(type, enumeratedEntries, enumeratedFeatures, null, null,
//             entryEnumerator, featureEnumerator,
//             DEFAULT_SKIP_INDEXING, DEFAULT_SKIP_INDEXING);
//    }
//
    public DoubleEnumeratingDeligate() {
        this(DEFAULT_TYPE, DEFAULT_IS_ENUMERATED, DEFAULT_IS_ENUMERATED,
             null, null,
             DEFAULT_SKIP_INDEXING, DEFAULT_SKIP_INDEXING);
    }

    @Override
    public final Enumerator<String> getEntryEnumerator() throws IOException {
        if (entryEnumerator == null) {
            // if tokens are preindexed then a file MUST be available
            // otherwise the file will be loaded if it exists
            openEntriesEnumerator();
//            entryEnumerator = EnumeratorDeligates.instantiateEnumerator(
//                    isEntriesEnumerated(), getEntryIndexFile());
        }
        return entryEnumerator;
    }

    @Override
    public final Enumerator<String> getFeatureEnumerator() throws IOException {
        if (featureEnumerator == null) {
            openFeaturesEnumerator();
//            featureEnumerator = EnumeratorDeligates.instantiateEnumerator(
//                    isFeaturesEnumerated(), getFeatureIndexFile());
        }
        return featureEnumerator;
    }

    @Override
    public final boolean isEntriesEnumerated() {
        return enumeratedEntries;
    }

    @Override
    public final boolean isFeaturesEnumerated() {
        return enumeratedFeatures;
    }

    @Override
    public final File getEntryEnumeratorFile() {
        return entriesIndexFile;
    }

    @Override
    public final File getFeatureEnumeratorFile() {
        return featuresIndexFile;
    }

    @Override
    public void openEntriesEnumerator() throws IOException {
        entryEnumerator = open(entriesIndexFile);;
    }

    @Override
    public void saveEntriesEnumerator() throws IOException {
        save(entryEnumerator);
    }

    @Override
    public void closeEntriesEnumerator() throws IOException {
        close(entryEnumerator);
        entryEnumerator = null;
    }

    @Override
    public void openFeaturesEnumerator() throws IOException {
        featureEnumerator = open(featuresIndexFile);
    }

    @Override
    public void saveFeaturesEnumerator() throws IOException {
        save(featureEnumerator);
    }

    @Override
    public void closeFeaturesEnumerator() throws IOException {
        close(featureEnumerator);
        featureEnumerator = null;
    }

    public void closeEnumerator() throws IOException {
        closeFeaturesEnumerator();
        closeEntriesEnumerator();
    }

    @Override
    public void saveEnumerator() throws IOException {
        saveEntriesEnumerator();
        saveFeaturesEnumerator();
    }

    @Override
    public void openEnumerator() throws IOException {
        openEntriesEnumerator();
        openFeaturesEnumerator();
    }

    @Override
    protected Objects.ToStringHelper toStringHelper() {
        return super.toStringHelper().
                add("preindexed1", isEntriesEnumerated()).
                add("preindexed2", isFeaturesEnumerated()).
                add("index1", getEntryEnumeratorFile()).
                add("index2", getFeatureEnumeratorFile());
    }

    @Override
    public SingleEnumerating getEntriesEnumeratorCarriar() {
        return EnumeratingDeligates.toSingleEntries(this);
    }

    @Override
    public SingleEnumerating getFeaturesEnumeratorCarriar() {
        return EnumeratingDeligates.toSingleFeatures(this);
    }

}
