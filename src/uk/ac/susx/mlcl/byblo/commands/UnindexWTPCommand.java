/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.susx.mlcl.byblo.commands;

import uk.ac.susx.mlcl.byblo.io.IndexDeligatePair;
import com.beust.jcommander.ParametersDelegate;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import uk.ac.susx.mlcl.byblo.io.*;
import uk.ac.susx.mlcl.lib.Checks;
import uk.ac.susx.mlcl.lib.io.Sink;
import uk.ac.susx.mlcl.lib.io.Source;

/**
 *
 * @author hiam20
 */
public class UnindexWTPCommand extends AbstractCopyCommand<Weighted<TokenPair>> {

    @ParametersDelegate
    private IndexDeligatePair indexDeligate = new IndexDeligatePair();

    public UnindexWTPCommand(
            File sourceFile, File destinationFile, Charset charset,
            File indexFile1, File indexFile2) {
        super(sourceFile, destinationFile, charset);
        indexDeligate.setIndexFile1(indexFile1);
        indexDeligate.setIndexFile2(indexFile2);
    }

    public UnindexWTPCommand() {
        super();
    }

    @Override
    public void runCommand() throws Exception {
        Checks.checkNotNull("indexFile1", indexDeligate.getIndexFile1());
        Checks.checkNotNull("indexFile2", indexDeligate.getIndexFile2());
        super.runCommand();
    }

    @Override
    protected Source<Weighted<TokenPair>> openSource(File file)
            throws FileNotFoundException, IOException {
        IndexDeligatePair dstIdx = new IndexDeligatePair(true, true);
        dstIdx.setSkipIndexed1(indexDeligate.isSkipIndexed1());
        dstIdx.setSkipIndexed2(indexDeligate.isSkipIndexed2());
        return WeightedTokenPairSource.open(
                file, getFilesDeligate().getCharset(),
                dstIdx);
    }

    @Override
    protected Sink<Weighted<TokenPair>> openSink(File file)
            throws FileNotFoundException, IOException {
        IndexDeligatePair srcIdx = new IndexDeligatePair(
                false, false,
                indexDeligate.getEnumerator1(),
                indexDeligate.getEnumerator2(), false, false);
        return WeightedTokenPairSink.open(
                file, getFilesDeligate().getCharset(),
                srcIdx, true);
    }

    public IndexDeligatePair getIndexDeligate() {
        return indexDeligate;
    }

    public void setIndexDeligate(IndexDeligatePair indexDeligate) {
        this.indexDeligate = indexDeligate;
    }
}
