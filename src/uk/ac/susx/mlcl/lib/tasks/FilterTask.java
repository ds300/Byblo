/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.susx.mlcl.lib.tasks;

import com.google.common.base.Predicate;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.Comparator;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static uk.ac.susx.mlcl.lib.Predicates2.*;
import uk.ac.susx.mlcl.lib.io.Sink;
import uk.ac.susx.mlcl.lib.io.Source;

/**
 *
 * @author hiam20
 */
public class FilterTask<T> extends CopyTask<T> {

    private static final Log LOG = LogFactory.getLog(FilterTask.class);

    private Predicate<T> accept = alwaysTrue();

    private Set<T> acceptedEntries = new ObjectOpenHashSet<T>();

    private Set<T> rejectedEntries = new ObjectOpenHashSet<T>();

    public FilterTask() {
    }

    public FilterTask(Source<T> source, Sink<T> sink) {
        super(source, sink);
    }

    public FilterTask(Source<T> source, Sink<T> sink,
            Comparator<T> comparator, Predicate<T> accept) {
        super(source, sink, comparator);
    }

    public Predicate<T> getAccept() {
        return accept;
    }

    public void setAccept(Predicate<T> acceptPredicate) {
        if (!acceptPredicate.equals(this.accept)) {
            this.accept = acceptPredicate;
        }
    }

    @Override
    protected void runTask() throws Exception {
        if (LOG.isInfoEnabled())
            LOG.info("Running FilterTask from \"" + getSource()
                    + "\" to \"" + getSink() + "\".");

        while (getSource().hasNext()) {
            final T record = getSource().read();
            if (accept.apply(record)) {
                getSink().write(record);
                acceptedEntries.add(record);
            } else {
                rejectedEntries.add(record);
            }
        }
    }

    public long getNumAccepted() {
        return acceptedEntries.size();
    }

    public long getNumRejected() {
        return rejectedEntries.size();
    }
}
