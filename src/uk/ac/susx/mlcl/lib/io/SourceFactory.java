/*
 * Copyright (c) 2010, Hamish Morgan.
 * All Rights Reserved.
 */
package uk.ac.susx.mlcl.lib.io;

/**
 *
 * @param <T> 
 * @author hamish
 */
public interface SourceFactory<T> {

    public Source<T> getSource();
}
