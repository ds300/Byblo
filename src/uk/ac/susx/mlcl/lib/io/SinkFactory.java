/*
 * Copyright (c) 2010, Hamish Morgan.
 * All Rights Reserved.
 */
package uk.ac.susx.mlcl.lib.io;

/**
 *
 * @author hamish
 * @param <T> 
 */
public interface SinkFactory<T> {

    public Sink<T> getSink() throws Exception;
    
}
