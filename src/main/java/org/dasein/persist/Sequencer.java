/**
 * Copyright (C) 1998-2011 enStratusNetworks LLC
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

/* $Id: Sequencer.java,v 1.2 2005/08/15 16:15:59 george Exp $ */
/* Copyright (c) 2002-2004 Valtira Corporation, All Rights Reserved */
package org.dasein.persist;

// J2SE imports
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// Apache imports
import org.apache.log4j.Logger;

/**
 * <p>
 * Provides a generic interface for sequence generation that may be
 * implemented by a number of different sequence providers. You request
 * sequencer instances by calling {@link #getInstance(String)} with the
 * name of the sequence or {@link #getInstance(Class)} with the class for which
 * the sequence should be unique. You may configure what sequencer is used through
 * the configuration file <i>dasein-persistence.properties</i>. You
 * specify a concrete sequencer using the following:<br/>
 * <code>dasein.sequencer.NAME=CLASSNAME</code>
 * </p>
 * <p>
 * For example:<br/>
 * <code>dasein.sequencer.pageId=org.dasein.persist.DaseinSequencer</code>
 * </p>
 * <p>
 * In code, you might have the following:
 * </p>
 * <p>
 * <code>
 * Sequencer seq = Sequencer.getInstance(this.class);<br/> 
 * id = seq.next();
 * </code>
 * </p>
 * <p>
 * Last modified $Date: 2005/08/15 16:15:59 $
 * </p>
 * @version $Revision: 1.2 $
 * @author George Reese
 */
public abstract class Sequencer {
    static public final String PROPERTIES = "/dasein-persistence.properties";
    
    static private final Logger logger = Logger.getLogger(Sequencer.class);

    /**
     * Class for the default sequencer, set to{@link DaseinSequencer}.class
     * by default.
     */
    static private final Class<? extends Sequencer> defaultSequencer;
    /**
     * All sequencers currently in memory.
     */
    static private final Map<String,Sequencer>      sequencers       = new HashMap<String,Sequencer>();

    /**
     * Loads the sequencers from the dasein-persistence.properties
     * configuration file.
     */
    static {
        Class<? extends Sequencer> def = DaseinSequencer.class;
        
        try {
            InputStream is = Sequencer.class.getResourceAsStream(PROPERTIES);
            Properties props = new Properties();
            Enumeration<?> propenum;

            if( is != null ) {
                props.load(is);
            }
            propenum = props.propertyNames();
            while( propenum.hasMoreElements() ) {
                String nom = (String)propenum.nextElement();

                if( nom.startsWith("dasein.sequencer.") ) {
                    String[] parts = nom.split("\\.");
                    String val;

                    val = props.getProperty(nom);
                    nom = parts[2];
                    try {
                        if( nom.equalsIgnoreCase("default") ) {
                            @SuppressWarnings("unchecked") Class<? extends Sequencer> seq = (Class<? extends Sequencer>)Class.forName(val);
                            
                            def = seq;
                        }
                        else {
                            Sequencer seq;

                            seq = (Sequencer)Class.forName(val).newInstance();
                            seq.setName(nom);
                            sequencers.put(nom, seq);
                        }
                    }
                    catch( Exception ignore ) {
                        /* ignore */
                    }
                }
            }
        }
        catch( Exception e ) {
            logger.error(e.getMessage(), e);
        }
        defaultSequencer = def;
    }
    
    /**
     * Looks to see if a sequencer has been generated for the sequence
     * with the specified name. If not, it will instantiate one.
     * Multiple calls to this method with the same name are guaranteed
     * to receive the same sequencer object. For best performance,
     * classes should save a reference to the sequencer once they get it
     * in order to avoid the overhead of a <code>HashMap</code> lookup.
     * @param name the name of the desired sequencer
     * @return the sequencer with the specified name
     */
    static public final Sequencer getInstance(String name) {
        logger.debug("enter - getInstance()");
        try {
            Sequencer seq = null;
            
            if( sequencers.containsKey(name) ) {
                seq = sequencers.get(name);
            }
            if( seq != null ) {
                return seq;
            }
            synchronized( sequencers ) {
                // redundant due to the non-synchronized calls above done for performance
                if( !sequencers.containsKey(name) ) {
                    try {
                        seq = defaultSequencer.newInstance();
                    }
                    catch( Exception e ) {
                        logger.error(e.getMessage(), e);
                        return null;
                    }
                    seq.setName(name);
                    sequencers.put(name, seq);
                    return seq;
                }
                else {
                    return sequencers.get(name);
                }
            }
        }
        finally {
            logger.debug("exit - getInstance()");
        }
    }

    /**
     * Returns a sequencer that will provide unique identifiers across all instances
     * of the specified class. This method is simply a convenience method for calling
     * {@link #getInstance(String)} where the name being used for the desired
     * sequence is the name of the class passed to this method.
     * @param cls the class for which unique IDs are desired
     * @return a sequencer that generates unique IDs for the specified class
     */
    static public Sequencer getInstance(Class<?> cls) {
        return getInstance(cls.getName());
    }
    
    /**
     * The name of this sequencer.
     */
    private String name     = null;

    /**
     * Constructs an empty sequencer with no name. This constructor
     * should be followed by a call to {@link #setName(String)} before
     * the sequencer is used.
     */
    public Sequencer() {
        super();
    }
    
    /**
     * @return the name of the sequencer
     */
    public String getName() {
        return name;
    }
    
    /**
     * Generates a new unique number based on the implementation class'
     * algorithm of choice. The generated number must be a Java long
     * that is guaranteed to be unique for all sequences sharing this
     * name.
     * @return a unique number for the sequence of this sequencer's name
     * @throws org.dasein.persist.PersistenceException a data store error
     * occurred while generating the number
     */
    public abstract long next() throws PersistenceException;

    /**
     * Sets the sequencer name.
     * @param nom the name of the sequencer
     */
    public final void setName(String nom) {
        name = nom;
    }
}
