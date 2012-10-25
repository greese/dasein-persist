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

/* $Id: Memento.java,v 1.5 2006/02/18 22:47:10 greese Exp $ */
/* Copyright (c) 2003-2004 Valtira Corporation, All Rights Reserved */
package org.dasein.persist;

// J2SE imports
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

// Apache imports
import org.apache.log4j.Logger;

/**
 * Provides state translation for persistent objects. By capturing
 * the state information automatically in a hash map, this class
 * decouples the implementation from its data structures in the persistent
 * store.
 * <br/>
 * Last modified: $Date: 2006/02/18 22:47:10 $
 * @version $Revision: 1.5 $
 * @author George Reese (http://george.reese.name)
 */
public class Memento<K> {
    static private final Logger logger = Logger.getLogger(Memento.class);

    /**
     * The persistent object whose state is being managed by this memento.
     */
    private K                                persistent = null;
    /**
     * The state of the managed object.
     */
    private HashMap<String,Object> state      = null;

    /**
     * Constructs a memento for the specified persistent object.
     * @param p the persistent object
     */
    public Memento(K p) {
        super();
        persistent = p;
    }

    /**
     * @return the state of this object as of its last call to save()
     */
    public Map<String, Object> getState() {
        return state;
    }

    /**
     * Loads the data values into the persistent object.
     * @param data the data to load into the persistent object
     * @throws org.dasein.persist.PersistenceException values from
     * the data map could not be loaded into the persistent object
     */
    public void load(Map<String, Object> data) throws PersistenceException {
        logger.debug("enter - load(Map)");
        try {
            for( String key : data.keySet() ) {
                Class<?> cls = persistent.getClass();
                Object val = data.get(key);
                Field f = null;
                
                while( f == null ) {
                    try {
                        f = cls.getDeclaredField(key);
                    }
                    catch( NoSuchFieldException ignore ) {
                        // ignore
                    }
                    if( f == null ) {
                        cls = cls.getSuperclass();
                        if( cls == null || cls.getName().equals(Object.class.getName()) ) {
                            break;
                        }
                    }
                }
                if( f == null ) {
                    logger.debug("No such field: " + key);
                    continue;
                }
                try {
                    f.setAccessible(true);
                    f.set(persistent, val);
                }
                catch( IllegalAccessException e ) {
                    String msg = "Error setting value for " + key + ":\n";
                    
                    if( val == null ) {
                        msg = msg + " (null)";
                    }
                    else {
                        msg = msg + " (" + val + ":" +
                            val.getClass().getName() + ")";
                    }
                    msg = msg + ":\n" + e.getClass().getName() + ":\n";
                    throw new PersistenceException(msg + e.getMessage());
                }
                catch( IllegalArgumentException e ) {
                    String msg = "Error setting value for " + key;

                    if( val == null ) {
                        msg = msg + " (null)";
                    }
                    else {
                        msg = msg + " (" + val + ":" +
                            val.getClass().getName() + ")";
                    }
                    msg = msg + ":\n" + e.getClass().getName() + ":\n";
                    throw new PersistenceException(msg + e.getMessage());
                }
            }
        }
        finally {
            logger.debug("exit - load(Map)");
        }
    }

    /**
     * Saves the current state of the persistent object.
     * @param data values that should be used to override any
     * current state
     * @throws org.dasein.persist.PersistenceException an error occurred
     * reading values from the persistent object
     */
    public void save(Map<String, Object> data) throws PersistenceException {
        logger.debug("enter - save(Map)");
        try {
            Class<?> cls = persistent.getClass();
            
            state = new HashMap<String, Object>();
            while( cls != null && !cls.getName().equals(Object.class.getName()) ) {
                Field[] fields = cls.getDeclaredFields();
                
                for( Field f : fields ) {
                    int m = f.getModifiers();
                
                    if( Modifier.isTransient(m) || Modifier.isStatic(m) ) {
                        continue;
                    }
                    f.setAccessible(true);
                    state.put(f.getName(), f.get(persistent));
                }
                cls = cls.getSuperclass();
            }
            for( String key: data.keySet() ) {
                state.put(key, data.get(key));
            }
        }
        catch( IllegalAccessException e ) {
            throw new PersistenceException(e.getMessage());
        }
        finally {
            logger.debug("exit - save(Map)");
        }
    }
}
