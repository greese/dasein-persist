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

package org.dasein.persist.attributes;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.dasein.attributes.AttributeMap;
import org.dasein.attributes.DataTypeMap;
import org.dasein.persist.Execution;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;

public class AttributeDAO {
    static public final String ATTRIBUTES  = "attributes";
    static public final String GROUP       = "group";
    static public final String INDEX       = "index";
    static public final String OWNER_CLASS = "ownerClass";
    static public final String OWNER_ID    = "ownerId";
    static public final String TYPE_CLASS  = "typeClass";
    static public final String TYPE_ID     = "typeId";
    static public final String TYPES       = "types";
    static public final String NAME        = "name";
    static public final String TEXT_VALUE  = "textValue";
    
    static private AttributeDAO factory = new AttributeDAO();

    static public AttributeDAO getInstance() {
        return factory;
    }

    public void saveAttributes(Transaction xaction, Class tc, String tcid, Class oc, String ocid, AttributeMap attrs) throws PersistenceException {
        Map<String,Object> state = new HashMap<String,Object>();

        state.put(TYPE_CLASS, tc.getName());
        state.put(TYPE_ID, tcid);
        state.put(OWNER_CLASS, oc.getName());
        state.put(OWNER_ID, ocid);
        state.put(ATTRIBUTES, attrs);
        state.put(TYPES, loadTypes(xaction, tc, tcid));
        xaction.execute(RemoveAttributes.class, state);
        xaction.execute(CreateAttributes.class, state);
    }

    public void saveTypes(Transaction xaction, Class tc, String tcid, DataTypeMap types) throws PersistenceException {
        Map<String,Object> state = new HashMap<String,Object>();
        
        state.put(TYPE_CLASS, tc.getName());
        state.put(TYPE_ID, tcid);
        state.put(ATTRIBUTES, types);
        xaction.execute(RemoveTypes.class, state);
        xaction.execute(CreateTypes.class, state);
    }
    
    public AttributeMap loadAttributes(Class cls, String oid) throws PersistenceException {
        Transaction xaction = Transaction.getInstance();
        
        try {
            Map<String,Object> state = new HashMap<String,Object>();
    
            state.put(OWNER_ID, oid);
            state.put(OWNER_CLASS, cls.getName());
            state = xaction.execute(LoadAttributes.class, state, Execution.getDataSourceName(cls.getName()));
            xaction.commit();
            return new AttributeMap(state);
        }
        finally {
            xaction.rollback();
        }
    }

    public DataTypeMap loadTypes(Class cls, String oid) throws PersistenceException {
        Transaction xaction = Transaction.getInstance();
        
        try {
            DataTypeMap types = loadTypes(xaction, cls, oid);
            
            xaction.commit();
            return types;
        }
        finally {
            xaction.rollback();
        }
    }

    public DataTypeMap loadTypes(Transaction xaction, Class cls, String oid) throws PersistenceException {
        Map<String,Object> state = new HashMap<String,Object>();
        
        state.put(TYPE_ID, oid);
        state.put(TYPE_CLASS, cls.getName());
        state = xaction.execute(LoadTypes.class, state, Execution.getDataSourceName(cls.getName()));
        return (DataTypeMap)state.get(ATTRIBUTES);        
    }
    
    public void removeAttributes(Transaction xaction, Class cls, String oid) throws PersistenceException {
        Map<String,Object> state = new HashMap<String,Object>();

        state.put(OWNER_ID, oid);
        state.put(OWNER_CLASS, cls.getName());
        xaction.execute(RemoveAttributes.class, state);
    }
    
    public void removeTypes(Transaction xaction, Class cls, String oid) throws PersistenceException {
        Map<String,Object> state = new HashMap<String,Object>();
        
        state.put(TYPE_ID, oid);
        state.put(TYPE_CLASS, cls.getName());
        xaction.execute(RemoveTypes.class, state);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
	public Collection<String> findOwnerId(Class cls, String name, String value) throws PersistenceException {
    	Transaction xaction = Transaction.getInstance();
        
        try {
	    	 Map<String,Object> state = new HashMap<String,Object>();
	         
	         state.put(OWNER_CLASS, cls);
	         state.put(NAME, name);
	         state.put(TEXT_VALUE, value);
	         Map<String,Object> results = xaction.execute(FindOwnerId.class, state);
	         
	         return (Collection<String>) results.get("results");	         
        } finally {
        	xaction.rollback();
        }                 
    }
}
