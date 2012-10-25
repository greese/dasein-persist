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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.dasein.attributes.DataType;
import org.dasein.attributes.DataTypeFactory;
import org.dasein.attributes.DataTypeMap;
import org.dasein.persist.Execution;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;

public class LoadTypes extends Execution {
    static final String TYPE_TABLE = "dsn_attribute_type";
    
    public String sql = null;
    
    public synchronized String getStatement() throws SQLException {
        if( sql == null ) {
        	StringBuilder str = new StringBuilder();
            
            str.append("SELECT ");
            str.append(getIdentifier(TYPE_TABLE, "name"));
            str.append(", ");
            str.append(getIdentifier(TYPE_TABLE, "group"));
            str.append(", ");
            str.append(getIdentifier(TYPE_TABLE, "index"));
            str.append(", ");
            str.append(getIdentifier(TYPE_TABLE, "data_type"));
            str.append(", ");
            str.append(getIdentifier(TYPE_TABLE, "type_parameters"));
            str.append(", ");
            str.append(getIdentifier(TYPE_TABLE, "required"));
            str.append(", ");
            str.append(getIdentifier(TYPE_TABLE, "multi_valued"));
            str.append(", ");
            str.append(getIdentifier(TYPE_TABLE, "multi_lingual"));
            str.append(", ");
            str.append(getIdentifier(TYPE_TABLE, "type_id"));
            str.append(" FROM ");
            str.append(getIdentifier(TYPE_TABLE));
            str.append(" WHERE ");
            str.append(getIdentifier(TYPE_TABLE, "type_class"));
            str.append(" = ?");
            str.append(" ORDER BY ");
            str.append(getIdentifier(TYPE_TABLE, "group"));
            str.append(", ");
            str.append(getIdentifier(TYPE_TABLE, "index"));
            sql = str.toString();
        }
        return sql;
    }
    
    static private final int TYPE_CLASS    = 1;
    
    static private final int NAME          = 1;
    static private final int GROUP         = 2;
    static private final int INDEX         = 3;
    static private final int TYPE          = 4;
    static private final int PARAMETERS    = 5;
    static private final int REQUIRED      = 6;
    static private final int MULTI_VALUED  = 7;
    static private final int MULTI_LINGUAL = 8;
    static private final int TYPE_ID       = 9;
    
    public Map<String,Object> run(Transaction xaction, Map<String,Object> params) throws PersistenceException, SQLException {
        Map<String,DataType<?>> types = new HashMap<String,DataType<?>>();
        Map<String,Object> map = new HashMap<String,Object>();
        String cname = (String)params.get(AttributeDAO.TYPE_CLASS);
        String id = (String)params.get(AttributeDAO.TYPE_ID);
        
        statement.setString(TYPE_CLASS, cname);
        ResultSet results = statement.executeQuery();
        
        try {
            while( results.next() ) {
                String nom = results.getString(NAME);
                String grp = results.getString(GROUP);
                Number idx = results.getInt(INDEX);
                if (results.wasNull()) {
                    idx = null;
                }
                String type = results.getString(TYPE);
                String[] tparams;
                String st, oid;
                boolean req, mv, ml;
                
                st = results.getString(PARAMETERS);
                if( st == null || results.wasNull() ) { 
                    st = null;
                }
                else {
                    st = st.trim();
                }
                if( st != null ) {
                    tparams = st.split(":");
                    if( tparams.length < 1 ) {
                        tparams = new String[1];
                        tparams[0] = st;
                    }
                }
                else {
                    tparams = new String[0];
                }
                req = results.getString(REQUIRED).trim().equalsIgnoreCase("y");
                mv = results.getString(MULTI_VALUED).trim().equalsIgnoreCase("y");
                ml = results.getString(MULTI_LINGUAL).trim().equalsIgnoreCase("y");
                oid = results.getString(TYPE_ID).trim();
                if( id == null && !oid.equals("") ) {
                    continue;
                }
                else if( id != null && !oid.equals("") && !id.equals(oid) ) {
                    continue;
                }
                types.put(nom, DataTypeFactory.getInstance(type).getType(grp, idx, ml, mv, req, tparams));
            }
        }
        finally {
            try { results.close(); }
            catch( SQLException e ) { }
        }
        map.put(AttributeDAO.ATTRIBUTES, new DataTypeMap(types)); 
        return map;
    }
}
