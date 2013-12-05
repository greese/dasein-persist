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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.dasein.persist.Execution;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;

public class FindOwnerId extends Execution {
    static final String ATTRIBUTE_TABLE = "dsn_attribute";
    
    private String sql = null;
    
    public synchronized String getStatement() throws SQLException {
        if( sql == null ) {
            StringBuilder str = new StringBuilder();
            
            str.append("SELECT ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "owner_id"));
            str.append(" FROM ");
            str.append(getIdentifier(ATTRIBUTE_TABLE));
            str.append(" WHERE ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "owner_class"));
            str.append(" = ? AND ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "name"));
            str.append(" = ? AND ");
            str.append(getIdentifier(ATTRIBUTE_TABLE, "text_value"));
            str.append(" = ?");
            
            sql = str.toString();
        }
        return sql;
    }
    
    static private final int OWNER_CLASS   = 1;
    static private final int NAME          = 2;
    static private final int TEXT_VALUE    = 3;
    
    static private final int OWNER_ID      = 1;
    
    @SuppressWarnings({ "rawtypes" })
    public Map<String, Object> run(Transaction xaction, Map<String,Object> params) throws PersistenceException, SQLException {
        HashMap<String,Object> attributes = new HashMap<String,Object>();
        Class clazz = (Class)params.get(AttributeDAO.OWNER_CLASS);
        String name = (String)params.get(AttributeDAO.NAME);
        String value = (String)params.get(AttributeDAO.TEXT_VALUE);
        Collection<String> values = new ArrayList<String>();

        statement.setString(OWNER_CLASS, clazz.getName());
        statement.setString(NAME, name);
        statement.setString(TEXT_VALUE, value);
        ResultSet results = statement.executeQuery();
        
        try {
            while( results.next() ) {
                values.add(results.getString(OWNER_ID));
            }
        }
        finally {
            try { results.close(); }
            catch( SQLException e ) { }
        }
        attributes.put("results", values);
        return attributes;
    }
    
    @Override
	public boolean isReadOnly() {
		return true;
	}
}
