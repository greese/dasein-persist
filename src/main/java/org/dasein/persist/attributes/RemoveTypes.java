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

import java.sql.SQLException;
import java.util.Map;

import org.dasein.persist.Execution;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;

public class RemoveTypes extends Execution {
    static private final int OBJECT_TYPE  = 1;
    static private final int OBJECT_ID    = 2;

    private String sql = null;
    
    public synchronized String getStatement() throws SQLException {
        if( sql == null ) {
        	StringBuilder str = new StringBuilder();
            
            str.append("DELETE FROM ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE));
            str.append(" WHERE ");
            str.append(getIdentifier("type_class"));
            str.append(" = ? AND ");
            str.append(getIdentifier("type_id"));
            str.append(" = ?");
            sql = str.toString();
        }
        return sql;
    }
    
    public Map<String, Object> run(Transaction xaction, Map<String,Object> params) throws PersistenceException, SQLException {
        String cname = (String)params.get(AttributeDAO.TYPE_CLASS);
        String id = (String)params.get(AttributeDAO.TYPE_ID);
        
        statement.setString(OBJECT_TYPE, cname);
        if( id == null ) {
            id = "";
        }
        statement.setString(OBJECT_ID, id);
        statement.executeUpdate();
        return null;
    }
}
    
