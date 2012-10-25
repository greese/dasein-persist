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
import java.sql.Types;
import java.util.Map;

import org.dasein.attributes.DataType;
import org.dasein.attributes.DataTypeMap;
import org.dasein.persist.Execution;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;

public class CreateTypes extends Execution {
    static private final int NAME          = 1;
    static private final int TYPE          = 2;
    static private final int GROUP         = 3;
    static private final int INDEX         = 4;
    static private final int PARAMETERS    = 5;
    static private final int REQUIRED      = 6;
    static private final int MULTI_VALUED  = 7;
    static private final int MULTI_LINGUAL = 8;
    static private final int OBJECT_ID     = 9;
    static private final int OBJECT_TYPE   = 10;
    
    private String sql = null;
    
    public synchronized String getStatement() throws SQLException {
        if( sql == null ) {
        	StringBuilder str = new StringBuilder();
            
            str.append("INSERT INTO ");
            str.append(getIdentifier(LoadTypes.TYPE_TABLE));
            str.append(" ( ");
            str.append(getIdentifier("name"));
            str.append(", ");
            str.append(getIdentifier("data_type"));
            str.append(", ");
            str.append(getIdentifier("group"));
            str.append(", ");
            str.append(getIdentifier("index"));
            str.append(", ");
            str.append(getIdentifier("type_parameters"));
            str.append(", ");
            str.append(getIdentifier("required"));
            str.append(", ");
            str.append(getIdentifier("multi_valued"));
            str.append(", ");
            str.append(getIdentifier("multi_lingual"));
            str.append(", ");
            str.append(getIdentifier("type_id"));
            str.append(", ");
            str.append(getIdentifier("type_class"));
            str.append(" ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )");
            sql = str.toString();
        }
        return sql;
    }
    
    public Map<String, Object> run(Transaction xaction, Map<String,Object> params) throws PersistenceException, SQLException {
        DataTypeMap attrs = (DataTypeMap)params.get(AttributeDAO.ATTRIBUTES);
        String cname = (String)params.get(AttributeDAO.TYPE_CLASS);
        String id = (String)params.get(AttributeDAO.TYPE_ID);
        
        for( Map.Entry<String,DataType<? extends Object>> entry : attrs.entrySet() ) {
            DataType<?> type = entry.getValue();
            String attr = entry.getKey();
            
            save(cname, id, attr, type);
        }
        return null;
    }

    private void save(String cname, String id, String attr, DataType<? extends Object> type) throws PersistenceException, SQLException {
    	StringBuilder str = null;
        
        statement.setString(NAME, attr);
        statement.setString(TYPE, type.getName());
        for( String param : type.getParameters() ) {
            if( str == null ) {
                str = new StringBuilder();
            }
            else {
                str.append(":");
            }
            if( param != null ) {
                str.append(param);
            }
        }
        if( str == null ) {
            statement.setNull(PARAMETERS, Types.VARCHAR);
        }
        else {
            statement.setString(PARAMETERS, str.toString());
        }
        statement.setString(REQUIRED, type.isRequired() ? "Y" : "N");
        statement.setString(MULTI_VALUED, type.isMultiValued() ? "Y" : "N");
        statement.setString(MULTI_LINGUAL, type.isMultiLingual() ? "Y" : "N");
        if( id == null ) {
            id = "";
        }
        statement.setString(OBJECT_ID, id);
        statement.setString(OBJECT_TYPE, cname);

        String st = type.getGroup();
        if (st == null) {
            statement.setNull(GROUP, Types.VARCHAR);
        } else {
            statement.setString(GROUP, st);
        }
        int num = type.getIndex();
        if (num == -1) {
            statement.setNull(INDEX, Types.INTEGER);
        } else {
            statement.setInt(INDEX, num);
        }        
        statement.executeUpdate();
        statement.clearParameters();
        
    }
}
    
