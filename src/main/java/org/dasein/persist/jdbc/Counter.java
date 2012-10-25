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

/* $Id: Counter.java,v 1.1 2006/07/24 14:29:25 greese Exp $ */
/* Copyright (c) 2006 Valtira Corporation, All Rights Reserved */
package org.dasein.persist.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;

public class Counter extends AutomatedSql {
    private String                   sql        = null;
    
    public Counter() {
        super();
    }
    
    public synchronized String getStatement() throws SQLException {
        if( sql == null ) {
            StringBuilder str = new StringBuilder();

            str.append("SELECT COUNT( * ) FROM ");
            str.append(getIdentifier(getTableName()));
            if( !getCriteria().isEmpty() ) {
                Iterator<Criterion> criteria;
                
                str.append(" WHERE ");
                criteria = getCriteria().iterator();
                while( criteria.hasNext() ) {
                    Criterion criterion = criteria.next();
                    
                    str.append(getIdentifier(getTableName(), getSqlName(criterion.column)));
                    str.append(" ");
                    str.append(criterion.operator.toString());
                    str.append(" ?");
                    if( criteria.hasNext() ) {
                        str.append(" ");
                        str.append(getJoin().toString());
                        str.append(" ");
                    }
                }
            }
            sql = str.toString();
        }
        return sql;
    }
    
    public void prepare(Map<String,Object> params) throws SQLException {
        int i = 1;
        
        for( Criterion criterion : getCriteria() ) {
            prepare(criterion.column, i++, params.get(criterion.column));
        }
    }
    
    public Map<String,Object> run(Transaction xaction, Map<String,Object> params) throws SQLException, PersistenceException {
        HashMap<String,Object> map = new HashMap<String,Object>();
        
        prepare(params);
        ResultSet results = statement.executeQuery();
        
        try {
            if( results.next() ) {
                map.put("count", results.getLong(1));
            }
            else {
                map.put("count", 0);
            }
        }
        finally {
            try { results.close(); }
            catch( SQLException e ) { }
        }
        return map;
    }
}
