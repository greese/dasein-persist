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

/* $Id: Updater.java,v 1.5 2007/03/25 18:46:27 greese Exp $ */
/* Copyright (c) 2006 Valtira Corporation, All Rights Reserved */
package org.dasein.persist.jdbc;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;
import org.dasein.util.Translator;

public class Updater extends AutomatedSql {
    private String  sql        = null;
    
    public Updater() {
        super();
    }
    
    public synchronized String getStatement() throws SQLException {
        if( sql == null ) {
            StringBuilder str = new StringBuilder();
            Iterator<String> it = getColumns().iterator();
            
            str.append("UPDATE ");
            str.append(getIdentifier(getTableName()));
            str.append(" SET ");
            while( it.hasNext() ) {
                String col = it.next();
                
                str.append(getIdentifier(getSqlName(col)));
                str.append(" = ?");
                if( it.hasNext() ) {
                    str.append(", ");
                }
            }
            if( !getCriteria().isEmpty() ) {
                Iterator<Criterion> criteria;
                
                str.append(" WHERE ");
                criteria = getCriteria().iterator();
                while( criteria.hasNext() ) {
                    Criterion criterion = criteria.next();
                    String col = criterion.column;
             
                    if( col.equals("timestamp") ) {
                        str.append(getIdentifier("last_modified"));
                    }
                    else {
                        str.append(getIdentifier(getSqlName(col)));
                    }
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
        
        for( String col : getColumns() ) {
            prepare(col, i++, params.get(col));
        }
        for( Criterion criterion : getCriteria() ) {
            prepare(criterion.column, i++, params.get(criterion.column));
        }
        /*
        if( terms != null ) {
            for( SearchTerm term : terms ) {
                prepare(term.getColumn(), i++, term.getValue());
            }
        }
        */
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public Map<String,Object> run(Transaction xaction, Map<String,Object> params) throws SQLException, PersistenceException {
        prepare(params);
        statement.executeUpdate();
        if( isTranslating() ) {
            Object key = params.get((String)params.get("--key--"));
            
            this.removeStringTranslations(xaction, getTarget(), key.toString());
            for( String field : getTranslators() ) {
                Translator<String> t = (Translator<String>)params.get(field);
                
                this.saveStringTranslation(xaction, getTarget(), key.toString(), field, t);
            }
        }
        return params;
    }
}
