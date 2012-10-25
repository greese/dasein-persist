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

/* $Id: Creator.java,v 1.5 2009/02/07 18:11:51 greese Exp $ */
/* Copyright (c) 2006 Valtira Corporation, All Rights Reserved */
package org.dasein.persist.jdbc;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.dasein.persist.PersistenceException;
import org.dasein.persist.Transaction;
import org.dasein.util.Translator;

public class Creator extends AutomatedSql {
    private String                   sql        = null;
    
    public Creator() {
        super();
    }
    
    public synchronized String getStatement() throws SQLException {
        if( sql == null ) {
            StringBuilder str = new StringBuilder();
            Iterator<String> it = getColumns().iterator();
            
            str.append("INSERT INTO ");
            str.append(getIdentifier(getTableName()));
            str.append(" ( ");
            while( it.hasNext() ) {
                String col = it.next();
                
                str.append(getIdentifier(getSqlName(col)));
                if( it.hasNext() ) {
                    str.append(", ");
                }
            }
            str.append(" ) VALUES ( ");
            it = getColumns().iterator();
            while( it.hasNext() ) {
                it.next();
                str.append("?");
                if( it.hasNext() ) {
                    str.append(", ");
                }
            }
            str.append(" )");
            sql = str.toString();
        }
        return sql;
    }
    
    public void prepare(Map<String,Object> params) throws SQLException {
        int i = 1;
        
        for( String col : getColumns() ) {
            prepare(col, i++, params.get(col));
        }
    }
    
    @SuppressWarnings("unchecked")
    public Map<String,Object> run(Transaction xaction, Map<String,Object> params) throws SQLException, PersistenceException {
        prepare(params);
        statement.executeUpdate();
        if( isTranslating() ) {
            Collection<String> translators = getTranslators();
            Object key = params.get((String)params.get("--key--"));

            if( translators.size() > 0 ) {
                removeStringTranslations(xaction, getTarget(), key.toString());
                for( String field : translators ) {
                    Translator<String> t = (Translator<String>)params.get(field);
    
                    if (t != null) {
                        saveStringTranslation(xaction, getTarget().getName(), key.toString(), field, t);
                    }
                }
            }
        }
        return params;
    }
}
