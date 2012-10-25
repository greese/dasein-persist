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

package org.dasein.persist.jdbc;

import org.dasein.persist.Execution;

public abstract class TranslationSql extends Execution {
    protected String getSqlNameForClassName(String cname) {
        String[] parts = cname.split("\\.");
        int i;
        
        if( parts != null && parts.length > 1 ) { 
            cname = parts[parts.length-1];
        }
        i = cname.lastIndexOf("$");
        if( i != -1 ) {
            cname = cname.substring(i+1);
        }                
        return getSqlName(cname);
    }
    
    protected String getSqlName(Class cls) {
        return getSqlNameForClassName(cls.getName());
    }
    
    protected String getSqlName(String nom) {
        StringBuilder sql = new StringBuilder();
        
        for(int i=0; i<nom.length(); i++) {
            char c = nom.charAt(i);
            
            if( Character.isLetter(c) && !Character.isLowerCase(c) ) {
                if( i != 0 ) {
                    sql.append("_");
                }
                sql.append(Character.toLowerCase(c));
            }
            else {
                sql.append(c);
            }
        }
        return sql.toString();
    }
}
