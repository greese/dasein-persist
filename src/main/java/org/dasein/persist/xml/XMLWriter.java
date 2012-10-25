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

package org.dasein.persist.xml;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import org.dasein.util.Translator;

public class XMLWriter<C> {
    static public String getElement(Class<?> type) {
        return type.getName().replaceAll("\\.", "_").replaceAll("\\$", "_");
    }
    
    private void toAttribute(StringBuffer str, String nom, String val) {
        str.append(" ");
        str.append(nom);
        str.append("=\"");
        str.append(val);
        str.append("\"");
    }
    
    private void toTag(StringBuffer str, String nom, String val) {
        str.append("<");
        str.append(nom);
        str.append(" type=\"string\"><![CDATA[");
        str.append(val);
        str.append("]]></");
        str.append(nom);
        str.append(">\n");
    }
    
    private void toTag(StringBuffer str, String nom, String val, Map<String,String> attrs) {
        str.append("<");
        str.append(nom);
        str.append(" type=\"string\"");
        for( Map.Entry<String,String> a : attrs.entrySet() ) {
            str.append(" ");
            str.append(a.getKey());
            str.append("=\"");
            str.append(a.getValue());
            str.append("\"");
        }
        str.append("><![CDATA[");
        str.append(val);
        str.append("]]></");
        str.append(nom);
        str.append(">\n");
    }
    
    private void toTag(StringBuffer str, String nom, Translator<?> trans) {
        Iterator<String> langs = trans.languages();
        
        str.append("<");
        str.append(nom);
        str.append(" type=\"translator\">");
        while( langs.hasNext() ) {
            String lang = langs.next();
            Iterator<String> ctrys;
            
            ctrys = trans.countries(lang);
            while( ctrys.hasNext() ) {
                String ctry = ctrys.next();
                Object ob;
                
                ob = trans.getExactTranslation(new Locale(lang, ctry)).getData();
                if( ob != null ) {
                    HashMap<String,String> attributes = new HashMap<String,String>();
                    String loc;
                    
                    loc = lang.toLowerCase();
                    if( ctry != null ) {
                        loc = loc + "_" + ctry.toUpperCase();
                    }
                    attributes.put("lang", loc);
                    toTag(str, "translation", ob.toString(), attributes);
                 }
            }
        }
        str.append("</");
        str.append(nom);
        str.append(">\n");
    }
    
    static private final int EXCLUDE = Modifier.TRANSIENT | Modifier.STATIC;
    
    private void toXml(C target, Class<?> tcls, StringBuffer attrs, StringBuffer body) {
        for( Field f : tcls.getDeclaredFields() ) {
            Class<? extends Object> type;
            Object value;
            
            f.setAccessible(true);
            if( (f.getModifiers() & EXCLUDE) != 0 ) {
                continue;
            }
            try {
                value = f.get(target);
                if( value == null ) {
                    continue;
                }
            }
            catch( IllegalArgumentException e ) {
                e.printStackTrace();
                continue;
            }
            catch( IllegalAccessException e ) {
                e.printStackTrace();
                continue;
            }
            type = f.getType();
            if( Enum.class.isAssignableFrom(type) ) {
                toAttribute(attrs, f.getName(), ((Enum)value).name());
            }
            else if( Number.class.isAssignableFrom(type) ) {
                toAttribute(attrs, f.getName(), value.toString());
            }
            else if( String.class.equals(type) ) {
                toTag(body, f.getName(), value.toString());
            }
            else if( type.equals(Translator.class) ) {
                toTag(body, f.getName(), (Translator)value);
            }
            else if( !value.getClass().isArray() ) {
                toAttribute(attrs, f.getName(), value.toString());                
            }
        }
        tcls = tcls.getSuperclass();
        if( tcls == null || tcls.equals(Object.class) ) {
            return;
        }
        toXml(target, tcls, attrs, body);
    }

    public void write(PrintWriter output, C target) throws IOException {
        write(output, target, (BodyAppender<C>)null);
    }
    
    public void write(PrintWriter output, C target, BodyAppender<C> appender) throws IOException {
        StringBuffer attrs = new StringBuffer();
        StringBuffer body = new StringBuffer();
        String elem = getElement(target.getClass());
        
        output.print("<");
        output.print(elem);
        toXml(target, target.getClass(), attrs, body);
        output.print(attrs.toString());
        output.print(">\n");
        output.print(body.toString());
        if( appender != null ) {
            output.print(appender.getXml(target));
        }
        output.print("</");
        output.print(elem);
        output.print(">\n");
        output.flush();
    }    

    public void write(PrintWriter output, C target, String id) throws IOException {
        write(output, target, id, null);
    }
    
    public void write(PrintWriter output, C target, String id, BodyAppender<C> appender) throws IOException {
        StringBuffer attrs = new StringBuffer();
        StringBuffer body = new StringBuffer();
        String elem = getElement(target.getClass());
        
        toAttribute(attrs, "id", id);
        output.print("<");
        output.print(elem);
        toXml(target, target.getClass(), attrs, body);
        output.print(attrs.toString());
        output.print(">\n");
        output.print(body.toString());
        if( appender != null ) {
            output.print(appender.getXml(target));
        }
        output.print("</");
        output.print(elem);
        output.print(">\n");
        output.flush();        
    }
    
    /********************** EVERYTHING BELOW HERE IS FOR TESTING PURPOSES AND NOT CRITICAL *********************/
    static class Sample {
        static public enum Froody { CRM114 }
        
        private Froody             frood = Froody.CRM114;
        private Translator<String> myTranslator;
        private String             text;
        private int                number;
        private boolean            truth = true;
        
        public Sample(String txt, String en, String fr) {
            HashMap<Locale,String> map = new HashMap<Locale,String>();
            
            text = txt;
            map.put(Locale.US, en);
            map.put(Locale.FRANCE, fr);
            myTranslator = new Translator<String>(map);
            number = Integer.parseInt(text);
        }
        
        public Froody getFrood() {
            return frood;
        }
        
        public Translator<String> getMyTranslator() {
            return myTranslator;
        }
        
        public int getNumber() {
            return number;
        }
        
        public String getText() {
            return text;
        }
        
        public boolean isTruth() {
            return truth;
        }
    }
    
    static public void main(String ... args) throws Exception {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(System.out));
        XMLWriter<Sample> x = new XMLWriter<Sample>();
        
        writer.print("<export>\n");
        x.write(writer, new Sample("1", "One", "Un"));
        x.write(writer, new Sample("2", "Two", "Deux"));
        x.write(writer, new Sample("3", "Three", "Trois"));
        writer.print("</export>\n");
        writer.flush();
    }
}
