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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Stack;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.dasein.persist.PersistenceException;
import org.dasein.persist.xml.XMLWriter.Sample;
import org.dasein.util.Jiterator;
import org.dasein.util.Translator;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class XMLReader<T> extends DefaultHandler {    
    static private Logger logger = Logger.getLogger(XMLReader.class);
    
    static public class StringDelegate extends TagDelegate {
        
        public boolean matches(String tagName) {
            return tagName.equals("string");
        }
        
        public void startElement(XMLReader reader, String nom, Map<String,String> attrs) {
            
        }
        
        public Object endElement(XMLReader reader, String nom) {
            setDone();
            if( reader.cdata == null ) {
                return null;
            }
            else {
                return reader.cdata.toString();
            }
        }
    }
    
    static public class TranslatorDelegate extends TagDelegate {
        private Locale             currentLanguage     = null;
        private Translator<Object> currentTranslator   = null;
        private String             myName              = null;
        private TagDelegate        translationDelegate = null;
        
        public boolean matches(String tagName) {
            return tagName.equals("translator");
        }
        
        public void startElement(XMLReader reader, String nom, Map<String,String> attrs) {
            if( myName == null ) {
                myName = nom;
            }
            if( nom.equals("translation") ) {
                String l = attrs.get("lang");
                String t = attrs.get("type");
             
                translationDelegate = reader.getDelegate(t);
                currentLanguage = Translator.parseLocale(l);
            }
            if( translationDelegate != null ) {
                translationDelegate.startElement(reader, nom, attrs);
            }
        }
        
        public Object endElement(XMLReader reader, String nom) {
            if( nom.equals(myName) ) {
                Translator<Object> trans = currentTranslator;
                
                currentTranslator = null;
                setDone();
                return trans;
            }
            else if( nom.equals("translation") ) {
                if( translationDelegate != null ) {
                    Object val = translationDelegate.endElement(reader, nom);
                    
                    if( val != null ) {
                        if( currentTranslator == null ) {
                            currentTranslator = new Translator<Object>(currentLanguage, val);
                        }
                        else {
                            currentTranslator = currentTranslator.newTranslator(currentLanguage, val);
                        }
                    }
                }
                translationDelegate = null;
                currentLanguage = null;
            }
            return null;
        }
    }
    
    private StringBuffer                  cdata           = null;
    private String                        currentElement  = null;
    private Map<String,Object>            currentState    = null;
    private TagDelegate                   currentDelegate = null;
    private Jiterator<Map<String,Object>> list            = new Jiterator<Map<String,Object>>();
    private String                        root            = null;
    private Stack<String>                 stack           = new Stack<String>();
    private TagDelegate[]                 tagDelegates    = null;
    private Class<T>                      targetClass     = null;
    
    public void characters(char[] chars, int start, int len) {
        if( cdata == null ) {
            cdata = new StringBuffer();
        }
        cdata.append(new String(chars, start, len));    
    }
    
    public void endElement(String uri, String nom, String qname) throws SAXException {
        nom = getName(nom, qname);
        if( nom.equals(root) ) {
            if( currentState != null ) {
                list.push(transform(currentState));
                currentState = null;
            }
        }
        else if( currentDelegate != null ) {
            Object val = currentDelegate.endElement(this, nom);
            
            if( val != null ) {
                currentState.put(currentElement, val);
                currentDelegate = null;
            }
            else if( currentDelegate.isDone() ) {
                currentDelegate = null;
            }
        }
        if( stack.isEmpty() ) {
            currentElement = null;
        }
        else {
            currentElement = stack.pop();
        }
    }

    private Map<String,String> getAttributes(Attributes attrs) {
        HashMap<String,String> map = new HashMap<String,String>();
        
        for(int i=0; i<attrs.getLength(); i++) {
            String anom = attrs.getLocalName(i);
            
            anom = getName(anom, attrs.getQName(i));
            map.put(anom, attrs.getValue(i));
        }
        return map;
    }
    
    String getCurrentParent() {
        if( stack.isEmpty() ) {
            return null;
        }
        return stack.peek();
    }
    
    String getName(String nom, String qnom) {
        if( nom.length() > 0 ) {
            return nom;
        }
        return qnom;
    }
    
    public TagDelegate getDelegate(String t) {
        if( t.equals("string") ) {
            return new StringDelegate();
        }
        else if( t.equals("translator") ) {
            return new TranslatorDelegate();
        }
        else if( tagDelegates != null ) {
            for( TagDelegate delegate : tagDelegates ) {
                if( delegate.matches(t) ) {
                    return delegate;
                }
            }
        }
        return null;
    }
    
    private boolean importObjects(InputStream input) throws PersistenceException {
        try {
            SAXParserFactory sax = SAXParserFactory.newInstance();
            SAXParser parser = sax.newSAXParser();
    
            parser.parse(input, this);
            return true;
        }
        catch( IOException e ) {
            System.err.println("Encountered an error while processing: " + currentElement);
            e.printStackTrace();
            throw new PersistenceException(e);            
        }
        catch( SAXException e ) {
            System.err.println("Encountered an error while processing: " + currentElement);
            e.printStackTrace();
            throw new PersistenceException(e);
        }
        catch( ParserConfigurationException e ) {
            System.err.println("Encountered an error while processing: " + currentElement);
            e.printStackTrace();
            throw new PersistenceException(e);
        }
    }
    
    public Iterable<Map<String,Object>> read(InputStream input, Class<T> type, TagDelegate ... delegates) {
        final InputStream in = input;
        
        list = new Jiterator<Map<String,Object>>();
        targetClass = type;
        root = XMLWriter.getElement(targetClass);
        tagDelegates = delegates;
        Thread t = new Thread() {
            public void run() {
                try { importObjects(in); } catch( PersistenceException e ) { e.printStackTrace(); }
                list.complete();
                currentState = null;
                cdata = null;
                stack = null;
            }
        };
        t.setDaemon(true);
        t.setName("XML reader: " + type.getName());
        t.start();
        return list;
    }
        
    public void startElement(String uri, String nom, String qname, Attributes attrs) throws SAXException {
        logger.debug("enter - startElement(String,String,String,Attributes)");
        try {
            Map<String,String> attributes;
            String prnt = currentElement;
            
            currentElement = getName(nom, qname);
            if( logger.isDebugEnabled() ) {
                logger.debug("Parsing " + currentElement);
            }
            cdata = null;
            if( currentElement.equals(root) ) {
                stack.clear();
                currentState = new HashMap<String,Object>();
            }
            else if( prnt != null ) {
                stack.push(prnt);            
            }
            else {
                stack.clear();
            }
            attributes = getAttributes(attrs);
            if( currentElement.equals(root) ) {
                currentState.putAll(attributes);
                currentDelegate = null;
            }
            else if( currentDelegate == null ) {
                String t = attributes.get("type");
                
                if( t != null ) {
                    currentDelegate = getDelegate(t);
                }
            }
            if( currentDelegate != null ) {
                currentDelegate.startElement(this, currentElement, attributes);
            }
        }
        finally {
            logger.debug("exit - startElement(String,String,String,Attributes)");            
        }
    }    
    
    @SuppressWarnings("unchecked")
    private Map<String,Object> transform(Map<String,Object> map) {
        HashMap<String,Object> state = new HashMap<String,Object>();
        
        for( Map.Entry<String,Object> field : map.entrySet() ) {
            try {
                Field f = targetClass.getDeclaredField(field.getKey());
                
                if( f != null ) {
                    Class<?> type;
                    
                    f.setAccessible(true);
                    type = f.getType();
                    if( String.class.equals(type) ) {
                        state.put(field.getKey(), field.getValue());
                    }
                    else if( Translator.class.isAssignableFrom(type) ) {
                        Object val = field.getValue();
                        
                        if( val instanceof String ) {
                            state.put(field.getKey(), new Translator<String>(Locale.US, (String)val));
                        }
                        else {
                            state.put(field.getKey(), val);
                        }
                    }
                    else if( Long.class.isAssignableFrom(type) || long.class.equals(type) ) {
                        state.put(field.getKey(), Long.parseLong((String)field.getValue()));
                    }
                    else if( Double.class.isAssignableFrom(type) || double.class.equals(type) || float.class.equals(type) ) {
                        state.put(field.getKey(), Double.parseDouble((String)field.getValue()));
                    }
                    else if( Integer.class.isAssignableFrom(type) || int.class.equals(type) || short.class.equals(type) ) {
                        state.put(field.getKey(), Integer.parseInt((String)field.getValue()));
                    }
                    else if( Number.class.isAssignableFrom(type) ) {
                        state.put(field.getKey(), Long.parseLong((String)field.getValue()));                        
                    }
                    else if( Boolean.class.isAssignableFrom(type) || boolean.class.equals(type) ) {
                        String str = (String)field.getValue();
                        
                        state.put(field.getKey(), str == null ? false : str.equalsIgnoreCase("true"));
                    }
                    else if( Enum.class.isAssignableFrom(type) ) {
                        String str = (String)field.getValue();

                        state.put(field.getKey(), Enum.valueOf((Class<? extends Enum>)type, str));
                    }
                    else {
                        state.put(field.getKey(), field.getValue());
                    }
                }
            }
            catch( SecurityException e ) {
                // ignore
            }
            catch( NoSuchFieldException e ) {
                // ignore
            }
        }
        return state;
    }
    
    /********************** EVERYTHING BELOW HERE IS FOR TESTING PURPOSES AND NOT CRITICAL *********************/
    
    static public void main(String ... args) throws Exception {
        FileInputStream fin = new FileInputStream(args[0]);
        XMLReader<Sample> reader = new XMLReader<Sample>();
        
        for( Map<String,Object> map : reader.read(fin, Sample.class) ) {
            System.out.println("Next...");
            for( Map.Entry<String,Object> entry : map.entrySet() ) {
                System.out.println("\t" + entry.getKey() + " (" + (entry.getValue() != null ? entry.getValue().getClass().getName() : "") + "): " + entry.getValue());
            }
        }
    }
}
