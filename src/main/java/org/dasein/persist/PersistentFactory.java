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

/* $Id: PersistentFactory.java,v 1.40 2009/07/02 01:36:20 greese Exp $ */
/* Copyright (c) 2005 Valtira Corporation, All Rights Reserved */
package org.dasein.persist;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.dasein.persist.dao.LoadTranslator;
import org.dasein.persist.dao.RemoveTranslator;
import org.dasein.persist.dao.SaveTranslator;
import org.dasein.persist.jdbc.AutomatedSql;
import org.dasein.persist.jdbc.AutomatedSql.Operator;
import org.dasein.persist.jdbc.Counter;
import org.dasein.persist.jdbc.Creator;
import org.dasein.persist.jdbc.Deleter;
import org.dasein.persist.jdbc.Loader;
import org.dasein.persist.jdbc.TranslatorDeleter;
import org.dasein.persist.jdbc.TranslatorLoader;
import org.dasein.persist.jdbc.TranslatorUpdater;
import org.dasein.persist.jdbc.Updater;
import org.dasein.persist.xml.XMLReader;
import org.dasein.persist.xml.XMLWriter;
import org.dasein.util.CacheLoader;
import org.dasein.util.CacheManagementException;
import org.dasein.util.ConcurrentMultiCache;
import org.dasein.util.JitCollection;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorFilter;
import org.dasein.util.Translator;

/**
 * <p>
 *   Manages the movement of persistent objects between the data store and a memory cache.
 *   This class is generally used as a delegate by true factory classes.
 * </p>
 * <pre>
 * public class EmployeeFactory {
 *   private PersistentFactory&lt;Employee&gt; factory;
 *   
 *   public EmployeeFactory() {
 *       factory = new PersistentFactory&lt;Employee&gt;(Employee.class, "employeeId", "ssn");
 *   }
 *   
 *   public Collection&lt;Employee&gt; findEmployees(String ln) throws PersistenceException {
 *       return factory.find("lastName", ln);
 *   }
 *   
 *   public Employee getEmployee(Long empid) throws PersistenceException {
 *       return factory.get("employeeId", empid);
 *   }
 *   
 *   public Employee getEmployee(String ssn) throws PersistenceException {
 *       return factory.get("ssn", ssn);
 *   }
 *   
 *   public void updateEmployee(Transaction xaction, Employee emp, Map&lt;String,Object&gt; state)
 *   throws PersistenceException {
 *       factory.update(xaction, emp, state);
 *   }
 * }
 * </pre>
 * <p>
 * This persistence is automatic. You may, however, specify your own queries that will be
 * managed by passing in an extension of the {@link Execution} class.
 * </p>
 * <p>
 *   Last modified: $Date: 2009/07/02 01:36:20 $
 * </p>
 * @version $Revision: 1.40 $
 * @author George Reese
 * @param <T> the type of object managed by this factory
 * @deprecated This API is ancient and is being deprecated in favor of the new @{link PersistentCache}
 */
public final class PersistentFactory<T> {
    static public final Logger logger = Logger.getLogger(PersistentFactory.class);
    
    static private final String classDirectory;
    static private final String persistenceLib;
    static private final String utilitiesLib;

    static {
        Properties props = new Properties();
        
        try {
            InputStream is = DaseinSequencer.class.getResourceAsStream(DaseinSequencer.PROPERTIES);

            if( is != null ) {
                props.load(is);
            }
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
        classDirectory = props.getProperty("dasein.persist.classes");
        persistenceLib = props.getProperty("dasein.persist.persistenceLib");
        utilitiesLib = props.getProperty("dasein.persist.utilitiesLib");
    }
    
    static public interface DependencyManager<T> {
        public abstract void createDependencies(Transaction xaction, Map<String,Object> state) throws PersistenceException;
        
        public abstract void loadDependencies(Map<String,Object> state) throws PersistenceException;
        
        public abstract void removeDependencies(Transaction xaction, Map<String,Object> state) throws PersistenceException;
        
        public abstract void updateDependencies(Transaction xaction, T item, Map<String,Object> state) throws PersistenceException;
    }
    
    static public Class<? extends Execution> compileTranslator(Class<?> t, String which) throws PersistenceException {
        logger.debug("enter - compileTranslator(Class)");
        try {
            StringBuilder str = new StringBuilder();
            String cname, fcn;
            String[] parts;

            fcn = "org.dasein.persist.runtime.trans.";
            str.append("package org.dasein.persist.runtime.trans");
            parts = t.getName().split("\\.");
            if( parts.length > 1 ) {
                for( int i=0; i<parts.length-1; i++ ) {
                    str.append(".");
                    str.append(parts[i]);
                    fcn += parts[i];
                    fcn += ".";
                }
                cname = parts[parts.length-1];
                fcn += cname;
            }
            else {
                cname = t.getName();
                fcn += cname;
            }
            cname = cname + "_" + which;
            fcn = fcn + "_" + which;
            // we should try to find away to check if the class already exists
            // however, a check for the class in this thread will prevent a further check from
            // finding the class, even after we have compiled it
            str.append(";\n\n");
            str.append("import org.dasein.persist.jdbc.Translator");
            str.append(which);
            str.append(";\n\n");
            str.append("public class ");
            str.append(cname);
            str.append(" extends ");
            if( which.equals("Loader") ) {
                str.append(TranslatorLoader.class.getName());
            }
            else if( which.equals("Updater") ) {
                str.append(TranslatorUpdater.class.getName());
            }
            else if( which.equals("Deleter") ) {
                str.append(TranslatorDeleter.class.getName());
            }
            else {
                str.append(TranslatorLoader.class.getName());
            }
            str.append(" {\n");
            str.append("public ");
            str.append(cname);
            str.append("() { }\n");
            str.append("public String getTable() { return ");
            str.append("getSqlNameForClassName(\"" + t.getName() + "\") + \"_translation\"; }");
            str.append("}\n");
            return compile(fcn, cname, str.toString());
        }
        finally {
            logger.debug("exit - compileTranslator(Class)");
        }
    }
    
    /**
     * Convenience constant used by search executions as they key for multi-valued results.
     */
    static public final String LISTING = "listing";
    
    static private boolean compiling = false;
    
    /**
     * The cache of objects in memory.
     */
    private ConcurrentMultiCache<T>                    cache      = null;
    /**
     * A mapping of single attribute counters to the query associated with them.
     */
    private HashMap<String,Class<? extends Execution>> counters     = new HashMap<String,Class<? extends Execution>>();
    /**
     * The class of an update execution, if any.
     */
    private Class<? extends Execution>                 create       = null;
    /**
     * Dependency delegate for managing any dependencies.
     */
    private DependencyManager<T>                       dependency   = null;
    private ExportHook<T>                              exportHook   = null;
    private ImportHook<T>                              importHook   = null;
    private HashMap<String,Class<? extends Execution>> joins        = new HashMap<String,Class<? extends Execution>>();
    private HashMap<String,Class<? extends Execution>> joinCounters = new HashMap<String,Class<? extends Execution>>();
    private String                                     key          = null;
    /**
     * A mapping of single attribute searches to the query associated with them.
     */
    private HashMap<String,Class<? extends Execution>> searches     = new HashMap<String,Class<? extends Execution>>();
    /**
     * A mapping of unique ID searches to the query associated with them.
     */
    private HashMap<String,Class<? extends Execution>> singletons = new HashMap<String,Class<? extends Execution>>();
    /**
     * The class of an update execution, if any.
     */
    private Class<? extends Execution>                 update     = null;
    /**
     * The class of a delete execution, if any.
     */
    private Class<? extends Execution>                 remove     = null;    
    /**
     * The translation method for the objects managed by this factory.  
     */
    private AutomatedSql.TranslationMethod 			   translationMethod = null;
    
    /**
     * Constructs a new persistent factory for objects of the specified class with 
     * the named unique identifier attributes.
     * @param cls the class of objects managed by this factory
     * @param keys a list of unique identifiers for instances of the specified class
     */
    public PersistentFactory(Class<T> cls, String ... keys) {
        this(cls, AutomatedSql.TranslationMethod.STANDARD, keys);
    }
    
    /**
     * Constructs a new persistent factory for objects of the specified class with 
     * the named unique identifier attributes.
     * @param cls the class of objects managed by this factory
     * @param keys a list of unique identifiers for instances of the specified class
     */
    @Deprecated
    public PersistentFactory(Class<T> cls, boolean custom, String ... keys) {
        super();
        cache = new ConcurrentMultiCache<T>(cls, keys);
        key = keys[0];
        if (custom) {
        	translationMethod = AutomatedSql.TranslationMethod.CUSTOM;
        }
    }
    
    /**
     * Constructs a new persistent factory for objects of the specified class with 
     * the named unique identifier attributes.
     * @param cls the class of objects managed by this factory
     * @param keys a list of unique identifiers for instances of the specified class
     */
    public PersistentFactory(Class<T> cls, AutomatedSql.TranslationMethod transMeth, String ... keys) {
        super();
        cache = new ConcurrentMultiCache<T>(cls, keys);
        key = keys[0];
    	translationMethod = transMeth;       
    }
    
    /**
     * Adds a query for counts on a specified field.
     * @param field the field to count matches on
     * @param cls the class of the query that performs the count
     */
    public void addCounter(String field, Class<? extends Execution> cls) {
        counters.put(field, cls);
    }
    
    /**
     * Adds a query for searches on a specified field.
     * @param field the field to search on
     * @param cls the class of the query that performs the search
     */
    public void addSearch(String field, Class<? extends Execution> cls) {
        searches.put(field, cls);
    }
    
    /**
     * Adds a query for searches on a specified field. These searches return unique values.
     * @param field the field to search on
     * @param cls the class of the query that performs the search
     */
    public void addSingleton(String field, Class<? extends Execution> cls) {
        singletons.put(field, cls);
    }

    static private transient long executionTime = -1L;
    static private transient String dsnJar = null;
    
    @SuppressWarnings({ "unchecked", "restriction" })
    static private Class<? extends Execution> compile(String fcn, String cname, String java) throws PersistenceException {
        /*
        System.out.println("Compiling: " + fcn);
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        JavaFileManager fileManager = new ClassFileManager(compiler.getStandardFileManager(null, null, null));
        List<JavaFileObject> jfiles = new ArrayList<JavaFileObject>();
        
        jfiles.add(new CharSequenceJavaFileObject(fcn, java));
        
        List<String> optionList = new ArrayList<String>();
        
        if( dsnJar == null ) {
            String pl, ul;

            if( persistenceLib == null ) {
                pl = (classDirectory + "/../lib/dasein-persistence.jar").replaceAll("\\\\", "\\\\");
            }
            else {
                pl = persistenceLib;
            }
            if( utilitiesLib == null ) {
                ul = classDirectory + "/../lib/dasein-utilities.jar";
            }
            else {
                ul = utilitiesLib;
            }
            dsnJar = pl + File.pathSeparator + ul;
        }
        
        System.out.println("CLASSPATH=" + dsnJar);
        optionList.addAll(Arrays.asList("-classpath", dsnJar));
        try {
            System.out.println("Compilinig: " + fcn);
            compiler.getTask(null, fileManager, null, optionList, null, jfiles).call();
        }
        catch( Throwable t ) {
            t.printStackTrace();
            throw new PersistenceException(t.getMessage());
        }
        try {
            System.out.println("Loading: " + fcn);
            return (Class<? extends Execution>)Class.forName(fcn);
            //return (Class<? extends Execution>)fileManager.getClassLoader(null).loadClass(fcn);
        }
        catch( Throwable t ) {
            t.printStackTrace();
            throw new PersistenceException(t.getMessage());
        }
        */
        logger.debug("enter - compile(String,String,String)");
        try {
            if( logger.isDebugEnabled() ) {
                logger.debug(fcn + "/" + cname);
            }
            String fname = System.getProperty("java.io.tmpdir");
            File src, tmp;
            String pre;
            
            if( fname == null ) {
                fname = "";
            }
            if( !fname.endsWith("/") ) {
                fname = fname + "/";
            }
            synchronized( PersistentFactory.class ) {
                if( executionTime == -1L ) {
                    long now = System.currentTimeMillis();
                
                    pre = String.valueOf(now);
                    tmp = new File(fname + pre);
                    while( tmp.exists() ) {
                        now++;
                        pre = String.valueOf(now);
                        tmp = new File(fname + pre);
                    }
                    tmp.mkdir();
                    executionTime = now;
                }
                else {
                    tmp = new File(fname + executionTime);
                }
                pre = String.valueOf(executionTime);
            }
            fname = fname + pre + "/";
            src = new File(mkdir(fname, fcn) + "/" + cname + ".java");
            src.deleteOnExit();
            try {   
                OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(src));
                BufferedWriter writer = new BufferedWriter(out);
                
                writer.write(java);
                writer.flush();
                writer.close();
            }
            catch( IOException e ) {
                e.printStackTrace();
                throw new PersistenceException(e.getMessage());
            }

            if( dsnJar == null ) {
                String pl, ul;

                if( persistenceLib == null ) {
                    pl = (classDirectory + "/../lib/dasein-persistence.jar").replaceAll("\\\\", "\\\\");
                }
                else {
                    pl = persistenceLib;
                }
                if( utilitiesLib == null ) {
                    ul = classDirectory + "/../lib/dasein-utilities.jar";
                }
                else {
                    ul = utilitiesLib;
                }
                dsnJar = pl + File.pathSeparator + ul;
            }
            logger.debug("dsnJar = " + dsnJar);
            try {
                com.sun.tools.javac.Main.compile(new String[] { "-classpath", dsnJar, "-d", classDirectory, src.getAbsolutePath() });
            }
            catch( Throwable t ) {
                t.printStackTrace();
                throw new PersistenceException(t.getMessage());
            }
            try {
                return (Class<? extends Execution>)Class.forName(fcn);
            }
            catch( Throwable t) {
                t.printStackTrace();
                throw new PersistenceException(t.getMessage());
            }
        }
        finally {
            logger.debug("exit - compile(String,String,String)");
        }
    }
    
    private void compileCounter(SearchTerm[] terms, Map<String,Class<? extends Execution>> map) throws PersistenceException {
        logger.debug("enter - compileCounter(String,Map)");
        try {
        	String key = getExecKey(terms, false);
        	
            synchronized( this ) {
                while( compiling && !map.containsKey(key) ) {
                    try { wait(100L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
                if( map.containsKey(key) ) {
                    return;
                }
                compiling = true;
            }
            try {
                Class<? extends Execution> cls;
                StringBuilder str = new StringBuilder();
                Class<T> t = cache.getTarget();
                String cname, fcn;
                String[] parts;
    
                fcn = "org.dasein.persist.runtime.counters.";
                str.append("package org.dasein.persist.runtime.counters");
                parts = t.getName().split("\\.");
                if( parts.length > 1 ) {
                    for( int i=0; i<parts.length-1; i++ ) {
                        str.append(".");
                        str.append(parts[i]);
                        fcn += parts[i];
                        fcn += ".";
                    }
                    cname = parts[parts.length-1];
                    fcn += cname;
                }
                else {
                    cname = t.getName();
                    fcn += cname;
                }
                if( terms != null && terms.length > 0 ) {
                    for( SearchTerm term : terms ) {
                        cname = cname + "_" + term.getColumn();
                        fcn = fcn + "_" + term.getColumn();
                        if( terms.length > 1 ) {
                            cname = cname + "_" + term.getOperator().name();
                            fcn = fcn + "_" + term.getOperator().name();
                        }
                    }
                }
                // we should try to find away to check if the class already exists
                // however, a check for the class in this thread will prevent a further check from
                // finding the class, even after we have compiled it
                str.append(";\n\n");
                str.append("import org.dasein.persist.jdbc.Counter;\n\n");
                str.append("public class ");
                str.append(cname);
                str.append(" extends ");
                str.append(Counter.class.getName());
                str.append(" {\n");
                str.append("public ");
                str.append(cname);
                str.append("() {\n");
                str.append("setTarget(\"");
                str.append(t.getName());
                str.append("\");\n");
                if( terms != null && terms.length > 0 ) {
                    str.append("setCriteria(");
                    if( terms.length == 1 && terms[0].getOperator().equals(Operator.EQUALS) ) {
                        str.append("\"");
                        str.append(terms[0].getColumn());
                        str.append("\"");
                    }
                    else {
                        for( int i=0; i<terms.length; i++ ) {
                            if( i > 0 ) {
                                str.append(", ");
                            }
                            str.append("new Criterion(\"");
                            str.append(terms[i].getColumn());
                            str.append("\", Operator.");
                            str.append(terms[i].getOperator().name());
                            str.append(")");
                        }
                    }
                    str.append(");\n");
                }
                str.append("}\n");
                str.append("public boolean isReadOnly() { return true; }\n");
                str.append("}\n");
                cls = compile(fcn, cname, str.toString());
                synchronized( this ) {
                    map.put(key, cls);
                    notifyAll();
                }
            }
            finally {
                synchronized( this ) {
                    compiling = false;
                    notifyAll();
                }
            }
        }
        finally {
            logger.debug("exit - compileCounter(String,Map)");
        }
    }

    private void compileCreator() throws PersistenceException {
        logger.debug("enter - compileCreator()");
        try {
            if( logger.isDebugEnabled() ) {
                logger.debug("For: " + cache.getTarget().getName());
            }
            synchronized( this ) {
                while( compiling && create == null ) {
                    try { wait(1000L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
                if( create != null  ) {
                    return;
                }
                compiling = true;
            }
            try {
                Class<? extends Execution> cls;
                StringBuilder str = new StringBuilder();
                Class<T> t = cache.getTarget();
                String cname, fcn;
                String[] parts;
    
                fcn = "org.dasein.persist.runtime.creators.";
                str.append("package org.dasein.persist.runtime.creators");
                parts = t.getName().split("\\.");
                if( parts.length > 1 ) {
                    for( int i=0; i<parts.length-1; i++ ) {
                        str.append(".");
                        str.append(parts[i]);
                        fcn += parts[i];
                        fcn += ".";
                    }
                    cname = parts[parts.length-1];
                    fcn += cname;
                }
                else {
                    cname = t.getName();
                    fcn += cname;
                }
                // we should try to find away to check if the class already exists
                // however, a check for the class in this thread will prevent a further check from
                // finding the class, even after we have compiled it
                str.append(";\n\n");
                str.append("import org.dasein.persist.jdbc.Creator;\n\n");
                str.append("public class ");
                str.append(cname);
                str.append(" extends ");
                str.append(Creator.class.getName());
                str.append(" {\n");
                str.append("public ");
                str.append(cname);
                str.append("() {\n");
                str.append("setTarget(\"");
                str.append(t.getName());
                str.append("\");\n");
                
                switch (translationMethod) {
                	case CUSTOM: str.append("setCustomTranslating();"); break;
                	case STANDARD: str.append("setTranslating(true);"); break;
                	case NONE: str.append("setTranslating(false);"); break;
                }
                
                str.append("}\n");
                str.append("}\n");
                cls = compile(fcn, cname, str.toString());
                synchronized( this ) {
                    create = cls;
                    notifyAll();
                }
            }
            finally {
                synchronized( this ) {
                    compiling = false;
                    notifyAll();
                }
            }
        }
        finally {
            logger.debug("exit - compileCreator()");
        }
    }
    
    private void compileDeleter() throws PersistenceException {
        logger.debug("enter - compileDeleter()");
        try {
            if( logger.isDebugEnabled() ) {
                logger.debug("For: " + cache.getTarget().getName());
            }
            synchronized( this ) {
                while( compiling && remove == null ) {
                    try { wait(1000L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
                if( remove != null  ) {
                    return;
                }
                compiling = true;
            }
            try {
                Class<? extends Execution> cls;
                StringBuilder str = new StringBuilder();
                Class<T> t = cache.getTarget();
                String cname, fcn;
                String[] parts;
    
                fcn = "org.dasein.persist.runtime.deleters.";
                str.append("package org.dasein.persist.runtime.deleters");
                parts = t.getName().split("\\.");
                if( parts.length > 1 ) {
                    for( int i=0; i<parts.length-1; i++ ) {
                        str.append(".");
                        str.append(parts[i]);
                        fcn += parts[i];
                        fcn += ".";
                    }
                    cname = parts[parts.length-1];
                    fcn += cname;
                }
                else {
                    cname = t.getName();
                    fcn += cname;
                }
                // we should try to find away to check if the class already exists
                // however, a check for the class in this thread will prevent a further check from
                // finding the class, even after we have compiled it
                str.append(";\n\n");
                str.append("public class ");
                str.append(cname);
                str.append(" extends ");
                str.append(Deleter.class.getName());
                str.append(" {\n");
                str.append("public ");
                str.append(cname);
                str.append("() {\n");
                str.append("setTarget(\"");
                str.append(t.getName());
                str.append("\");\n");
                switch (translationMethod) {
	            	case CUSTOM: str.append("setCustomTranslating();"); break;
	            	case STANDARD: str.append("setTranslating(true);"); break;
	            	case NONE: str.append("setTranslating(false);"); break;
                }
                str.append("setCriteria(\"");
                str.append(getKey());
                str.append("\");\n");
                str.append("}\n");
                str.append("}\n");
                cls = compile(fcn, cname, str.toString());
                synchronized( this ) {
                    remove = cls;
                    notifyAll();
                }
            }
            finally {
                synchronized( this ) {
                    compiling = false;
                    notifyAll();
                }
            }
        }
        finally {
            logger.debug("exit - compileDeleter()");
        }
    }
    
    private void compileJoin(Class<? extends Object> jc, String field, String join) throws PersistenceException {
        logger.debug("enter - compileJoin(Class,String,String)");
        try {
            if( logger.isDebugEnabled() ) {
                logger.debug("For: " + cache.getTarget().getName() + "/" + jc.getName() + "/" + field + "/" + join);
            }
            String sField = getSqlName(field);
            String sJoin = getSqlName(join);
            String jcn = jc.getName();
            
            synchronized( this ) {
                while( compiling && !joins.containsKey(jcn) ) {
                    try { wait(1000L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
                if( joins.containsKey(jcn) ) {
                    return;
                }
                compiling = true;
            }
            try {
                Class<? extends Execution> cls;
                StringBuilder str = new StringBuilder();
                Class<T> t = cache.getTarget();
                String fcn, cname, table;
                String[] parts;
    
                fcn = "org.dasein.persist.runtime.joins.";
                str.append("package org.dasein.persist.runtime.joins");
                parts = t.getName().split("\\.");
                if( parts.length > 1 ) {
                    for( int i=0; i<parts.length-1; i++ ) {
                        str.append(".");
                        str.append(parts[i]);
                        fcn += parts[i];
                        fcn += ".";
                    }
                    cname = parts[parts.length-1];
                }
                else {
                    cname = t.getName();
                }
                parts = jc.getName().split("\\.");
                if( parts.length > 1 ) {
                    cname = cname + "_" + parts[parts.length-1];
                }
                else {
                    cname = cname + "_" + jc.getName();
                }            
                cname = cname + "_" + field + "_" + join;
                fcn = fcn + cname;
                // we should try to find away to check if the class already exists
                // however, a check for the class in this thread will prevent a further check from
                // finding the class, even after we have compiled it
                str.append(";\n\n");
                str.append("public class ");
                str.append(cname);
                str.append(" extends ");
                str.append(Loader.class.getName());
                str.append(" {\n");
                str.append("public ");
                str.append(cname);
                str.append("() {\n");
                str.append("setTarget(\"");
                str.append(t.getName());
                str.append("\");\n");
                switch (translationMethod) {
	            	case CUSTOM: str.append("setCustomTranslating();"); break;
	            	case STANDARD: str.append("setTranslating(true);"); break;
	            	case NONE: str.append("setTranslating(false);"); break;
	            }
                str.append("}\n");
                
                str.append("private String sql = null;\n");
                str.append("public String getStatement(java.sql.Connection conn) throws java.sql.SQLException {\n");
                str.append("if( sql == null ) {\n");
                str.append("StringBuilder str = new StringBuilder();\n");
                str.append("str.append(super.getStatement(conn));\n");
                table = getJoinTable(t, jc);
                str.append("str.append(\", \" +");
                str.append("getIdentifier(\"");
                str.append(table);
                str.append("\")");
                str.append(");\n");
                str.append("str.append(\" WHERE \" + getIdentifier(getSqlNameForClassName(\"");
                str.append(t.getName());
                str.append("\"), \"");
                str.append(sField);
                str.append("\"));\n");
                str.append("str.append(\" = \");\n");
                str.append("str.append(getIdentifier(\"");
                str.append(table);
                str.append("\", \"");
                str.append(sField);
                str.append("\"));\n");
                str.append("str.append(\" AND \");\n");
                str.append("str.append(getIdentifier(\"");
                str.append(table);
                str.append("\", \"");
                str.append(sJoin);
                str.append("\"));\n");
                str.append("str.append(\" = ?\");\n");            
                str.append("sql = str.toString();\n");
                str.append("}\n");
                str.append("return sql;\n");
                str.append("}\n");
                str.append("public void prepare(java.util.Map<String,Object> params) throws java.sql.SQLException {\n");
                str.append("prepareFor(\"");
                str.append(join);
                str.append("\", 1, params.get(\"");
                str.append(join);
                str.append("\"), \"");
                str.append(jc.getName());
                str.append("\");\n");
                str.append("}\n");
                str.append("}\n");
                cls = compile(fcn, cname, str.toString());
                synchronized( this ) {
                    joins.put(jcn, cls);
                    notifyAll();
                }
            }
            finally {
                synchronized( this ) {
                    compiling = false;
                    notifyAll();
                }
            }
        }
        finally {
            logger.debug("exit - compileJoin(Class,String,String)");
        }
    }
    
    private void compileJoinCounter(Class<? extends Object> jc, String field, String join) throws PersistenceException {
        String jcn = jc.getName();
        
        synchronized( this ) {
            while( compiling && !joinCounters.containsKey(jcn) ) {
                try { wait(1000L); }
                catch( InterruptedException ignore ) { /* ignore this */ }
            }
            if( joinCounters.containsKey(jcn) ) {
                return;
            }
            compiling = true;
        }
        try {
            Class<? extends Execution> cls;
            StringBuilder str = new StringBuilder();
            Class<T> t = cache.getTarget();
            String fcn, cname, table;
            String[] parts;

            fcn = "org.dasein.persist.runtime.jc.";
            str.append("package org.dasein.persist.runtime.jc");
            parts = t.getName().split("\\.");
            if( parts.length > 1 ) {
                for( int i=0; i<parts.length-1; i++ ) {
                    str.append(".");
                    str.append(parts[i]);
                    fcn += parts[i];
                    fcn += ".";
                }
                cname = parts[parts.length-1];
                fcn += cname;
            }
            else {
                cname = t.getName();
                fcn += cname;
            }
            cname = cname + "_" + field + "_" + join;
            fcn = fcn + "_" + field + "_" + join;
            // we should try to find away to check if the class already exists
            // however, a check for the class in this thread will prevent a further check from
            // finding the class, even after we have compiled it
            str.append(";\n\n");
            str.append("public class ");
            str.append(cname);
            str.append(" extends ");
            str.append(Counter.class.getName());
            str.append(" {\n");
            str.append("public ");
            str.append(cname);
            str.append("() {\n");
            str.append("setTarget(\"");
            str.append(t.getName());
            str.append("\");\n");
            str.append("setTranslating(false);");
            str.append("}\n");
            
            str.append("private String sql = null;\n");
            str.append("public String getStatement(java.sql.Connection conn) throws java.sql.SQLException {\n");
            str.append("if( sql == null ) {\n");
            str.append("StringBuilder str = new StringBuilder();\n");
            str.append("str.append(super.getStatement(conn));\n");
            table = getJoinTable(t, jc);
            str.append("str.append(\", \" +");
            str.append("getIdentifier(\"");
            str.append(table);
            str.append("\")");
            str.append(");\n");
            str.append("str.append(\" WHERE \" + getIdentifier(getTableName(");
            str.append(t.getClass().getName());
            str.append("), \"");
            str.append(field);
            str.append("\"));\n");
            str.append("str.append(\" = \");\n");
            str.append("str.append(getIdentifier(\"");
            str.append(table);
            str.append("\", \"");
            str.append(field);
            str.append("));\n");
            str.append("str.append(\" AND \");\n");
            str.append("str.append(getIdentifier(\"");
            str.append(table);
            str.append("\", \"");
            str.append(key);
            str.append("\"));\n");
            str.append("str.append(\" = ?\");\n");            
            str.append("sql = str.toString();\n");
            str.append("}\n");
            str.append("return sql;\n");
            str.append("}\n");
            
            str.append("}\n");
            cls = compile(fcn, cname, str.toString());
            synchronized( this ) {
                joins.put(jcn, cls);
                notifyAll();
            }
        }
        finally {
            synchronized( this ) {
                compiling = false;
                notifyAll();
            }
        }
    }
    
    private void compileLoader(SearchTerm[] terms, Map<String,Class<? extends Execution>> map, Boolean orderDesc, String ... orderFields) throws PersistenceException {
        logger.debug("enter - compileLoader(String,Map)");
        try {
            String key = getExecKey(terms, orderDesc, orderFields);
            
            synchronized( this ) {
                while( compiling && !map.containsKey(key) ) {
                    try { wait(100L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
                if( map.containsKey(key) ) {
                    return;
                }
                compiling = true;
            }
            try {
                Class<? extends Execution> cls;
                StringBuilder str = new StringBuilder();
                Class<T> t = cache.getTarget();
                String cname, fcn;
                String[] parts;
    
                fcn = "org.dasein.persist.runtime.loaders.";
                str.append("package org.dasein.persist.runtime.loaders");
                parts = t.getName().split("\\.");
                if( parts.length > 1 ) {
                    for( int i=0; i<parts.length-1; i++ ) {
                        str.append(".");
                        str.append(parts[i]);
                        fcn += parts[i];
                        fcn += ".";
                    }
                    cname = parts[parts.length-1];
                    fcn += cname;
                }
                else {
                    cname = t.getName();
                    fcn += cname;
                }
                if( terms != null && terms.length > 0 ) {
                    for( SearchTerm term : terms ) {
                        cname = cname + "_" + term.getColumn();
                        fcn = fcn + "_" + term.getColumn();
                        if( terms.length > 1 ) {
                            cname = cname + "_" + term.getOperator().name();
                            fcn = fcn + "_" + term.getOperator().name();
                        }
                    }
                }
                // we should try to find away to check if the class already exists
                // however, a check for the class in this thread will prevent a further check from
                // finding the class, even after we have compiled it
                str.append(";\n\n");
                str.append("public class ");
                str.append(cname);
                str.append(" extends ");
                str.append(Loader.class.getName());
                str.append(" {\n");
                str.append("public ");
                str.append(cname);
                str.append("() {\n");
                str.append("setTarget(\"");
                str.append(t.getName());
                str.append("\");\n");
                if( terms != null && terms.length > 0 ) {
                    str.append("setCriteria(");
                    if( terms.length == 1 && terms[0].getOperator().equals(Operator.EQUALS) ) {
                        str.append("\"");
                        str.append(terms[0].getColumn());
                        str.append("\"");
                    }
                    else {
                        for( int i=0; i<terms.length; i++ ) {
                            if( i > 0 ) {
                                str.append(", ");
                            }
                            str.append("new Criterion(\"");
                            str.append(terms[i].getColumn());
                            str.append("\", Operator.");
                            str.append(terms[i].getOperator().name());
                            str.append(")");
                        }
                    }
                    str.append(");\n");
                }
                switch (translationMethod) {
	            	case CUSTOM: str.append("setCustomTranslating();"); break;
	            	case STANDARD: str.append("setTranslating(true);"); break;
	            	case NONE: str.append("setTranslating(false);"); break;
	            }
                if( orderDesc != null ) {
                    str.append("setOrder(");
                    if( orderDesc ) {
                        str.append("true");
                    }
                    else {
                        str.append("false");
                    }
                    for( String f : orderFields ) {
                        str.append(", \"");
                        str.append(f);
                        str.append("\"");
                    }
                    str.append(");");
                }
                str.append("}\n");
                str.append("public boolean isReadOnly() { return true; }\n");
                str.append("}\n");
                cls = compile(fcn, cname, str.toString());
                synchronized( this ) {
                    map.put(key, cls);
                    notifyAll();
                }
            }
            finally {
                synchronized( this ) {
                    compiling = false;
                    notifyAll();
                }
            }
        }
        finally {
            logger.debug("exit - compileLoader(String,Map)");
        }
    }
    
    private void compileUpdater() throws PersistenceException {
        synchronized( this ) {
            while( compiling && update == null ) {
                try { wait(1000L); }
                catch( InterruptedException ignore ) { /* ignore this */ }
            }
            if( update != null  ) {
                return;
            }
            compiling = true;
        }
        try {
            Class<? extends Execution> cls;
            StringBuilder str = new StringBuilder();
            Class<T> t = cache.getTarget();
            Field lastModified = null;
            String cname, fcn;
            String[] parts;

            fcn = "org.dasein.persist.runtime.updaters.";
            str.append("package org.dasein.persist.runtime.updaters");
            try {
                lastModified = t.getField("lastModified");
            }
            catch( SecurityException ignore ) {
                // ignore
            }
            catch( NoSuchFieldException ignore ) {
                // ignore
            }
            parts = t.getName().split("\\.");
            if( parts.length > 1 ) {
                for( int i=0; i<parts.length-1; i++ ) {
                    str.append(".");
                    str.append(parts[i]);
                    fcn += parts[i];
                    fcn += ".";
                }
                cname = parts[parts.length-1];
                fcn += cname;
            }
            else {
                cname = t.getName();
                fcn += cname;
            }
            // we should try to find away to check if the class already exists
            // however, a check for the class in this thread will prevent a further check from
            // finding the class, even after we have compiled it
            str.append(";\n\n");
            str.append("public class ");
            str.append(cname);
            str.append(" extends ");
            str.append(Updater.class.getName());
            str.append(" {\n");
            str.append("public ");
            str.append(cname);
            str.append("() {\n");
            str.append("setTarget(\"");
            str.append(t.getName());
            str.append("\");\n");
            switch (translationMethod) {
	        	case CUSTOM: str.append("setCustomTranslating();"); break;
	        	case STANDARD: str.append("setTranslating(true);"); break;
	        	case NONE: str.append("setTranslating(false);"); break;
	        }
            str.append("setCriteria(\"");
            str.append(getKey());
            if( lastModified != null ) {
                str.append(", timestamp");
            }
            str.append("\");\n");
            str.append("}\n");
            str.append("}\n");
            cls = compile(fcn, cname, str.toString());
            synchronized( this ) {
                update = cls;
                notifyAll();
            }
        }
        finally {
            synchronized( this ) {
                compiling = false;
                notifyAll();
            }
        }
    }
    
    /**
     * Counts the total number of objects governed by this factory in the database.
     * @return the number of objects in the database
     * @throws PersistenceException an error occurred counting the elements in the database
     */
    public long count() throws PersistenceException {
        return count((String)null, null);
    }
    
    /**
     * Counts the total number of objects in the database matching the specified criteria.
     * @param field the field to match on
     * @param val the value to match against
     * @return the number of matching objects
     * @throws PersistenceException an error occurred counting the elements in the database
     */
    public long count(String field, Object val) throws PersistenceException {
        logger.debug("enter - count(String,Object)");
        try {
            if( logger.isDebugEnabled() ) {
                logger.debug("For: " + cache.getTarget().getName() + "/" + field + "/" + val);
            }
            
            Class<? extends Execution> cls;
            
            SearchTerm[] terms = new SearchTerm[field == null ? 0 : 1];
            
            if( field != null ) {
                terms[0] = new SearchTerm(field, Operator.EQUALS, val);
            }
            
            String key = getExecKey(terms, false);
            Map<String,Object> params = toParams(terms);
            
            synchronized( this ) {
                while( !counters.containsKey(key) ) {
                	
                    compileCounter(terms, counters);
                    try { wait(1000L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
                cls = counters.get(key);
            }
            if( cls == null ) {
                throw new PersistenceException("Unable to compile a default counter for field: " + field);
            }
            return count(cls, params);
        }
        finally {
            logger.debug("exit - count(String,Object)");
        }
    }
        
    public long count(SearchTerm[] terms) throws PersistenceException {
        logger.debug("enter - count(SearchTerm[])");
        try {
            Class<? extends Execution> cls;
            String key = getExecKey(terms, false);
                        
            synchronized( this ) {
                while( !counters.containsKey(key) ) {
                    compileCounter(terms, counters);
                    try { wait(1000L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
                cls = counters.get(key);
            }        
            if( cls == null ) {
                throw new PersistenceException("No support for counters on " + key);
            }
            Map<String,Object> params = toParams(terms);
            
            return count(cls, params);
        }
        finally {
            logger.debug("exit - count(SearchTerm[])");
        }
    }
    
    /**
     * Counts the number of items matching an arbitrary query. This method expects the passed in
     * query to have one field called 'count' in the map it returns. Anything else is ignored
     * @param cls the execution class for the arbitrary query
     * @param criteria the criteria to match against for the query
     * @return the number of matches (essentially, the number returned from the arbitrary query in the 'count' field)
     * @throws PersistenceException an error occurred executing the query.
     */
    public long count(Class<? extends Execution> cls, Map<String,Object> criteria) throws PersistenceException {
        logger.debug("enter - count(Class,Map)");
        if( logger.isDebugEnabled() ) {
            logger.debug("For: " + cache.getTarget().getName() + "/" + cls + "/" + criteria);
        }
        try {
            Transaction xaction = Transaction.getInstance(true);
            
            try {
                long count = 0L;
    
                criteria = xaction.execute(cls, criteria);
                count = ((Number)criteria.get("count")).longValue();
                xaction.commit();
                return count;
            }
            finally {
                xaction.rollback();
            }
        }
        finally {
            logger.debug("exit - count(Class,Map)");
        }
    }
    
    public long countJoin(Class<? extends Object> jc, String key, Object val) throws PersistenceException {
        HashMap<String,Object> criteria;
        Class<? extends Execution> cls;
        String jcn;
        
        jcn = jc.getName();
        criteria = new HashMap<String,Object>();
        criteria.put(key, val);
        synchronized( this ) {
            while( !joinCounters.containsKey(jcn) ) {
                this.compileJoinCounter(jc, getKey(), key);
                try { wait(1000L); }
                catch( InterruptedException ignore ) { /* ignore this */ }
            }
            cls = joinCounters.get(jcn);
        }
        if( cls == null ) {
            throw new PersistenceException("Unable to compile a default join counter for: " + jcn);
        }
        return count(cls, criteria);
    }
    
    /**
     * Creates the specified object with the data provided in the specified state under
     * the governance of the specified transaction.
     * @param xaction the transaction governing this event
     * @param state the new state for the new object
     * @throws PersistenceException an error occurred talking to the data store, or
     * creates are not supported
     */
    public T create(Transaction xaction, Map<String,Object> state) throws PersistenceException {
        if( create == null ) {
            synchronized( this ) {
                while( create == null ) {
                    compileCreator();
                    try { wait(1000L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
            }
        }
        state.put("--key--", getKey());
        xaction.execute(create, state);
        if( dependency != null ) {
            dependency.createDependencies(xaction, state);
        }
        return cache.find(state);
    }
    
    /**
     * Executes a search that may return multiple values. The specified field
     * must have been set up by a call to @{link #addSearch(String,Class)}.
     * @param field the field being searched on
     * @param val the value being searched against
     * @return a list of objects that match the criteria
     * @throws PersistenceException an error occurred talking to the data store
     */
    public Collection<T> find(String field, Object val) throws PersistenceException {
        logger.debug("enter - find(String,Object");
        try {
            return find(field, val, null);
        }
        finally {
            logger.debug("exit - find(String,Object)");
        }
    }

    public Collection<T> find(String field, Object val, Boolean orderDesc, String ... orderFields) throws PersistenceException {
        logger.debug("enter - find(String, Object, Boolean, String...)");
        try {
            SearchTerm[] terms = new SearchTerm[field == null ? 0 : 1];
       
            if( field != null ) {
                terms[0] = new SearchTerm(field, Operator.EQUALS, val);
            }
            return find(terms, orderDesc, orderFields);
        }
        finally {
            logger.debug("exit - find(String, Object, Boolean, String...)");            
        }
    }
    
    public Collection<T> find(SearchTerm ... terms) throws PersistenceException {
        return find(terms, null);
    }
    
    public Collection<T> find(SearchTerm[] terms, Boolean orderDesc, String ... orderFields) throws PersistenceException {
        logger.debug("enter - find(SearchTerm[], Boolean, String...)");
        try {
            return find(terms, null, orderDesc, orderFields);
        }
        finally {
            logger.debug("exit - find(SearchTerm[], Boolean, String...)");
        }
    }
    
    public Collection<T> find(SearchTerm[] terms, JiteratorFilter<T> filter, Boolean orderDesc, String ... orderFields) throws PersistenceException {
        logger.debug("enter - find(SearchTerm[], Boolean, String...)");
        try {
            Class<? extends Execution> cls;
            String key = getExecKey(terms, orderDesc, orderFields);
            
            synchronized( this ) {
                while( !searches.containsKey(key) ) {
                    compileLoader(terms, searches, orderDesc, orderFields);
                    try { wait(1000L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
                cls = searches.get(key);
            }        
            if( cls == null ) {
                throw new PersistenceException("No support for searches on " + key);
            }
            return load(cls, filter, terms);
        }
        finally {
            logger.debug("exit - find(SearchTerm[], Boolean, String...)");
        }
    }
    
    public Collection<T> find(Class<? extends Object> jc, String key, Object id) throws PersistenceException {
        logger.debug("enter - find(Class,String,Object)");
        if( logger.isDebugEnabled() ) {
            logger.debug("For: " + cache.getTarget().getClass() + "/" + jc + "/" + key + "/" + id);
        }
        try {
            String cname = jc.getName();
            Class<? extends Execution> cls;
            SearchTerm[] terms;
            
            synchronized( this ) {
                while( !joins.containsKey(cname) ) {
                    compileJoin(jc, getKey(), key);
                    try { wait(1000L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
                cls = joins.get(cname);
            }
            if( cls == null ) {
                throw new PersistenceException("No support for joins on " + cname);
            }
            terms = new SearchTerm[1];
            terms[0] = new SearchTerm(key, Operator.EQUALS, id);
            return load(cls, null, terms);
        }
        finally {
            logger.debug("exit - find(Class,String,Object)");
        }
    }
    
    /**
     * Executes an arbitrary search using the passed in search class and criteria. This is useful
     * for searches that simply do not fit well into the rest of the API.
     * @param cls the class to perform the search
     * @param criteria the search criteria
     * @return the results of the search
     * @throws PersistenceException an error occurred performing the search
     */
    public Collection<T> find(Class<? extends Execution> cls, Map<String,Object> criteria) throws PersistenceException {
        logger.debug("enter - find(Class,Map)");
        try {
            Collection<String> keys = criteria.keySet();
            SearchTerm[] terms = new SearchTerm[keys.size()];
            int i = 0;
            
            for( String key : keys ) {
                terms[i++] = new SearchTerm(key, Operator.EQUALS, criteria.get(key));
            }
            return load(cls, null, terms);
        }
        finally {
            logger.debug("exit - find(Class,Map)");
        }
    }
    
    public T get(Map<String,Object> state) throws PersistenceException {
        logger.debug("enter - get(Map)");
        try {
            return cache.find(state);
        }
        finally {
            logger.debug("exit - get(Map)");
        }
    }
    
    /**
     * Retrieves the object uniquely identified by the value for the specified ID field.
     * @param id the ID field identifying the object
     * @param val the value that uniquely identifies the desired object
     * @return the object matching the query criterion
     * @throws PersistenceException an error occurred talking to the data store
     */
    public T get(String id, Object val) throws PersistenceException {
        logger.debug("enter - get(String,Object)");
        if( logger.isDebugEnabled() ) {
            logger.debug("For: " + cache.getTarget().getName() + "/" + id + "/" + val);
        }
        try {
            Class<? extends Execution> cls;
            CacheLoader<T> loader;
            
            loader = new CacheLoader<T>() {
                public T load(Object ... args) {
                    SearchTerm[] terms = new SearchTerm[1];
                    Collection<T> list;
                    
                    terms[0] = new SearchTerm((String)args[0], Operator.EQUALS, args[1]);
                    try {
                        list = PersistentFactory.this.load(singletons.get(args[0]), null, terms);
                    }
                    catch( PersistenceException e ) {
                        try {
                            try { Thread.sleep(1000L); }
                            catch( InterruptedException ignore ) { }
                            list = PersistentFactory.this.load(singletons.get(args[0]), null, terms);
                        }
                        catch( Throwable forgetIt ) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }
                    if( list.isEmpty() ) {
                        return null;
                    }
                    return list.iterator().next();
                }
            };
            synchronized( this ) {
                while( !singletons.containsKey(id) ) {
                    SearchTerm[] terms = new SearchTerm[1];
                    
                    terms[0] = new SearchTerm(id, Operator.EQUALS, val);
                    compileLoader(terms, singletons, null);
                    try { wait(100L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
                cls = singletons.get(id);
            }            
            if( cls == null ) {
                throw new PersistenceException("Queries on the field " + id + " will not return single values.");
            }
            logger.debug("Executing cache find...");
            try {
                return cache.find(id, val, loader, id, val);
            }
            catch( CacheManagementException e ) {
                throw new PersistenceException(e);
            }
            catch( RuntimeException e ) {
                Throwable t = e.getCause();
                
                if( t != null && t instanceof PersistenceException ) {
                    throw (PersistenceException)t;
                }
                if( logger.isDebugEnabled() ) {
                    e.printStackTrace();
                }
                throw new PersistenceException(e);
            }
            finally {
                logger.debug("Executed.");
            }
        }
        finally {
            logger.debug("exit - get(String,Object)");
        }
    }    
    
    private String getExecKey(SearchTerm[] terms, Boolean orderDesc, String ... orderFields) {
        StringBuilder key = new StringBuilder();
        
        for( int i =0; i<terms.length; i++ ) {
            SearchTerm term = terms[i];
            
            if( i > 0 ) {
                key.append("/");
            }
            key.append(term.getColumn());
            if( !term.getOperator().equals(Operator.EQUALS) ) {
                key.append("-");
                key.append(term.getOperator().name());
            }
        }
        if( orderDesc != null ) {
            key.append(":");
            key.append(orderDesc ? "true" : "false");
            for( String of : orderFields ) {
                key.append(":");
                key.append(of);
            }
        }
        return key.toString();
    }
    
    private String getJoinTable(Class<T> c1, Class<? extends Object> c2) {
        String cname1 = getSqlName(c1);
        String cname2 = getSqlName(c2);
        int x = cname1.compareTo(cname2);
        
        if( x < 0 ) {
            return cname1 + "_" + cname2;
        }
        else {
            return cname2 + "_" + cname1;
        }
    }
    
    public String getKey() {
        return key;
    }

    public long getNewKeyValue() throws PersistenceException {
        return Sequencer.getInstance(cache.getTarget().getName() + "." + getKey()).next();
    }
    
    private String getSqlName(Class<? extends Object> cls) {
        String cname = cls.getName();
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
    
    private String getSqlName(String nom) {
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
    
    /**
     * Loads all elements of this class from the data store. Use this method only when you know
     * exactly what you are doing. Otherwise, you will pull a lot of data.
     * @return all objects from the database
     * @throws PersistenceException an error occurred executing the query
     */
    public Collection<T> list() throws PersistenceException {
        logger.debug("enter - list()");
        try {
            return find((String)null, (Object)null);
        }
        finally {
            logger.debug("exit - list()");
        }
    }
    
    public Collection<T> list(boolean orderDesc, String ... orderFields) throws PersistenceException {
        return find((String)null, (Object)null, orderDesc, orderFields);
    }
    
    public Collection<T> list(Class<? extends Execution> cls) throws PersistenceException {
        logger.debug("enter - list(Class)");
        try {
            HashMap<String,Object> criteria = new HashMap<String,Object>();
        
            return find(cls, criteria);
        }
        finally {
            logger.debug("exit - list(Class)");
        }
    }
    
    private Map<String,Object> toParams(SearchTerm ... searchTerms) {
        HashMap<String,Object> params = new HashMap<String,Object>();
        
        for( SearchTerm term : searchTerms ) {
            params.put(term.getColumn(), term.getValue());
        }
        return params;
    }
    
    @SuppressWarnings("unchecked")
    private Collection<T> load(Class<? extends Execution> cls, JiteratorFilter<T> filter, SearchTerm ... usingTerms) throws PersistenceException {
        logger.debug("enter - load(Class,SearchTerm...)");
        try {
            Map<String,Object> params = toParams(usingTerms);
            Transaction xaction = Transaction.getInstance(true);
            final Jiterator<T> it = new Jiterator<T>(filter);

            params.put("--key--", getKey());
            try {
                final Map<String,Object> results;
                
                results = xaction.execute(cls, params);
                xaction.commit();
                Thread t = new Thread() {
                    public void run() {
                        try {
                            for( Map<String,Object> map: (Collection<Map<String,Object>>)results.get(LISTING) ) {
                                T item = null;
                                
                                if( singletons.size() > 0 ) {
                                    for( String key : singletons.keySet() ) {
                                        Object ob = map.get(key);
                                        
                                        if( ob instanceof java.math.BigDecimal ) {
                                            java.math.BigDecimal tmp = (java.math.BigDecimal)ob;
                                            
                                            ob = tmp.longValue();
                                            map.put(key, ob);
                                        }
                                        item = cache.find(key, ob);
                                        if( item != null ) {
                                            break;
                                        }
                                    }
                                }
                                if( item == null ) {
                                    if( dependency != null ) {
                                        try {
                                            dependency.loadDependencies(map);
                                        }
                                        catch( PersistenceException e ) {
                                            it.setLoadException(e);
                                            return;
                                        }
                                    }
                                    item = cache.find(map);
                                }
                                /*
                                if( dependency != null ) {
                                    if( singletons.size() > 0 ) {
                                        for( String key : singletons.keySet() ) {
                                            Object ob = map.get(key);
                                            
                                            if( ob.getClass().getName().equals(java.math.BigDecimal.class.getName()) ) {
                                                java.math.BigDecimal tmp = (java.math.BigDecimal)ob;
                                                
                                                ob = tmp.longValue();
                                                map.put(key, ob);
                                            }
                                            item = cache.find(key, ob);
                                            if( item != null ) {
                                                break;
                                            }
                                        }
                                    }
                                    if( item == null ) {
                                        try {
                                            dependency.loadDependencies(map);
                                        }
                                        catch( PersistenceException e ) {
                                            it.setLoadException(e);
                                            return;
                                        }
                                    }
                                }
                                if( item == null ) {
                                    item = cache.find(map);
                                }
                                */
                                it.push(item);
                            }
                            it.complete();
                        }
                        catch( Exception e ) {
                            it.setLoadException(e);
                        }
                        catch( Throwable t ) {
                            it.setLoadException(new RuntimeException(t));
                        }
                    }
                };
                
                t.setDaemon(true);
                t.setName("Loader");
                t.start();
                return new JitCollection<T>(it, cache.getTarget().getName());
            }
            catch( PersistenceException e ) {
                it.setLoadException(e);
                throw e;
            }
            catch( RuntimeException e ) {
                it.setLoadException(e);
                throw e;
            }
            catch( Throwable t ) {
                RuntimeException e = new RuntimeException(t);
                
                it.setLoadException(e);
                throw e;
            }
            finally {
                xaction.rollback();
            }
        }
        finally {
            logger.debug("exit - load(Class,Map)");
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String,Translator<String>> loadTranslations(Transaction xaction, String idstr) throws PersistenceException {
        logger.debug("enter - loadTranslations(Transaction,String)");
        if( logger.isDebugEnabled() ) {
            logger.debug("For: " + cache.getTarget().getName() + "/" + idstr);
        }
        try {
            Map<String,Object> criteria = new HashMap<String,Object>();
            Map<String,Translator<String>> map = new HashMap<String,Translator<String>>();
            Class<T> cls = cache.getTarget();
            
            criteria.put("ownerClass", cls);
            criteria.put("ownerId", idstr);
            criteria = xaction.execute(LoadTranslator.class, criteria, Execution.getDataSourceName(cls.getName()));
            // a retarded side-effect of the lame-ass implementation of generics in Java
            for( String attr : criteria.keySet() ) {
                Object trans = criteria.get(attr);
                
                map.put(attr, (Translator<String>)trans);
            }
            return map;        
        }
        finally {
            logger.debug("exit - loadTranslations(Transaction,String)");
        }
    }
    
    public void loadXml(InputStream in) throws PersistenceException {
        XMLReader<T> reader = new XMLReader<T>();
        
        for( Map<String,Object> state : reader.read(in, cache.getTarget()) ) {
            Transaction xaction = Transaction.getInstance();
            
            try {
                String k = getKey();
                Object id;
                T target;
    
                id = state.get(k);
                if( id == null ) {
                    throw new PersistenceException("No value specified for key: " + k);
                }
                target = get(k, id);
                if( target == null ) {
                    if( importHook == null || importHook.prepareCreate(xaction, state) ) {
                        create(xaction, state);
                    }
                }
                else {
                    // the import hook is checked in the synchronize method
                    synchronize(xaction, target, state);
                }
                xaction.commit();
            }
            finally {
                xaction.rollback();
            }
        }            
    }
    
    static private File mkdir(String tmpdir, String fcn) {
        File f = new File(tmpdir);
        String[] parts;
        
        parts = fcn.split("\\.");
        if( parts.length < 2 ) {
            return new File(tmpdir);
        }
        for( int i=0; i<parts.length-1; i++ ) {
            tmpdir = f.getAbsolutePath();
            if( !tmpdir.endsWith("/") ) {
                tmpdir = tmpdir + "/";
            }
            tmpdir = tmpdir + parts[i];
            f = new File(tmpdir);
            if( !f.exists() ) {
                if( !f.mkdir() ) {
                    throw new RuntimeException("Failed to make directory: " + tmpdir);
                }
            }
        }
        return f;
    }
    
    /**
     * Removes the specified item from the system permanently.
     * @param xaction the transaction under which this event is occurring
     * @param item the item to be removed
     * @throws PersistenceException an error occurred talking to the data store or
     * removal of these objects is prohibited
     */
    public void remove(Transaction xaction, T item) throws PersistenceException {
        Map<String,Object> keys = cache.getKeys(item);
        
        if( remove == null ) {
            synchronized( this ) {
                while( remove == null ) {
                    compileDeleter();
                    try { wait(1000L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
            }
        }
        xaction.execute(remove, keys);
        if( dependency != null ) {
            dependency.removeDependencies(xaction, keys);
        }
        cache.release(item);
    }
    
    public void removeTranslations(Transaction xaction, String idstr) throws PersistenceException {
        Map<String,Object> state = new HashMap<String,Object>();
        Class<T> cls = cache.getTarget();
        
        state.put("ownerClass", cls);
        state.put("ownerId", idstr);
        xaction.execute(RemoveTranslator.class, state, Execution.getDataSourceName(cls.getName()));
    }
    
    public void saveTranslation(Transaction xaction, String idstr, String attr, Translator<String> val) throws PersistenceException {
        Map<String,Object> state = new HashMap<String,Object>();
        String cname = cache.getTarget().getName();
        
        state.put("ownerClass", cname);
        state.put("ownerId", idstr);
        state.put("attribute", attr);
        state.put("translation", val);
        xaction.execute(SaveTranslator.class, state, Execution.getDataSourceName(cname));        
    }
    
    /**
     * Sets the class that manages the query that will create objects in this factory
     * in the data store.
     * @param cls the execution class that creates objects in the data store
     */
    public void setCreate(Class<? extends Execution> cls) {
        create = cls;
    }
    
    /** 
     * Sets the callback class to handle the management of dependencies.
     * @param mgr the dependency manager to use for dependency management
     */
    public void setDependency(DependencyManager<T> mgr) {
        dependency = mgr;
    }
    
    public void setExportHook(ExportHook<T> hook) {
        exportHook = hook;
    }
    
    public void setImportHook(ImportHook<T> hook) {
        importHook = hook;
    }
    
    /**
     * Sets the class that manages the query that will remove objects in this factory
     * from the data store.
     * @param cls the execution class that removes objects from the data store
     */
    public void setRemove(Class<? extends Execution> cls) {
        remove = cls;
    }
    
    /**
     * Sets the class that manages the query that will update objects in this factory
     * in the data store.
     * @param cls the execution class that updates objects in the data store
     */
    public void setUpdate(Class<? extends Execution> cls) {
        update = cls;
    }
    
    private void synchronize(Transaction xaction, T target, Map<String,Object> state) throws PersistenceException {
        Class<T> t = cache.getTarget();
        
        try {
            Method lastModified = t.getMethod("getLastModified", new Class[0]);
            
            if( lastModified != null ) {
                Number lm = (Number)lastModified.invoke(target, new Object[0]);
                Number ts = (Number)state.get("lastModified");

                if( ts != null && lm != null ) {
                    if( ts.longValue() == lm.longValue() ) {
                        // assume general equivalence
                        return;
                    }
                    else if( ts.longValue() < lm.longValue() ) {
                        System.err.println("Potential conflict in " + t.getName() + " (" + lm + ") factory for " + state);
                        return;
                    }
                }
            }
        }
        catch( SecurityException ignore ) {
            // ignore
        }
        catch( IllegalArgumentException e ) {
            // ignore
        }
        catch( IllegalAccessException e ) {
            // ignore
        }
        catch( NoSuchMethodException e ) {
            // ignore
        }
        catch( InvocationTargetException e ) {
            // ignore
        }
        if( importHook == null || importHook.prepareUpdate(xaction, state) ) {
            Memento<T> memento = new Memento<T>(target);
            
            memento.save(state);
            state = memento.getState();
            update(xaction, target, state);
        }
    }
    
    public String toString() {
        return cache.toString();
    }
    
    /**
     * Updates the specified object with the data provided in the specified state under
     * the governance of the specified transaction.
     * @param xaction the transaction governing this event
     * @param item the item to be updated
     * @param state the new state for the updated object
     * @throws PersistenceException an error occurred talking to the data store, or
     * updates are not supported
     */
    public void update(Transaction xaction, T item, Map<String,Object> state) throws PersistenceException {
        if( update == null ) {
            synchronized( this ) {
                while( update == null ) {
                    compileUpdater();
                    try { wait(1000L); }
                    catch( InterruptedException ignore ) { /* ignore this */ }
                }
            }
        }        
        state.put("--key--", getKey());
        xaction.execute(update, state);
        if( dependency != null ) {
            dependency.updateDependencies(xaction, item, state);
        }
    }    
    
    public void write(File file, String data) throws IOException {
        File prnt, backup;
        
        prnt = file.getParentFile();
        if( !prnt.exists() ) {
            prnt.mkdirs();
        }        
        if( file.exists() ) {
            backup = new File(file.getAbsolutePath() + ".bak");
            if( backup.exists() ) {
                backup.delete();
            }
            if( !file.renameTo(backup) ) {
                throw new IOException("Unable to save backup: " + backup.getAbsolutePath());
            }
        }
        else {
            backup = null;
        }
        try {
            FileOutputStream fos = new FileOutputStream(file);
            BufferedWriter writer;

            writer = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));
            writer.write(data);
            writer.flush();
            writer.close();
            if( backup != null && backup.exists() ) {
                backup.delete();
                backup = null;
            }
            file = null;
        }
        finally {
            if( (backup != null) && backup.exists() && (file != null) ) {
                if( file.exists() ) {
                    file.delete();
                }
                backup.renameTo(file);
            }
        }        
    }
    
    public void write(File file, InputStream is) throws IOException {
        File prnt, backup;
        
        prnt = file.getParentFile();
        if( !prnt.exists() ) {
            prnt.mkdirs();
        }        
        if( file.exists() ) {
            backup = new File(file.getAbsolutePath() + ".bak");
            if( backup.exists() ) {
                backup.delete();
            }
            if( !file.renameTo(backup) ) {
                throw new IOException("Unable to save backup: " + backup.getAbsolutePath());
            }
        }
        else {
            backup = null;
        }
        try {
            BufferedInputStream in = new BufferedInputStream(is);
            FileOutputStream os = new FileOutputStream(file);
            BufferedOutputStream out = new BufferedOutputStream(os);
            int c = in.read();

            while( c != -1 ) {
                out.write(c);
                c = in.read();
            }
            out.flush();
            in.close();
            out.close();
            if( backup != null && backup.exists() ) {
                backup.delete();
                backup = null;
            }
            file = null;
        }
        finally {
            if( (backup != null) && backup.exists() && (file != null) ) {
                if( file.exists() ) {
                    file.delete();
                }
                backup.renameTo(file);
            }
        }        
    }
    
    public void writeXml(PrintWriter output) throws PersistenceException {
        // loading everything into a Jiterator frees memory quicker
        Jiterator<T> it = new Jiterator<T>(list());
        
        for( T item : it ) {
            writeXml(output, item);
            output.print("\n");
        }
    }

    public void writeXml(PrintWriter output, T item) throws PersistenceException {
        if( exportHook == null || exportHook.prepareExport(output, item) ) {
            Map<String,Object> keys = cache.getKeys(item);
            Object id = keys.get(getKey());
            
            try {
                XMLWriter<T> writer = new XMLWriter<T>();
            
                if( id != null ) {
                    writer.write(output, item, id.toString());
                }
                else {
                    writer.write(output, item);
                }
            }
            catch( IOException e ) {
                e.printStackTrace();
                throw new PersistenceException(e);
            }
        }
    }
    
    public void writeXml(PrintWriter output, String field, Object val) throws PersistenceException {
        // loading everything into a Jiterator frees memory quicker
        Jiterator<T> it = new Jiterator<T>(find(field, val));
        
        for( T item : it ) {
            writeXml(output, item);
            output.print("\n");
        }
    }
    
    public void writeXml(PrintWriter output, Class<? extends Execution> cls, Map<String,Object> criteria) throws PersistenceException {
        // loading everything into a Jiterator frees memory quicker
        Jiterator<T> it = new Jiterator<T>(find(cls, criteria));
        
        for( T item : it ) {
            writeXml(output, item);
            output.print("\n");
        }
    }    
}
