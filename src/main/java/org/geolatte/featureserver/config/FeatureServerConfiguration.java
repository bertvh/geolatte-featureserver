/*
 * This file is part of the GeoLatte project. This code is licenced under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the
 * License.
 *
 * Copyright (C) 2010 - 2010 and Ownership of code is shared by:
 * Qmino bvba - Romeinsestraat 18 - 3001 Heverlee  (http://www.Qmino.com)
 * Geovise bvba - Generaal Eisenhowerlei 9 - 2140 Antwerpen (http://www.geovise.com)
 */

package org.geolatte.featureserver.config;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.geolatte.core.expressions.Expression;
import org.geolatte.core.expressions.Expressions;
import org.geolatte.core.expressions.Filter;
import org.geolatte.core.expressions.FilterTransformation;
import org.geolatte.core.transformer.ClosedTransformerChain;
import org.geolatte.core.transformer.SimpleTransformerSink;
import org.geolatte.core.transformer.SimpleTransformerSource;
import org.geolatte.core.transformer.TransformerChainFactory;

import java.util.*;

/**
 * This class (re)reads the mapping-configuration for the featureserver and offers it in a convenient format for
 * the application. It is implemented as a singleton class.
 * <p/>
 * <i>Creation-Date</i>: 1-jul-2010<br>
 * <i>Creation-Time</i>: 11:05:24<br>
 * </p>
 *
 * @author Yves Vandewoude
 * @author <a href="http://www.qmino.com">Qmino bvba</a>
 * @since SDK1.5
 */
public class FeatureServerConfiguration {

    private static final Logger LOGGER = LogManager.getLogger(FeatureServerConfiguration.class);
    private static String STD_CONFIG_FILE_NAME = "FeatureServerConfiguration.xml";

    private static String STD_CONFIG_FILE_PROPERTY_NAME = "geolatte.fs.config";

    private boolean error = false;
    private String errorMessage = null;
    private List<String> includeRules;
    private List<String> excludeRules;
    private Map<String, String> hibernateProperties = new HashMap<String, String>();

    private String dbaseSchema = null;
    private String propertyFileName;

    /**
     * Returns the instance of the featureserver configuration. This method will return a configurationobject, even if
     * the parsing of the underlying xml file failed. To ensure that there are no errors, the caller can call the isInvalid
     * method on the resulting object. If it returns true, all methods called on the object will result in a ConfgurationException.
     * The reason for the failure can be requested by the getErrorMessage method.
     *
     * @return the single instance for this configuration.
     */
    public static FeatureServerConfiguration getInstance() {
        return SingletonHolder.INSTANCE;
    }

    /**
     * Lazy and threadsafe implementation of singleton. Solution of Bill Pugh.
     */
    private static class SingletonHolder {
        private static final FeatureServerConfiguration INSTANCE = new FeatureServerConfiguration();
    }

    /**
     * Will read the configuration file. Note that this method endangeres the threadsafety of this class, as reparsing is
     * basically impossible to combine with real threadsafe singleton. During the reparse, the entity is temporary in an
     * error state (all methods will return a ConfigurationException).
     *
     * @throws ConfigurationException if something went wrong during the parse of the configurationfile.
     */
    public static void configure()
            throws ConfigurationException {
        SingletonHolder.INSTANCE.reparse();
    }

    /**
     * Private constructor, requires the name of the configurationfile.
     *
     */
    private FeatureServerConfiguration() {
        try {
            reparse();
        } catch (ConfigurationException e) {
            error = true;
            errorMessage = e.getMessage();
            throw e;
        }
    }

    /**
     * Reads the properties from the system environment to set the location of the configurationfile.
     */
    private void setProperties() {
        propertyFileName = System.getProperty(STD_CONFIG_FILE_PROPERTY_NAME);
        if (propertyFileName == null) {
            propertyFileName = System.getenv(STD_CONFIG_FILE_PROPERTY_NAME);
        }
        if (propertyFileName == null) {
            throw new ConfigurationException("No property file defined.");
        }
    }

    /**
     * Reparses the configurationfile
     *
     * @throws ConfigurationException if something went wrong parsing the file
     */
    protected void reparse()
            throws ConfigurationException {
        error = true;
        setProperties();
        includeRules = new ArrayList<String>();
        excludeRules = new ArrayList<String>();
        try {
            SAXReader reader = new SAXReader();
            Document document = reader.read(propertyFileName);
            if (document == null) {
                throw new ConfigurationException(String.format("No such properties file found: %s", propertyFileName));
            }

            List includes = document.selectNodes( "//FeatureServerConfig/Mapping/Tables/Include/Item" );
            List excludes = document.selectNodes( "//FeatureServerConfig/Mapping/Tables/Exclude/Item");

            for (int i = 0; i < includes.size(); i++) {
                Element el = (Element)includes.get(i);
                includeRules.add(el.getTextTrim());
                LOGGER.info(String.format("Include rule added: \"%s\"", el.getTextTrim()));
            }
            for (int i = 0; i < excludes.size(); i++) {
                 Element el = (Element)includes.get(i);
                excludeRules.add(el.getTextTrim());
                LOGGER.info(String.format("Exclude rule added: %s", el.getTextTrim()));
            }
            List hibernateConfigProps = document.selectNodes("//FeatureServerConfig/HibernateConfiguration/property");
            hibernateProperties.clear();
            for (int i = 0; i < hibernateConfigProps.size(); i++) {
                Element el = (Element)hibernateConfigProps.get(i);
                String propertyName = el.attributeValue("name");
                if (!propertyName.startsWith("hibernate")) {
                    propertyName = "hibernate." + propertyName;
                }
                String propertyValue = el.getTextTrim();
                hibernateProperties.put(propertyName, propertyValue);
            }
            Node schema = document.selectSingleNode("//FeatureServerConfig/Mapping/Tables/Schema");
            if (schema != null) {
                dbaseSchema = schema.getText();
                LOGGER.info(String.format("Schema is: %s", dbaseSchema));
            } else {
                dbaseSchema = null;
                LOGGER.info(String.format("No schema specified."));
            }
            errorMessage = null;
            error = false;
        } catch (DocumentException e) {
            LOGGER.error("Error reading configuration file: ", e);
            errorMessage = e.getMessage();
            throw new ConfigurationException("Error parsing the configurationfile: " + e.getMessage(), e);
        }
    }

    /**
     * Returns the list of tables that should be included according to this mappingconfiguration. If the configuration
     * contains an error, an empty list is returned.
     *
     * @param allTables the list of tables to check
     * @return the names of the tables that pass the include/excludes
     * @throws ConfigurationException (unchecked) if the configuration is invalid.
     */
    public List<String> includedTables(List<String> allTables)
            throws ConfigurationException {
        if (isInvalid()) {
            throw new ConfigurationException("Configuration invalid: " +  getErrorMessage());
        }
        Expression<Boolean> includeExpression = Expressions.constant(false);
        for (String includeRule : includeRules) {
            includeExpression = Expressions.or(includeExpression, Expressions.like(Expressions.stringProperty(null), Expressions.constant(includeRule), '*'));
        }
        Expression<Boolean> excludeExpression = Expressions.constant(true);
        for (String excludeRule : excludeRules) {
            excludeExpression = Expressions.and(excludeExpression, Expressions.notLike(Expressions.stringProperty(null), Expressions.constant(excludeRule), '*'));
        }
        Filter ourFilter = new Filter(Expressions.and(includeExpression, excludeExpression));
        SimpleTransformerSink<String> mySink = new SimpleTransformerSink<String>();
        ClosedTransformerChain myChain = TransformerChainFactory.<String, String>newChain().add(new SimpleTransformerSource<String>(allTables))
                .addFilter(new FilterTransformation<String>(ourFilter)).last(mySink);
        myChain.run();
        return mySink.getCollectedOutput();
    }


    /**
     * Returns the name of the database scheme to use
     *
     * @return the name of the schema to use
     * @throws ConfigurationException (unchecked) if this configuration object is invalid.
     */
    public String getDbaseSchema() {
        if (isInvalid()) {
            throw new ConfigurationException("Configuration invalid: " +  getErrorMessage());
        }
        return dbaseSchema;
    }


    /**
     * @return all hibernate property-names in the configuration file
     * @throws ConfigurationException (unchecked) if this configuration object is invalid.
     */
    public Collection<String> getHibernateProperties() {
        if (isInvalid()) {
            throw new ConfigurationException("Configuration invalid: " +  getErrorMessage());
        }
        return hibernateProperties.keySet();
    }

    /**
     * @param propertyName the name of the property to retrieve
     * @return the value of the hibernate property with the given name. if the property does not exist, or this
     *         configuration is invalid, null is returned.
     * @throws ConfigurationException (unchecked) if this configuration object is invalid.
     */
    public String getHibernateProperty(String propertyName) {
        if (isInvalid()) {
            throw new ConfigurationException("Configuration invalid: " + getErrorMessage());
        }
        return hibernateProperties.get(propertyName);
    }

    /**
     * @return whether this configuration is currently invalid. A configuration is invalid if the underlying XML file
     *         does not parse, or if the configuration is in the middle of a reparse.
     */
    public boolean isInvalid() {
        return error;
    }

    /**
     * Returns the reason why this configuration is in errormode
     * @return the reason why this configuration is in error
     */
    public String getErrorMessage() {
        return errorMessage;
    }
}