/*
 * This file is part of the GeoLatte project.
 *
 *     GeoLatte is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     GeoLatte is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with GeoLatte.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2010 - 2011 and Ownership of code is shared by:
 * Qmino bvba - Esperantolaan 4 - 3001 Heverlee  (http://www.qmino.com)
 * Geovise bvba - Generaal Eisenhowerlei 9 - 2140 Antwerpen (http://www.geovise.com)
 */

package org.geolatte.featureserver.dbase;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.geolatte.featureserver.config.FeatureServerConfiguration;
import org.hibernate.*;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernatespatial.cfg.HSConfiguration;
import org.hibernatespatial.pojo.AutoMapper;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/**
 * Database facade class. Creates readers for the different tables and retrieves meta-information.
 * <p>
 * <i>Creation-Date</i>: 9-apr-2010<br>
 * <i>Creation-Time</i>:  11:48:54<br>
 * </p>
 *
 * @author Yves Vandewoude
 * @author <a href="http://www.qmino.com">Qmino bvba</a>
 * @since SDK1.5
 */
public class DbaseFacade {

    private static DbaseFacade instance;
    private SessionFactory sessionFactory;
    private List<String> mappedTables;
    private static final Logger LOGGER = LogManager.getLogger(DbaseFacade.class);    

    /**
     * Private constructor of the database facade. Maps all tables currently present in the database!
     *
     * @throws java.sql.SQLException If retrieval of the database classes, or mapping of the tables was unsuccesfull.
     */
    private DbaseFacade() throws SQLException {
        if (FeatureServerConfiguration.getInstance() == null || FeatureServerConfiguration.getInstance().isInvalid())
        {
            throw new SQLException("Invalid FeatureServer configuration");
        }
        String connectionString = FeatureServerConfiguration.getInstance().getHibernateProperty("hibernate.connection.url");
        String user = FeatureServerConfiguration.getInstance().getHibernateProperty("hibernate.connection.username");
        String password = FeatureServerConfiguration.getInstance().getHibernateProperty("hibernate.connection.password");
        Connection dbConnection = DriverManager.getConnection(connectionString, user, password);
        DatabaseMetaData databaseMetaData = dbConnection.getMetaData();
        String schema = FeatureServerConfiguration.getInstance().getDbaseSchema();
        ResultSet resultSet = databaseMetaData.getTables(null, schema, null, new String[]{"TABLE", "VIEW"});
        ArrayList<String> names = new ArrayList<String>();
        while (resultSet.next()) {
            names.add(resultSet.getString("TABLE_NAME"));
        }
        Configuration newConfig = new Configuration();
        for (String property: FeatureServerConfiguration.getInstance().getHibernateProperties())
        {
            newConfig.setProperty(property, FeatureServerConfiguration.getInstance().getHibernateProperty(property));
        }

        mappedTables = FeatureServerConfiguration.getInstance().includedTables(names);
        // We may only invoke this once, since currently the automapper crashes if a table was already mapped in the
        // past. Since this call is present in the private constructor which is only called in the singleton method
        // there is no problem.
        Document tableMapping = AutoMapper.map(dbConnection, null, schema,mappedTables);
        resultSet.close();
        dbConnection.close();
        newConfig.addXML(tableMapping.asXML());
        
        HSConfiguration configuration = new HSConfiguration();
        this.sessionFactory = newConfig.buildSessionFactory();
        LOGGER.info("Sessionfactory created: " + sessionFactory);
        configuration.configure(newConfig);
    }

    /**
     * Returns a reader for the given table if that table exists, otherwise returns null.
     *
     * @param tableName the table for which a reader is desired.
     * @param bbox a boundingbox constraint for the resulting features. This parameter is ignored
     * if it is invalid, null or if the entities in the given table do not contain a valid
     * geometry property.
     * @return a reader for the given table, or null if no such table exists
     * @throws DatabaseException If for some reason the reader can not be constructed
     */
    public StandardFeatureReader getReader(String tableName, String bbox) throws DatabaseException {
        return getReader(tableName, bbox, null, null, null, null);
    }

    /**
     * @return all tables mapped by this the current featureserver mapping
     */
    public List<String> getAllMappedTables()
    {
        return mappedTables == null ? new ArrayList<String>() : new ArrayList<String>(mappedTables);
    }

    /**
     * Returns a reader for the given table if that table eqxists, otherwise returns null.
     *
     * @param tableName the table for which a reader is desired.
     * @param bbox a boundingbox constraint for the resulting features. This parameter is ignored
     * if it is invalid, null or if the entities in the given table do not contain a valid
     * geometry property.
     * @param CQLString A cql expression that should be executed on the featureserver by the reader.
     * @param start If specified (may be null), this contains the follownumber of the first item to be returned (pagination)
     * @param limit If specified (may be null), this contains the number of items to return.
     * @param orderings a list of orderings on which the results will be sorted. If the list is empty or null, the parameter
     * is ignored.
     * @return a reader for the given table, or null if no such table exists
     * @throws DatabaseException If the a reader can not be constructed (eg: invalid cql query)
     */
    public StandardFeatureReader getReader(String tableName, String bbox, String CQLString, Integer start, Integer limit,
                                           List<Order> orderings)
            throws DatabaseException {
        Class tableClass = AutoMapper.getClass(null, FeatureServerConfiguration.getInstance().getDbaseSchema(), tableName);
        if (tableClass == null) {
            return null;
        }
        return new StandardFeatureReader(sessionFactory, CQLString, tableClass, bbox, start, limit, orderings);
    }

    public <T> List<T> getDistinctValues(Class<?> entityClass, String property, Class<T> propertyType){
        Transaction tx = null;
        try {
            Session session = sessionFactory.getCurrentSession();
            tx = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityClass);
            criteria.add(Restrictions.isNotNull(property));
            criteria.setProjection(Projections.distinct(Projections.property(property)));
            List<T> result = (List<T>) criteria.list();
            tx.commit();
            return result;
        }catch(HibernateException e){
            LOGGER.error(e);
            if (tx != null) {
                tx.rollback();
            }
            throw new DatabaseException(e);
        }finally{            
            sessionFactory.getCurrentSession().close();
        }
    }


    /**
     * Retrieves an instance of the database, or throws a database exception (unchecked exception), if the
     * databasefacade can not be constructed due to an SQL error. This is typically the case when the settings
     * for the connection are not available from the configuration, if the necessary drivers are not present or if
     * there is a problem with the scheme.
     * @return An instance of the databasefacade
     * @throws DatabaseException If anything went wrong that made it impossible to retrieve the facade.
     */
     public static DbaseFacade getInstance()
             throws DatabaseException
     {
        if (instance == null) {
            try{
                instance = new DbaseFacade();
            } catch (SQLException e) {
                LOGGER.error(e);
                throw new DatabaseException(e);
            }
        }
        return instance;
    }
}