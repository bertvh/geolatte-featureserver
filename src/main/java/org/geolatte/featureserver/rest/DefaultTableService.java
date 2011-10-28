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

package org.geolatte.featureserver.rest;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.geolatte.common.Feature;
import org.geolatte.common.dataformats.csv.CsvSerializationTransformation;
import org.geolatte.common.dataformats.json.jackson.JsonSerializationTransformation;
import org.geolatte.common.dataformats.json.jackson.SimpleDateFormatSerializer;
import org.geolatte.common.reflection.EntityClassReader;
import org.geolatte.common.reflection.ObjectToFeatureTransformation;
import org.geolatte.common.transformer.*;
import org.geolatte.featureserver.config.ConfigurationException;
import org.geolatte.featureserver.config.FeatureServerConfiguration;
import org.geolatte.featureserver.dbase.DatabaseException;
import org.geolatte.featureserver.dbase.DbaseFacade;
import org.geolatte.featureserver.dbase.StandardFeatureReader;
import org.hibernate.criterion.Order;
import org.hibernatespatial.pojo.AutoMapper;

import javax.ws.rs.core.Response;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Base implementation of a simple tableserver. This class implements the TableService interface.
 * <p>
 * <i>Creation-Date</i>: 9-apr-2010<br>
 * <i>Creation-Time</i>:  11:48:54<br>
 * </p>
 *
 * @author Yves Vandewoude
 * @author Bert Vanhooff
 * @author <a href="http://www.qmino.com">Qmino bvba</a>
 * @since SDK1.5
 */
// It is used by reflection.
public class DefaultTableService implements TableService {

    private final JsonSerializationTransformation jts = new JsonSerializationTransformation();
    private static final Logger LOGGER = LogManager.getLogger(DefaultTableService.class);
    private enum OutputFormat {
        JSON,
        CSV
    }

    static {
        // Initialize the facade, if not you might run into problems if you try to get a reader from the AutoMapper
        // before you do.
        DbaseFacade.getInstance();
    }

    public DefaultTableService() {
        jts.addClassSerializer(Date.class, new SimpleDateFormatSerializer());
    }

    public String getAllTables() {
        try {
            List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
            for (String table : DbaseFacade.getInstance().getAllMappedTables()) {
                Map<String, Object> current = new HashMap<String, Object>();
                current.put("name", table);
                EntityClassReader reader;
                reader = EntityClassReader.getClassReaderFor(
                        AutoMapper.getClass(null, FeatureServerConfiguration.getInstance().getDbaseSchema(), table));
                List<Map<String, String>> properties = new ArrayList<Map<String, String>>();
                current.put("properties", properties);
                if (reader.getIdName() != null) {
                    Map<String, String> propertyMap = new HashMap<String, String>();
                    propertyMap.put("name", reader.getIdName());
                    propertyMap.put("type", reader.getPropertyType(reader.getIdName()).getSimpleName());
                    properties.add(propertyMap);
                }
                if (reader.getGeometryName() != null) {
                    Map<String, String> propertyMap = new HashMap<String, String>();
                    propertyMap.put("name", reader.getGeometryName());
                    propertyMap.put("type", reader.getPropertyType(reader.getGeometryName()).getSimpleName());
                    properties.add(propertyMap);
                }
                for (String property : reader.getProperties()) {
                    String propertyType = reader.getPropertyType(property).getSimpleName();
                    Map<String, String> propertyMap = new HashMap<String, String>();
                    propertyMap.put("name", property);
                    propertyMap.put("type", propertyType);
                    properties.add(propertyMap);
                }
                tables.add(current);
            }
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("items", tables);
            result.put("total", tables.size());
            try {
                return jts.transform(result);
            } catch (TransformationException e) {
                //we should throw an exception here
                return null;
            }
        } catch (ConfigurationException e) {
            return "{\"error\": \"Invalid Featureserver configuration: " + e.getMessage() + "\"}";
        } catch (DatabaseException e) {
            return "{\"error\": \"Database access problem: " + e.getMessage() + "\"}";
        }

    }

    public Response getTableCSV(String tableName,
                                String bbox,
                                String cql,
                                Integer start,
                                Integer limit,
                                String sortColumns,
                                String sortDirections,
                                String visibleColumns,
                                String separator,
                                String asdownload) {

        return getTable(OutputFormat.CSV,
                        tableName,
                        bbox,
                        cql,
                        start, limit,
                        sortColumns, sortDirections, visibleColumns,
                        separator,
                        asdownload);
    }

    public Response getTableJSON(String tableName,
                                 String bbox,
                                 String cql,
                                 Integer start,
                                 Integer limit,
                                 String sortColumns,
                                 String sortDirections,
                                 String visibleColumns,
                                 String asdownload) {

        return getTable(OutputFormat.JSON,
                        tableName,
                        bbox,
                        cql,
                        start, limit,
                        sortColumns, sortDirections, visibleColumns,
                        null,
                        asdownload);
    }

    /**
     * Gets the requested table in the requested format, docs see
     * {@link #getTableCSV(String, String, String, Integer, Integer, String, String, String, String, String)} and
     * {@link #getTableJSON(String, String, String, Integer, Integer, String, String, String, String)}.
     */
    private Response getTable(OutputFormat format,
                              String tableName,
                              String bbox,
                              String cql,
                              Integer start,
                              Integer limit,
                              String sortColumns,
                              String sortDirections,
                              String visibleColumns,
                              String separator,
                              String asdownload) {
        StandardFeatureReader featureReader = null;
        try {
            List<Order> orderings = getOrderings(tableName, sortColumns, sortDirections);
            featureReader = DbaseFacade.getInstance().getReader(tableName, bbox, cql, start, limit, orderings);
            if (featureReader == null) {
                Response.ResponseBuilder builder =
                    Response.status(Response.Status.NOT_FOUND)
                            .entity(tableNotExistsMessage(tableName));
                return builder.build();
            }
            List<String> visible = new ArrayList<String>();
            if (visibleColumns != null) {
                visible.add(visibleColumns);
            }
            List<List<String>> columnNamesToShow = getColumnNames(tableName, visible);
            String contentDisposition = buildContentDisposition(tableName, asdownload, format);
            String msg;
            switch (format) {
                case CSV:
                    msg = getTablesInCsv(featureReader, columnNamesToShow.size() > 0 ? columnNamesToShow.get(0) : null, separator);
                    break;
                default:
                    msg = getTablesInJson(featureReader);
            }
            return toResponse(msg, contentDisposition);

        } catch (ConfigurationException e) {
            LOGGER.warn("Invalid Featureserver configuration: " + e.getMessage());
            Response.ResponseBuilder builder =
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity("{\"error\": \"Invalid Featureserver configuration: " + e.getMessage() + "\"}");
            return builder.build();
        } catch (DatabaseException e) {
            LOGGER.warn("Database access problem: " + e.getMessage());
            Response.ResponseBuilder builder =
                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity("{\"error\": \"Database access problem: " + e.getMessage() + "\"}");
            return builder.build();
        } finally {
            if (featureReader != null) {
                featureReader.close();
            }
        }
    }

    private String buildContentDisposition(String tableName, String asdownload, OutputFormat output) {
        if (asdownload != null && asdownload.equalsIgnoreCase("true")){
            String extension = OutputFormat.JSON.equals(output) ? "js" : "csv";
            return String.format("attachment; filename=%s.%s", tableName , extension);
        }
        return null;
    }

    private Response toResponse(String msg, String disposition) {
        Response.ResponseBuilder builder= Response.ok(msg);
        if (disposition != null && !disposition.isEmpty()){
            builder.header("Content-disposition", disposition);
        }
        return builder.build();
    }

    public String getTablesInCsv(StandardFeatureReader dataSource, List<String> visible, String separator) {
        if (separator == null || separator.isEmpty())
            separator = DEFAULT_SEPARATOR;
        char seperatorChar = separator.charAt(0);
        CsvSerializationTransformation<Object> csv =
                new CsvSerializationTransformation<Object>(dataSource.getEntityClass(), seperatorChar, visible);
        OpenTransformerChain<String> myChain = TransformerChainFactory.<Object, String>newChain().add(dataSource)
                .last(new DefaultTransformer<Object, String>(csv));

        StringBuilder sb = new StringBuilder();
        sb.append(csv.getHeader());
        sb.append("\n");
        for (String item : myChain) {
            sb.append(item);
            sb.append("\n");
        }
        return sb.toString();
    }

    public String getTablesInJson(StandardFeatureReader featureReader) {
        Transformer<Object, Feature> featureCreator = new DefaultTransformer<Object, Feature>(new ObjectToFeatureTransformation());
        OpenTransformerChain<Feature> myChain = TransformerChainFactory.<Object, Feature>newChain().add(featureReader).last(featureCreator);
        List<Feature> allFeatures = new ArrayList<Feature>();
        for (Feature f : myChain) {
            allFeatures.add(f);
        }
        Map<String, Object> outputMap = new HashMap<String, Object>();
        outputMap.put("total", featureReader.getTotalCount());
        outputMap.put("items", allFeatures);
        try {
            return jts.transform(outputMap);
        } catch (TransformationException e) {
            return null;
        }
    }


    /**
     * Helpermethod that expects the name of a table a list of columnnames and corresponding sortDirections. The
     * method will return a list of Hibernate-Order objects that can be used to sort the outcome of the query.
     * Note:
     * <ul>
     * <li>Strings in sortFields which are not columnnames in the given table are ignored
     * <li>If the number of strings in sortFields <> number of strings in sortDirections, an empty list of order
     * objects is returned
     * <li>Sorting is not permitted on geometry fields. such sortings are ignored.
     * </ul>
     *
     * @param tableName      the name of the table for which the sortobjects are desired. If the tableName is invalid
     *                       null is returned.
     * @param sortFields     a '|' seperated list of strings each denoting a columnname in tableName.
     * @param sortDirections a '|' seperated list of strings each denoting either 'asc' or 'desc'. If set to null,
     *                       all sortFields are considered asc. If specified, the number of elements must be equal to sortFields.
     * @return a list of hibernate order objects
     */
    private List<Order> getOrderings(String tableName,
                                     String sortFields,
                                     String sortDirections) {
        if (sortFields != null) {
            List<String> fieldInfo = new ArrayList<String>();
            fieldInfo.add(sortFields);
            if (sortDirections != null) {
                fieldInfo.add(sortDirections);
            }
            try {
                List<List<String>> columnInfo = getColumnNames(tableName, fieldInfo);
                List<Order> result = new ArrayList<Order>();
                for (int i = 0; i < columnInfo.get(0).size(); i++) {
                    boolean asc = sortDirections == null || "asc".equalsIgnoreCase(columnInfo.get(1).get(i));
                    result.add(asc ? Order.asc(columnInfo.get(0).get(i)) : Order.desc(columnInfo.get(0).get(i)));
                }
                return result;
            } catch (IllegalArgumentException e) {
                return new ArrayList<Order>();
            }
        }
        return null;
    }


    /**
     * <p>Expects a list of ';' separated list of strings, containing at least one element. The first element in the list
     * is a ';' separated list of columnNames. The subsequent elements of the given list are also ';' separated lists of
     * data which may contain any string.</p>
     * <p/>
     * <p>This method will split each string in the commaseparated list and return the result of that split in a list of lists.
     * In addition, elements from the first string that do not correspond to a column in the given table are removed, as well
     * as coindexed elements in the other strings.</p>
     * <p/>
     * <p>The method therefore requires that each of the given strings in commaSeparatedList has the same length after being
     * split on ';'.</p>
     * <p/>
     * Suppose this is your input:
     * <blockquote>
     * "firstColumn;secondColumn;unexistingColumn;anotherColumn", "84;12;36;7", "abc;cde;efg;ghi"
     * </blockquote>
     * You will receive as output, assuming that firstColumn, secondColumn and thirdColumn are valid columnnames for the
     * given table and unexistingColumn is not:
     * <blockquote>
     * ["firstColumn", "secondColumn", anotherColumn"], ["84", "12", "7"], ["abc", "cde", "ghi"]
     * </blockquote>
     *
     * @param tableName          the name of a table currently served by featureserver. If the tableName
     *                           does not correspond with an existing table served by featureserver, an empty list is returned
     *                           since clearly none of the strings correspond with a columnname in an unexisting table.
     * @param commaSeparatedList a list of ';' separated list of columnnames. If null or empty, an empty list will
     *                           be returned. If the split version of the items in the list are of different sizes, an exception is thrown since
     *                           co-indexing is needed.
     * @return a list containing splits of all strings on ';', removing unexisting columns and coindexed.
     */
    private List<List<String>> getColumnNames(String tableName, List<String> commaSeparatedList) {
        if (tableName == null || commaSeparatedList == null || tableName.length() == 0 || commaSeparatedList.size() == 0) {
            return new ArrayList<List<String>>();
        } else {
            String[] columnNames = commaSeparatedList.get(0).split(";");
            List<String> coIndexed = commaSeparatedList.size() > 1 ? commaSeparatedList.subList(1, commaSeparatedList.size())
                    : new ArrayList<String>();
            List<String[]> coIndexedSplit = new ArrayList<String[]>();
            for (String dataItem : coIndexed) {
                String[] split = dataItem.split(";");
                if (split.length != columnNames.length) {
                    throw new IllegalArgumentException("Not all inputstrings have same splitsize");
                }
                coIndexedSplit.add(split);
            }
            List<List<String>> result = new ArrayList<List<String>>();
            for (int i = 0; i < coIndexedSplit.size() + 1; i++) {
                result.add(new ArrayList<String>());
            }
            EntityClassReader reader = EntityClassReader.getClassReaderFor(
                    AutoMapper.getClass(null, FeatureServerConfiguration.getInstance().getDbaseSchema(), tableName));
            for (int i = 0; i < columnNames.length; i++) {
                if (reader.exists(columnNames[i], true)) {
                    result.get(0).add(columnNames[i]);
                    for (int j = 0; j < coIndexedSplit.size(); j++) {
                        result.get(j + 1).add(coIndexedSplit.get(j)[i]);
                    }
                }
            }
            return result;
        }
    }

    public Response getPropertyValuesCSV(String tableName,
                                         String propertyName,
                                         String separator) {
        return getPropertyValues(OutputFormat.CSV, tableName, propertyName, separator);
    }

    public Response getPropertyValuesJSON(String tableName, String propertyName) {
        return getPropertyValues(OutputFormat.JSON, tableName, propertyName, null);
    }

    private Response getPropertyValues(OutputFormat format,
                                       String tableName,
                                       String propertyName,
                                       String separator) {

        final String schema = FeatureServerConfiguration.getInstance().getDbaseSchema();
        Class<?> entityClass = AutoMapper.getClass(null, schema, tableName);
        if (entityClass == null) {
            Response.ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
            builder.entity(tableNotExistsMessage(tableName));
            return builder.build();
        }

        Class<?> propertyType;
        try {
            propertyType = getPropertyType(entityClass, propertyName);
        } catch (NoSuchFieldException e) {
            Response.ResponseBuilder builder= Response.status(Response.Status.NOT_FOUND);
            builder.entity(propertyNotExistsMessage(tableName, propertyName));
            return builder.build();
        }
        if (!canDoDistinct(propertyType)) {
            Response.ResponseBuilder builder= Response.status(Response.Status.PRECONDITION_FAILED);
            builder.entity(propertyDistinctNotSupportedMessage(tableName, propertyName));
            return builder.build();
        }
        List<?> values = DbaseFacade.getInstance().getDistinctValues(entityClass, propertyName, propertyType);
        Response.ResponseBuilder builder= Response.ok(toFormat(values, format, tableName, propertyName, separator));
        return builder.build();

    }

    private String toFormat(List<?> values, OutputFormat outputFormat, String tableName, String propertyName, String separator) {
        if (OutputFormat.JSON.equals(outputFormat)) {
            return toJSONOutput(values,tableName, propertyName);
        } else {
            return toCSVOutput(values, separator);
        }
    }

    private String toJSONOutput(List<?> values, String tableName, String propertyName) {
        Map<String, Object> map = new HashMap<String, Object>(3);
        map.put("table", tableName);
        map.put("property", propertyName);
        map.put("distinct-values", values);
        try {
            return jts.transform(map);
        } catch (TransformationException e) {
            LOGGER.error(e);
            return null;
        }
    }

    private String toCSVOutput(List<?> values, String separator) {
        if (separator == null || separator.isEmpty())
            separator = DEFAULT_SEPARATOR;
        char separatorChar = separator.charAt(0);
        StringBuilder stb = new StringBuilder();
        for (Object o : values){
            stb.append(o).append(separatorChar);
        }
        stb.deleteCharAt(stb.length() -1);
        return stb.toString();
    }

    private boolean canDoDistinct(Class<?> propertyType) {
        return String.class.isAssignableFrom(propertyType) ||
                Integer.class.isAssignableFrom(propertyType) ||
                Byte.class.isAssignableFrom(propertyType) ||
                Boolean.class.isAssignableFrom(propertyType);

    }

    private Class<?> getPropertyType(Class<?> entityClass, String propertyName) throws NoSuchFieldException {
        Field field = entityClass.getDeclaredField(propertyName);
        return field.getType();
    }

    private String tableNotExistsMessage(String tableName) {
        return "{\"error\": \"Table " + tableName + " does not exist\"}";
    }


    private String propertyNotExistsMessage(String tableName, String propertyName) {
        return "{\"error\": \"Table " + tableName + " does not have property " + propertyName + "\"}";
    }

    private String propertyDistinctNotSupportedMessage(String tableName, String propertyName) {
        return "{\"error\": \"Property " + propertyName + " of Table " + tableName + " is not of type Integer or String\"}";
    }

}
