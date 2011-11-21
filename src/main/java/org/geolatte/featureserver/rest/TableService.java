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

package org.geolatte.featureserver.rest;

import org.jboss.resteasy.annotations.GZIP;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * The tableservice interface is a generic rest interface that allows the user to specify the name of the
 * table and retrieve all the records for this table
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
@Path("/rest")
public interface TableService {

    public final static String DEFAULT_SEPARATOR = "|";
    public final static MediaType MEDIA_TYPE_TEXT_CSV_TYPE = new MediaType("text", "csv");
    public final static String    MEDIA_TYPE_TEXT_CSV      = "text/csv";

    /**
     * Gets the names and properties of all tables served in JSON format.
     *
     * @return the names and properties of all tables served.
     */
    @GET
    @Path("/tables")
    public String getAllTables();

    /**
     * Gets the table content in CSV format.
     *
     * @param tableName the name of the table to retrieve
     * @param bbox a boundingbox filter for the elements in the table. May be null if not required
     * @param cql a cql expression to execute on the elements to retrieve, this is executed in addition to the bounding
     * -box filter.
     * @param start for pagination, the number of the item
     * @param limit the maximum number of elements to return
     *
     * @param sortColumns the names of the fields on which the results should be sorted. May be null or empty, in which case it is
     * ignored. Entries in the list that do not correspond to an existing columnname are ignored.
     * The entries in the sortFields list are seperated by ';'. Unless overriden (see sortDirections), all sortings are
     * considered to be asc.
     * @param sortDirections the direction (asc/desc) of the sorting. This parameter may be left out, in which case
     * all sortings (if any) are considered asc. If specified, however, the list must have an equal amount of elements
     * as sortFields, each of which must either be 'asc' or 'desc'. The elements are coindexed with those of sortFields
     * @param visibleColumns a ';' separated list of the columns that must be shown. If set to null, all columns will be shown/served.
     * If specified, the list is considered to be exhaustive: all columns you wish to see must be specified. In addition, the
     * list of columns also implies an ordering. Columns in this list that do not exist are simply ignored.
     * @param separator the separator character to use wit h CSV-output. Is ignored in case output format is not CSV.
     * In case a string of length > 1 is specified, only the first character is used.
     * @param asdownload if this parameter equals "true", then the content-disposition of the response will be set as an
     *                   attachment.
     * @return the contents of the requested table
     */
    @GET
    @Produces(MEDIA_TYPE_TEXT_CSV)
    @GZIP
    @Path("/tables/{name}")
    public Response getTableCSV(@PathParam("name") String tableName,
                                @QueryParam("bbox") String bbox,
                                @QueryParam("cql") String cql,
                                @QueryParam("start") Integer start,
                                @QueryParam("limit") Integer limit,
                                @QueryParam("sortColumns") String sortColumns,
                                @QueryParam("sortDirections") String sortDirections,
                                @QueryParam("visibleColumns") String visibleColumns,
                                @DefaultValue(DEFAULT_SEPARATOR) @QueryParam("separator") String separator,
                                @QueryParam("asdownload") String asdownload);

    /**
     * Gets the table content in JSON format.
     *
     * @param tableName the name of the table to retrieve
     * @param bbox a boundingbox filter for the elements in the table. May be null if not required
     * @param cql a cql expression to execute on the elements to retrieve, this is executed in addition to the bounding
     * -box filter.
     * @param start for pagination, the number of the item
     * @param limit the maximum number of elements to return
     * @return the contents of the requested table
     * @param sortColumns the names of the fields on which the results should be sorted. May be null or empty, in which case it is
     * ignored. Entries in the list that do not correspond to an existing columnname are ignored.
     * The entries in the sortFields list are seperated by ';'. Unless overriden (see sortDirections), all sortings are
     * considered to be asc.
     * @param sortDirections the direction (asc/desc) of the sorting. This parameter may be left out, in which case
     * all sortings (if any) are considered asc. If specified, however, the list must have an equal amount of elements
     * as sortFields, each of which must either be 'asc' or 'desc'. The elements are coindexed with those of sortFields
     * @param visibleColumns a ';' separated list of the columns that must be shown. If set to null, all columns will be shown/served.
     * If specified, the list is considered to be exhaustive: all columns you wish to see must be specified. In addition, the
     * list of columns also implies an ordering. Columns in this list that do not exist are simply ignored.
     * In case a string of length > 1 is specified, only the first character is used.
     * @param asdownload if this parameter equals "true", then the content-disposition of the response will be sett as an attachment.
     */
    @GET
    @GZIP
    @Path("/tables/{name}")
    public Response getTableJSON(@PathParam("name") String tableName,
                                 @QueryParam("bbox") String bbox,
                                 @QueryParam("cql") String cql,
                                 @QueryParam("start") Integer start,
                                 @QueryParam("limit") Integer limit,
                                 @QueryParam("sortColumns") String sortColumns,
                                 @QueryParam("sortDirections") String sortDirections,
                                 @QueryParam("visibleColumns") String visibleColumns,
                                 @QueryParam("asdownload") String asdownload);


    /**
     * Gets the values of the given property in the given table in CSV format. Returns distinct values. Is only
     * applicable on integer or string valued properties.
     *
     * @param tableName    the name of the table to retrieve
     * @param propertyName the name of the property to retrieve values from
     * @param separator    the separator character to use with CSV-output. Is ignored in case output format is not CSV.
     *                     In case a string of length > 1 is specified, only the first character is used.
     *
     * @return the contents of the requested table
     */
    @GET
    @Produces(MEDIA_TYPE_TEXT_CSV)
    @Path("/tables/{name}/{property}")
    public Response getPropertyValuesCSV(@PathParam("name") String tableName,
                                         @PathParam("property") String propertyName,
                                         @DefaultValue(DEFAULT_SEPARATOR) @QueryParam("separator") String separator);

    /**
     * Gets the values of the given property in the given table in JSON format. Returns distinct values. Is only
     * applicable on integer or string valued properties.
     *
     * @param tableName the name of the table to retrieve
     * @param propertyName the name of the property to retrieve values from
     * In case a string of length > 1 is specified, only the first character is used.
     * 
     * @return the contents of the requested table
     */
    @GET
    @Path("/tables/{name}/{property}")
    public Response getPropertyValuesJSON(@PathParam("name") String tableName,
                                          @PathParam("property") String propertyName);

}
