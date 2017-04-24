using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using SqlBulkTools.Enumeration;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkInsert<T> : AbstractOperation<T>, ITransaction
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="bulkCopySettings"></param>
        /// <param name="propertyInfoList"></param>
        public BulkInsert(IEnumerable<T> list, string tableName, string schema, HashSet<string> columns,
            Dictionary<string, string> customColumnMappings, BulkCopySettings bulkCopySettings, List<PropertyInfo> propertyInfoList) :

            base(list, tableName, schema, columns, customColumnMappings, bulkCopySettings, propertyInfoList)
        { }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two 
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsert<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            base.SetIdentity(columnName);
            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two 
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        /// <returns></returns>
        public BulkInsert<T> SetIdentityColumn(Expression<Func<T, object>> columnName, ColumnDirectionType outputIdentity)
        {
            base.SetIdentity(columnName, outputIdentity);
            return this;
        }

        /// <summary>
        /// Disables all Non-Clustered indexes on the table before the transaction and rebuilds after the 
        /// transaction. This option should only be considered for very large operations.
        /// </summary>
        /// <returns></returns>
        public BulkInsert<T> TmpDisableAllNonClusteredIndexes()
        {
            _disableAllIndexes = true;
            return this;
        }

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for the operation to be 
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public int Commit(SqlConnection connection)
        {
            try
            {
                int affectedRows = 0;

                if (!_list.Any())
                {
                    return affectedRows;
                }

                DataTable dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn, _outputIdentity);
                dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic);

                // Must be after ToDataTable is called. 
                BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);

                if (connection.State == ConnectionState.Closed)
                    connection.Open();

                BulkOperationsHelper.ValidateMsSqlVersion(connection, OperationType.Insert);

                DataTable dtCols = BulkOperationsHelper.GetDatabaseSchema(connection, _schema, _tableName);

                string destinationTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema, _tableName);
                var schemaDetail = BulkOperationsHelper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);

                SqlCommand command = connection.CreateCommand();
                command.Connection = connection;

                if (_disableAllIndexes)
                {
                    command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(Constants.Disable, _tableName,
                        _schema, connection);
                    command.ExecuteNonQuery();
                }

                // If InputOutput identity is selected, must use staging table.
                if (_outputIdentity == ColumnDirectionType.InputOutput && dtCols != null)
                {
                    command.CommandText = schemaDetail.BuildCreateTableQuery;
                    command.ExecuteNonQuery();

                    if (BulkOperationsHelper.GetBulkInsertStrategyType(dt, _columns) ==
                        BulkInsertStrategyType.MultiValueInsert)
                    {

                        var tempTableSetup = BulkOperationsHelper.BuildInsertQueryFromDataTable(_customColumnMappings, dt, _identityColumn,
                            _columns, _bulkCopySettings, schemaDetail, Constants.TempTableName, keepIdentity: true, keepInternalId: true);
                        command.CommandText = tempTableSetup.InsertQuery;
                        command.Parameters.AddRange(tempTableSetup.SqlParameterList.ToArray());
                        command.ExecuteNonQuery();
                        command.Parameters.Clear();
                    }
                    else
                        BulkOperationsHelper.InsertToTmpTableWithBulkCopy(connection, dt, _bulkCopySettings);

                    command.CommandText = BulkOperationsHelper.GetInsertIntoStagingTableCmd(command, connection, _schema,
                        _tableName,
                        _columns, _identityColumn, _outputIdentity);
                    command.ExecuteNonQuery();

                    BulkOperationsHelper.LoadFromTmpOutputTable(command, _identityColumn, _outputIdentityDic,
                        OperationType.Insert, _list);

                }

                else if (BulkOperationsHelper.GetBulkInsertStrategyType(dt, _columns) ==
                         BulkInsertStrategyType.MultiValueInsert)
                {
                    var tableSetup = BulkOperationsHelper.BuildInsertQueryFromDataTable(_customColumnMappings, dt, _identityColumn,
                    _columns, _bulkCopySettings, schemaDetail, destinationTableName);
                    command.CommandText = GetSetIdentityCmd(on: true);
                    command.CommandText += tableSetup.InsertQuery;
                    command.CommandText += " " + GetSetIdentityCmd(on: false);
                    command.Parameters.AddRange(tableSetup.SqlParameterList.ToArray());
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }

                else
                {
                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection, _bulkCopySettings.SqlBulkCopyOptions, null))
                    {
                        bulkcopy.DestinationTableName = destinationTableName;
                        BulkOperationsHelper.MapColumns(bulkcopy, _columns, _customColumnMappings);

                        BulkOperationsHelper.SetSqlBulkCopySettings(bulkcopy, _bulkCopySettings);
                        bulkcopy.WriteToServer(dt);

                        bulkcopy.Close();
                    }
                }

                if (_disableAllIndexes)
                {
                    command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(Constants.Rebuild, _tableName,
                        _schema, connection);
                    command.ExecuteNonQuery();
                }



                affectedRows = dt.Rows.Count;
                return affectedRows;
            }

            catch (SqlException e)
            {
                for (int i = 0; i < e.Errors.Count; i++)
                {
                    // Error 8102 is identity error. 
                    if (e.Errors[i].Number == 8102 || e.Errors[i].Number == 544)
                    {
                        // Expensive but neccessary to inform user of an important configuration setup. 
                        throw new IdentityException(e.Errors[i].Message);
                    }
                }

                throw;
            }
        }

        /// <summary>
        /// Commits a transaction to database asynchronously. A valid setup must exist for the operation to be 
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public async Task<int> CommitAsync(SqlConnection connection)
        {
            try
            {
                int affectedRows = 0;

                if (!_list.Any())
                {
                    return affectedRows;
                }

                DataTable dt = BulkOperationsHelper.CreateDataTable<T>(_propertyInfoList, _columns, _customColumnMappings, _ordinalDic, _matchTargetOn, _outputIdentity);
                dt = BulkOperationsHelper.ConvertListToDataTable(_propertyInfoList, dt, _list, _columns, _ordinalDic);

                // Must be after ToDataTable is called. 
                BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);

                if (connection.State == ConnectionState.Closed)
                    await connection.OpenAsync();

                BulkOperationsHelper.ValidateMsSqlVersion(connection, OperationType.Insert);

                DataTable dtCols = BulkOperationsHelper.GetDatabaseSchema(connection, _schema, _tableName);

                string destinationTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema, _tableName);
                var schemaDetail = BulkOperationsHelper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);

                SqlCommand command = connection.CreateCommand();
                command.Connection = connection;

                if (_disableAllIndexes)
                {
                    command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(Constants.Disable, _tableName,
                        _schema, connection);
                    await command.ExecuteNonQueryAsync();
                }

                // If InputOutput identity is selected, must use staging table.
                if (_outputIdentity == ColumnDirectionType.InputOutput && dtCols != null)
                {
                    command.CommandText = schemaDetail.BuildCreateTableQuery;
                    await command.ExecuteNonQueryAsync();

                    if (BulkOperationsHelper.GetBulkInsertStrategyType(dt, _columns) ==
                        BulkInsertStrategyType.MultiValueInsert)
                    {

                        var tempTableSetup = BulkOperationsHelper.BuildInsertQueryFromDataTable(_customColumnMappings, dt, _identityColumn,
                            _columns, _bulkCopySettings, schemaDetail, Constants.TempTableName, keepIdentity: true, keepInternalId: true);
                        command.CommandText = tempTableSetup.InsertQuery;
                        command.Parameters.AddRange(tempTableSetup.SqlParameterList.ToArray());
                        await command.ExecuteNonQueryAsync();
                        command.Parameters.Clear();
                    }
                    else
                        await BulkOperationsHelper.InsertToTmpTableWithBulkCopyAsync(connection, dt, _bulkCopySettings);

                    command.CommandText = BulkOperationsHelper.GetInsertIntoStagingTableCmd(command, connection, _schema,
                        _tableName,
                        _columns, _identityColumn, _outputIdentity);
                    await command.ExecuteNonQueryAsync();

                    await BulkOperationsHelper.LoadFromTmpOutputTableAsync(command, _identityColumn, _outputIdentityDic,
                        OperationType.Insert, _list);

                }

                else if (BulkOperationsHelper.GetBulkInsertStrategyType(dt, _columns) ==
                         BulkInsertStrategyType.MultiValueInsert)
                {
                    var tableSetup = BulkOperationsHelper.BuildInsertQueryFromDataTable(_customColumnMappings, dt, _identityColumn,
                        _columns, _bulkCopySettings, schemaDetail, destinationTableName);
                    command.CommandText = GetSetIdentityCmd(on: true);
                    command.CommandText += tableSetup.InsertQuery;
                    command.CommandText += " " + GetSetIdentityCmd(on: false);
                    command.Parameters.AddRange(tableSetup.SqlParameterList.ToArray());
                    await command.ExecuteNonQueryAsync();
                    command.Parameters.Clear();
                }

                else
                {
                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection, _bulkCopySettings.SqlBulkCopyOptions, null))
                    {
                        bulkcopy.DestinationTableName = destinationTableName;
                        BulkOperationsHelper.MapColumns(bulkcopy, _columns, _customColumnMappings);

                        BulkOperationsHelper.SetSqlBulkCopySettings(bulkcopy, _bulkCopySettings);
                        await bulkcopy.WriteToServerAsync(dt);

                        bulkcopy.Close();
                    }

                }

                if (_disableAllIndexes)
                {
                    command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(Constants.Rebuild, _tableName,
                        _schema, connection);
                    await command.ExecuteNonQueryAsync();
                }

                affectedRows = dt.Rows.Count;
                return affectedRows;
            }

            catch (SqlException e)
            {
                for (int i = 0; i < e.Errors.Count; i++)
                {
                    // Error 8102 is identity error. 
                    if (e.Errors[i].Number == 8102 || e.Errors[i].Number == 544)
                    {
                        // Expensive but neccessary to inform user of an important configuration setup. 
                        throw new IdentityException(e.Errors[i].Message);
                    }
                }

                throw;
            }
        }
    }
}
