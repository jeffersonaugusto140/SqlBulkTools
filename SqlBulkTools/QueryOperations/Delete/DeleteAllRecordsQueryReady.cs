using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DeleteAllRecordsQueryReady<T> : ITransaction
    {
        private readonly string _tableName;
        private readonly string _schema;
        private int? _batchQuantity;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        public DeleteAllRecordsQueryReady(string tableName, string schema)
        {
            _tableName = tableName;
            _schema = schema;
            _batchQuantity = null;
        }

        /// <summary>
        /// The maximum number of records to delete per transaction.
        /// </summary>
        /// <param name="batchQuantity"></param>
        /// <returns></returns>
        public DeleteAllRecordsQueryReady<T> SetBatchQuantity(int batchQuantity)
        {
            _batchQuantity = batchQuantity;
            return this;
        }

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for the operation to be 
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        public int Commit(SqlConnection connection, int commandTimeout = 30)
        {
            if (connection.State == ConnectionState.Closed)
                connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandTimeout = commandTimeout;

            command.CommandText = GetQuery(connection);

            int affectedRows = command.ExecuteNonQuery();

            return affectedRows;
        }

        /// <summary>
        /// Commits a transaction to database asynchronously. A valid setup must exist for the operation to be 
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        public async Task<int> CommitAsync(SqlConnection connection, int commandTimeout = 30)
        {

            if (connection.State == ConnectionState.Closed)
                await connection.OpenAsync();

            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandTimeout = commandTimeout;

            command.CommandText = command.CommandText = GetQuery(connection);

            int affectedRows = await command.ExecuteNonQueryAsync();

            return affectedRows;
        }

        private string GetQuery(SqlConnection connection)
        {
            string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema,
                 _tableName);

            string batchQtyStart = _batchQuantity != null ? "DeleteMore:\n" : string.Empty;
            string batchQty = _batchQuantity != null ? $"TOP ({_batchQuantity}) " : string.Empty;
            string batchQtyRepeat = _batchQuantity != null ? $"\nIF @@ROWCOUNT != 0\ngoto DeleteMore" : string.Empty;

            string comm = $"{batchQtyStart}DELETE {batchQty}FROM {fullQualifiedTableName} {batchQtyRepeat}";

            return comm;
        }
    }
}
