using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Linq;

namespace SqlBulkTools.Core
{
    /// <summary>
    /// 
    /// </summary>
    public static class BulkOperationsUtility
    {

        private static readonly Dictionary<Type, DbType> DbTypeMappings = new Dictionary<Type, DbType>()
        {
            { typeof(byte), DbType.Byte},
            { typeof(sbyte), DbType.Int16},
            { typeof(ushort), DbType.UInt16},
            { typeof(int), DbType.Int32},
            { typeof(uint), DbType.UInt32},
            { typeof(long), DbType.Int64},
            { typeof(ulong), DbType.UInt64 },
            { typeof(float), DbType.Single },
            { typeof(double), DbType.Double},
            { typeof(decimal), DbType.Decimal},
            { typeof(bool), DbType.Boolean},
            { typeof(string), DbType.String },
            { typeof(char), DbType.StringFixedLength},
            { typeof(char[]), DbType.String},
            { typeof(Guid), DbType.Guid},
            { typeof(DateTime), DbType.DateTime},
            { typeof(DateTimeOffset), DbType.DateTimeOffset },
            { typeof(byte[]), DbType.Binary},
            { typeof(byte?), DbType.Byte},
            { typeof(sbyte?), DbType.SByte },
            { typeof(short), DbType.Int16},
            { typeof(short?), DbType.Int16},
            { typeof(ushort?), DbType.UInt16},
            { typeof(int?), DbType.Int32},
            { typeof(uint?), DbType.UInt32},
            { typeof(long?), DbType.Int64},
            { typeof(ulong?), DbType.UInt64},
            { typeof(float?), DbType.Single},
            { typeof(double?), DbType.Double},
            { typeof(decimal?), DbType.Decimal},
            { typeof(bool?), DbType.Boolean},
            { typeof(char?), DbType.StringFixedLength},
            { typeof(Guid?), DbType.Guid},
            { typeof(DateTime?), DbType.DateTime },
            { typeof(DateTimeOffset?), DbType.DateTimeOffset},
            { typeof(Binary), DbType.Binary},
            { typeof(TimeSpan), DbType.Time },
            { typeof(TimeSpan?), DbType.Time },
        };

        private static readonly Dictionary<Type, SqlDbType> SqlDbMappings = new Dictionary<Type, SqlDbType>()
        {

                {typeof (Boolean), SqlDbType.Bit},
                {typeof (Boolean?), SqlDbType.Bit},
                {typeof (Byte), SqlDbType.TinyInt},
                {typeof (Byte?), SqlDbType.TinyInt},
                {typeof (String), SqlDbType.NVarChar},
                {typeof (DateTime), SqlDbType.DateTime},
                {typeof (DateTime?), SqlDbType.DateTime},
                {typeof (Int16), SqlDbType.SmallInt},
                {typeof (Int16?), SqlDbType.SmallInt},
                {typeof (Int32), SqlDbType.Int},
                {typeof (Int32?), SqlDbType.Int},
                {typeof (Int64), SqlDbType.BigInt},
                {typeof (Int64?), SqlDbType.BigInt},
                {typeof (Decimal), SqlDbType.Decimal},
                {typeof (Decimal?), SqlDbType.Decimal},
                {typeof (Double), SqlDbType.Float},
                {typeof (Double?), SqlDbType.Float},
                {typeof (Single), SqlDbType.Real},
                {typeof (Single?), SqlDbType.Real},
                {typeof (TimeSpan), SqlDbType.Time},
                {typeof (Guid), SqlDbType.UniqueIdentifier},
                {typeof (Guid?), SqlDbType.UniqueIdentifier},
                {typeof (Byte[]), SqlDbType.Binary},
                {typeof (Byte?[]), SqlDbType.Binary},
                {typeof (Char[]), SqlDbType.Char},
                {typeof (Char?[]), SqlDbType.Char}
        };

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <exception cref="KeyNotFoundException"></exception>
        public static DbType GetSqlTypeFromDotNetType(Type type)
        {
            DbType dbType;

            if (DbTypeMappings.TryGetValue(type, out dbType))
            {
                return dbType;
            }

            throw new KeyNotFoundException($"The type {type} could not be found.");
        }
    }
}
