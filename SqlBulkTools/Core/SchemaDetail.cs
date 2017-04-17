using System.Collections.Generic;

namespace SqlBulkTools.Core
{
    /// <summary>
    /// 
    /// </summary>
    internal class SchemaDetail
    {
        internal Dictionary<string, PrecisionType> NumericPrecisionTypeDic { get; set; }
        internal Dictionary<string, string> DateTimeTypePrecisionDic { get; set; }
        internal Dictionary<string, string> MaxCharDic { get; set; }
        internal Dictionary<string, bool> NullableDic { get; set; }
        internal string BuildCreateTableQuery { get; set; }
    }
}
