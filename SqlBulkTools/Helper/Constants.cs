 // ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    internal static class Constants
    {
        internal const string DefaultSchemaName = "dbo";
        internal const string InternalId = "SqlBulkTools_InternalId";
        internal const string TempTableName = "#TmpTable";
        internal const string TempOutputTableName = "#TmpOutput";
        internal const string SourceAlias = "Source";
        internal const string TargetAlias = "Target";
        internal const string UniqueParamIdentifier = "Condition";
        internal const string Rebuild = "REBUILD";
        internal const string Disable = "DISABLE";
        internal const int BulkCopyRecordThreshold = 20;
        internal const int MaxSqlParameters = 2099;
    }
}
