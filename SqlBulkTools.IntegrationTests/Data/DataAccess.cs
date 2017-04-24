using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using SprocMapperLibrary.SqlServer;
using SqlBulkTools.TestCommon.Model;

namespace SqlBulkTools.IntegrationTests.Data
{
    public class DataAccess
    {
        private readonly SqlServerAccess _dataAccess;

        public DataAccess()
        {
            _dataAccess = new SqlServerAccess(ConfigurationManager.ConnectionStrings["SqlBulkToolsTest"].ConnectionString);
        }
        public List<Book> GetBookList(string isbn = null)
        {

            var books = _dataAccess.Sproc()
                .AddSqlParameter("@Isbn", isbn)
                .ExecuteReader<Book>("dbo.GetBooks")
                .ToList();

            return books;

        }

        public int GetBookCount()
        {

            var bookCount = _dataAccess.Sproc()
                .ExecuteScalar<int>("dbo.GetBookCount");
            return bookCount;

        }

        public List<SchemaTest1> GetSchemaTest1List()
        {

            var schemaTestList = _dataAccess.Sproc()
                .AddSqlParameter("@Schema", "dbo")
                .ExecuteReader<SchemaTest1>("dbo.GetSchemaTest")
                .ToList();

            return schemaTestList;

        }

        public List<SchemaTest2> GetSchemaTest2List()
        {

            var schemaTestList = _dataAccess.Sproc()
                .AddSqlParameter("@Schema", "AnotherSchema")
                .ExecuteReader<SchemaTest2>("dbo.GetSchemaTest")
                .ToList();

            return schemaTestList;

        }

        public List<CustomColumnMappingTest> GetCustomColumnMappingTests()
        {

            var customColumnMappingTests = _dataAccess.Sproc()
                .CustomColumnMapping<CustomColumnMappingTest>(x => x.NaturalIdTest, "NaturalId")
                .CustomColumnMapping<CustomColumnMappingTest>(x => x.ColumnXIsDifferent, "ColumnX")
                .CustomColumnMapping<CustomColumnMappingTest>(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                .ExecuteReader<CustomColumnMappingTest>("dbo.GetCustomColumnMappingTests")
                .ToList();

            return customColumnMappingTests;

        }

        public List<ReservedColumnNameTest> GetReservedColumnNameTests()
        {
            var reservedColumnNameTests = _dataAccess.Sproc()
                .ExecuteReader<ReservedColumnNameTest>("dbo.GetReservedColumnNameTests")
                .ToList();

            return reservedColumnNameTests;

        }

        public int GetComplexTypeModelCount()
        {
            return _dataAccess.Sproc()
                .ExecuteScalar<int>("dbo.GetComplexModelCount");

        }

        public void ReseedBookIdentity(int idStart)
        {
            _dataAccess.Sproc()
                .AddSqlParameter("@IdStart", idStart)
                .ExecuteNonQuery("dbo.ReseedBookIdentity");

        }

        public List<CustomIdentityColumnNameTest> GetCustomIdentityColumnNameTestList()
        {
            return _dataAccess.Sproc()
                .CustomColumnMapping<CustomIdentityColumnNameTest>(x => x.Id, "ID_COMPANY")
                .ExecuteReader<CustomIdentityColumnNameTest>("dbo.GetCustomIdentityColumnNameTestList")
                .ToList();

        }
    }
}
