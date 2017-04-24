using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using SqlBulkTools.Core;
using SqlBulkTools.TestCommon.Model;
using SqlBulkTools.TestCommon;

namespace SqlBulkTools.UnitTests
{
    [TestClass]
    public class BulkOperationsTests
    {

        [TestMethod]
        public void GetTableAndSchema_WhenNoSchemaIsSpecified()
        {
            var expectedSchema = "dbo";
            var expectedTableName = "MyTable";

            var result = BulkOperationsHelper.GetTableAndSchema("MyTable");

            Assert.AreEqual(result.Name, expectedTableName);
            Assert.AreEqual(result.Schema, expectedSchema);
        }

        [TestMethod]
        public void GetTableAndSchema_WhenASchemaIsSpecified_WithNoFormatting()
        {
            var expectedSchema = "TestSchema";
            var expectedTableName = "MyTable";

            var result = BulkOperationsHelper.GetTableAndSchema("TestSchema.MyTable");

            Assert.AreEqual(result.Name, expectedTableName);
            Assert.AreEqual(result.Schema, expectedSchema);
        }

        [TestMethod]
        public void GetTableAndSchema_WhenASchemaIsSpecified_WithFormatting1()
        {
            var expectedSchema = "TestSchema";
            var expectedTableName = "MyTable";

            var result = BulkOperationsHelper.GetTableAndSchema("[TestSchema].[MyTable]");

            Assert.AreEqual(result.Name, expectedTableName);
            Assert.AreEqual(result.Schema, expectedSchema);
        }

        [TestMethod]
        public void GetTableAndSchema_WhenASchemaIsSpecified_WithFormatting2()
        {
            var expectedSchema = "TestSchema";
            var expectedTableName = "MyTable";

            var result = BulkOperationsHelper.GetTableAndSchema("[TestSchema].MyTable");

            Assert.AreEqual(result.Name, expectedTableName);
            Assert.AreEqual(result.Schema, expectedSchema);
        }

        [TestMethod]
        public void GetTableAndSchema_WhenASchemaIsSpecified_WithFormatting3()
        {
            var expectedSchema = "TestSchema";
            var expectedTableName = "MyTable";

            var result = BulkOperationsHelper.GetTableAndSchema("TestSchema.[MyTable]");

            Assert.AreEqual(result.Name, expectedTableName);
            Assert.AreEqual(result.Schema, expectedSchema);
        }

        [TestMethod]
        [MyExpectedException(typeof(SqlBulkToolsException), "Table name can't contain more than one period '.' character.")]
        public void GetTableAndSchema_WithAnInvalidName()
        {
            var result = BulkOperationsHelper.GetTableAndSchema("TestSchema.InvalidName.MyTable");
        }

        [TestMethod]
        public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWithThreeConditions()
        {
            // Arrange
            List<string> joinOnList = new List<string>() { "MarketPlaceId", "FK_BusinessId", "AddressId" };

            // Act
            var result = BulkOperationsHelper.BuildJoinConditionsForInsertOrUpdate(joinOnList.ToArray(), "Source", "Target", new Dictionary<string, string>(), new Dictionary<string, bool>());

            // Assert
            Assert.AreEqual("ON ([Target].[MarketPlaceId] = [Source].[MarketPlaceId]) AND ([Target].[FK_BusinessId] = [Source].[FK_BusinessId]) AND ([Target].[AddressId] = [Source].[AddressId]) ", result);
        }

        [TestMethod]
        public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWithTwoConditions()
        {
            // Arrange
            List<string> joinOnList = new List<string>() { "MarketPlaceId", "FK_BusinessId" };

            // Act
            var result = BulkOperationsHelper.BuildJoinConditionsForInsertOrUpdate(joinOnList.ToArray(), "Source", "Target", new Dictionary<string, string>() { { "FK_BusinessId", "DEFAULT_COLLATION" } }, new Dictionary<string, bool>());

            // Assert
            Assert.AreEqual("ON ([Target].[MarketPlaceId] = [Source].[MarketPlaceId]) AND ([Target].[FK_BusinessId] = [Source].[FK_BusinessId] COLLATE DEFAULT_COLLATION) ", result);
        }

        [TestMethod]
        public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWitSingleCondition()
        {
            // Arrange
            List<string> joinOnList = new List<string>() { "MarketPlaceId" };

            // Act
            var result = BulkOperationsHelper.BuildJoinConditionsForInsertOrUpdate(joinOnList.ToArray(), "Source", "Target", new Dictionary<string, string>(), new Dictionary<string, bool>());

            // Assert
            Assert.AreEqual("ON ([Target].[MarketPlaceId] = [Source].[MarketPlaceId]) ", result);
        }

        [TestMethod]
        public void BulkOperationsHelpers_BuildUpdateSet_BuildsCorrectSequenceForMultipleColumns()
        {
            // Arrange
            var updateOrInsertColumns = GetTestColumns();
            var expected =
                "SET [Target].[Email] = [Source].[Email], [Target].[id] = [Source].[id], [Target].[IsCool] = [Source].[IsCool], [Target].[Name] = [Source].[Name], [Target].[Town] = [Source].[Town] ";

            // Act
            var result = BulkOperationsHelper.BuildUpdateSet(updateOrInsertColumns, "Source", "Target", null);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [TestMethod]
        public void BulkOperationsHelpers_BuildUpdateSet_BuildsCorrectSequenceForSingleColumn()
        {
            // Arrange
            var updateOrInsertColumns = new HashSet<string>();
            updateOrInsertColumns.Add("Id");

            var expected =
                "SET [Target].[Id] = [Source].[Id] ";

            // Act
            var result = BulkOperationsHelper.BuildUpdateSet(updateOrInsertColumns, "Source", "Target", null);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [TestMethod]
        public void BulkOperationsHelpers_BuildMergeInsert_BuildsCorrectSequenceForMultipleColumns()
        {
            // Arrange
            var updateOrInsertColumns = GetTestColumns();
            var expected =
                "INSERT ([Email], [IsCool], [Name], [Town]) values ([Source].[Email], [Source].[IsCool], [Source].[Name], [Source].[Town])";

            // Act
            var result = BulkOperationsHelper.BuildMergeInsert(updateOrInsertColumns, "Source", "id");

            // Assert
            Assert.AreEqual(expected, result);

        }

        [TestMethod]
        public void BulkOperationsHelpers_BuildInsertIntoSet_BuildsCorrectSequenceForSingleColumn()
        {
            // Arrange
            var columns = new HashSet<string>();
            columns.Add("Id");
            var tableName = "TableName";

            var expected = "INSERT INTO TableName ([Id]) ";

            // Act
            var result = BulkOperationsHelper.BuildInsertIntoSet(columns, null, tableName);

            // Assert
            Assert.AreEqual(result, expected);
        }

        [TestMethod]
        public void BulkOperationsHelpers_BuildInsertIntoSet_BuildsCorrectSequenceForMultipleColumns()
        {
            var columns = GetTestColumns();
            var tableName = "TableName";
            var expected =
                "INSERT INTO TableName ([Email], [IsCool], [Name], [Town]) ";

            // Act
            var result = BulkOperationsHelper.BuildInsertIntoSet(columns, "id", tableName);

            // Assert
            Assert.AreEqual(result, expected);

        }

        [TestMethod]
        public void BulkOperationsHelpers_BuildMergeInsert_BuildsCorrectSequenceForSingleColumn()
        {
            // Arrange
            var updateOrInsertColumns = new HashSet<string>();
            updateOrInsertColumns.Add("Id");
            var expected =
                "INSERT ([Id]) values ([Source].[Id])";

            // Act
            var result = BulkOperationsHelper.BuildMergeInsert(updateOrInsertColumns, "Source", null);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [TestMethod]
        public void BulkOperationsHelper_GetAllPropertiesForComplexType_ReturnsCorrectSet()
        {
            // Arrange
            HashSet<string> expected = new HashSet<string>() { "AverageEstimate_TotalCost", "AverageEstimate_CreationDate", "Competition", "Id", "MinEstimate_TotalCost", "MinEstimate_CreationDate", "SearchVolume" };
            List<PropertyInfo> propertyInfoList = typeof(ComplexTypeModel).GetProperties().OrderBy(x => x.Name).ToList();

            // Act
            var result = BulkOperationsHelper.GetAllValueTypeAndStringColumns(propertyInfoList, typeof(ComplexTypeModel));

            // Assert
            CollectionAssert.AreEqual(expected.ToList(), result.ToList());
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildValueSetFromDataTable_WithSingleValue()
        {
            var schemaDetail = new SchemaDetail()
            {
                DateTimeTypePrecisionDic = new Dictionary<string, string>(),
                MaxCharDic = new Dictionary<string, string>(),
                NumericPrecisionTypeDic = new Dictionary<string, PrecisionType>(),
                NullableDic = new Dictionary<string, bool>()
            };
            var bookList = GetBookList().Where(x => x.Title == "Book1");
            HashSet<string> columns = new HashSet<string>() { "Description", "Id", "ISBN", "Title"  };
            Dictionary<string, int> ordinalDic = new Dictionary<string, int>();
            List<PropertyInfo> propertyInfoList = typeof(Book).GetProperties().OrderBy(x => x.Name).ToList();

            var dataTable = BulkOperationsHelper.CreateDataTable<Book>(propertyInfoList, columns, null, ordinalDic);
            BulkOperationsHelper.ConvertListToDataTable(propertyInfoList, dataTable, bookList, columns, ordinalDic);

            var result = BulkOperationsHelper.BuildInsertQueryFromDataTable(new Dictionary<string, string>(), dataTable, "Id", columns, null, schemaDetail, Constants.TempTableName);
            var result2 = BulkOperationsHelper.BuildInsertQueryFromDataTable(new Dictionary<string, string>(), dataTable, "Id", columns, 
                new BulkCopySettings(){SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity}, schemaDetail, Constants.TempTableName);

            Assert.AreEqual("INSERT INTO #TmpTable ([Description], [ISBN], [Title]) VALUES (@Description1, @ISBN1, @Title1)", result.InsertQuery);
            Assert.AreEqual("INSERT INTO #TmpTable ([Description], [Id], [ISBN], [Title]) VALUES (@Description1, @Id1, @ISBN1, @Title1)", result2.InsertQuery);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildValueSetFromDataTable_WithMultipleValues()
        {
            var schemaDetail = new SchemaDetail()
            {
                DateTimeTypePrecisionDic = new Dictionary<string, string>(),
                MaxCharDic = new Dictionary<string, string>(),
                NumericPrecisionTypeDic = new Dictionary<string, PrecisionType>(),
                NullableDic = new Dictionary<string, bool>()
            };
            var bookList = GetBookList();
            HashSet<string> columns = new HashSet<string>() { "Description", "Id", "ISBN", "Title" };
            Dictionary<string, int> ordinalDic = new Dictionary<string, int>();
            List<PropertyInfo> propertyInfoList = typeof(Book).GetProperties().OrderBy(x => x.Name).ToList();

            var dataTable = BulkOperationsHelper.CreateDataTable<Book>(propertyInfoList, columns, null, ordinalDic);
            BulkOperationsHelper.ConvertListToDataTable(propertyInfoList, dataTable, bookList, columns, ordinalDic);

            var result = BulkOperationsHelper.BuildInsertQueryFromDataTable(new Dictionary<string, string>(), dataTable, "Id", columns, null, schemaDetail, Constants.TempTableName);
            var result2 = BulkOperationsHelper.BuildInsertQueryFromDataTable(new Dictionary<string, string>(), dataTable, "Id", columns,
                new BulkCopySettings() { SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity }, schemaDetail, Constants.TempTableName);

            Assert.AreEqual("INSERT INTO #TmpTable ([Description], [ISBN], [Title]) VALUES (@Description1, @ISBN1, @Title1), " +
                            "(@Description2, @ISBN2, @Title2), (@Description3, @ISBN3, @Title3)", result.InsertQuery);

            Assert.AreEqual("INSERT INTO #TmpTable ([Description], [Id], [ISBN], [Title]) VALUES (@Description1, @Id1, @ISBN1, @Title1), " +
                            "(@Description2, @Id2, @ISBN2, @Title2), (@Description3, @Id3, @ISBN3, @Title3)", result2.InsertQuery);
        }

        [TestMethod]
        public void BulkOperationsHelper_CreateDataTableForComplexType_IsStructuredCorrectly()
        {
            HashSet<string> columns = new HashSet<string>() { "AverageEstimate_TotalCost", "AverageEstimate_CreationDate", "Competition", "MinEstimate_TotalCost", "MinEstimate_CreationDate", "SearchVolume" };
            List<PropertyInfo> propertyInfoList = typeof(ComplexTypeModel).GetProperties().OrderBy(x => x.Name).ToList();

            var result = BulkOperationsHelper.CreateDataTable<ComplexTypeModel>(propertyInfoList, columns, null, new Dictionary<string, int>());

            Assert.AreEqual(result.Columns["AverageEstimate_TotalCost"].DataType, typeof(double), "AverageEstimate_TotalCost");
            Assert.AreEqual(result.Columns["AverageEstimate_CreationDate"].DataType, typeof(DateTime), "AverageEstimate_CreationDate");
            Assert.AreEqual(result.Columns["MinEstimate_TotalCost"].DataType, typeof(double), "MinEstimate_TotalCost");
            Assert.AreEqual(result.Columns["MinEstimate_CreationDate"].DataType, typeof(DateTime), "MinEstimate_CreationDate");
            Assert.AreEqual(result.Columns["SearchVolume"].DataType, typeof(double), "SearchVolume");
            Assert.AreEqual(result.Columns["Competition"].DataType, typeof(double), "Competition");
        }

        [TestMethod]
        public void BulkOperationsHelpers_GetAllValueTypeAndStringColumns_ReturnsCorrectSet()
        {
            // Arrange
            HashSet<string> expected = new HashSet<string>() { "BoolTest", "CreatedTime", "IntegerTest", "Price", "Title" };
            List<PropertyInfo> propertyInfoList = typeof(ModelWithMixedTypes).GetProperties().OrderBy(x => x.Name).ToList();

            // Act
            var result = BulkOperationsHelper.GetAllValueTypeAndStringColumns(propertyInfoList, typeof(ModelWithMixedTypes));

            // Assert
            CollectionAssert.AreEqual(expected.ToList(), result.ToList());
        }
      

        [TestMethod]
        public void BulkOperationsHelpers_GetIndexManagementCmd_WhenDisableAllIndexesIsTrueReturnsCorrectCmd()
        {
            // Arrange
            string expected =
                @"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;' FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' AND sys.objects.name = '[SqlBulkTools].[dbo].[Books]'; EXEC(@sql);";
            var databaseName = "SqlBulkTools";

            var sqlConnMock = new Mock<IDbConnection>();
            sqlConnMock.Setup(x => x.Database).Returns(databaseName);

            // Act
            string result = BulkOperationsHelper.GetIndexManagementCmd(Constants.Disable, "Books", "dbo", sqlConnMock.Object);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [TestMethod]
        public void BulkOperationsHelpers_RebuildSchema_WithExplicitSchemaIsCorrect()
        {
            // Arrange
            string expected = "[db].[CustomSchemaName].[TableName]";

            // Act
            string result = BulkOperationsHelper.GetFullQualifyingTableName("db", "CustomSchemaName", "TableName");

            // Act
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void BulkOperationsHelper_GetDropTmpTableCmd_ReturnsCorrectCmd()
        {
            // Arrange
            var expected = "DROP TABLE #TmpOutput;";

            // Act
            var result = BulkOperationsHelper.GetDropTmpTableCmd();

            // Assert
            Assert.AreEqual(expected, result);

        }

        [TestMethod]
        public void BulkOperationsHelper_BuildPredicateQuery_LessThanDecimalCondition()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>()
            {
                new PredicateCondition()
                {
                    Expression = ExpressionType.LessThan,
                    LeftName = "Price",
                    Value = "50",
                    ValueType = typeof (decimal),
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[Price] < @PriceCondition1 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, new Dictionary<string, string>());

            // Assert
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildPredicateQuery_IsNullCondition()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>()
            {
                new PredicateCondition()
                {
                    Expression = ExpressionType.Equal,
                    LeftName = "Description",
                    Value = "null",
                }
            };

            var expected = "AND [Target].[Description] IS NULL ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildPredicateQuery_IsNotNullCondition()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>()
            {
                new PredicateCondition()
                {
                    Expression = ExpressionType.NotEqual,
                    LeftName = "Description",
                    Value = "null",
                }
            };

            var expected = "AND [Target].[Description] IS NOT NULL ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildPredicateQuery_LessThan()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>()
            {
                new PredicateCondition()
                {
                    Expression = ExpressionType.LessThan,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[Description] < @DescriptionCondition1 COLLATE DEFAULT_COLLATION ";

            var hashSet = new HashSet<string>() { "DEFAULT_COLLATION" };

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, new Dictionary<string, string>() { { "Description", "DEFAULT_COLLATION" } });

            // Assert
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildPredicateQuery_LessThanOrEqualTo()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>()
            {
                new PredicateCondition()
                {
                    Expression = ExpressionType.LessThanOrEqual,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[Description] <= @DescriptionCondition1 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildPredicateQuery_GreaterThan()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>()
            {
                new PredicateCondition()
                {
                    Expression = ExpressionType.GreaterThan,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[Description] > @DescriptionCondition1 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildPredicateQuery_GreaterThanOrEqualTo()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>()
            {
                new PredicateCondition()
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[Description] >= @DescriptionCondition1 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildPredicateQuery_CustomColumnMapping()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>()
            {
                new PredicateCondition()
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Description",
                    Value = "null",
                    CustomColumnMapping = "ShortDescription",
                    SortOrder = 1
                }
            };

            var expected = "AND [Target].[ShortDescription] >= @DescriptionCondition1 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildPredicateQuery_MultipleConditions()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<PredicateCondition>()
            {
                new PredicateCondition()
                {
                    Expression = ExpressionType.NotEqual,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                },
                new PredicateCondition()
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Price",
                    Value = "50",
                    ValueType = typeof(decimal),
                    SortOrder = 2
                },
            };

            var expected = "AND [Target].[Description] IS NOT NULL AND [Target].[Price] >= @PriceCondition2 ";

            // Act
            var result = BulkOperationsHelper.BuildPredicateQuery(updateOn, conditions, targetAlias, null);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        [ExpectedException(typeof(SqlBulkToolsException))]
        public void BulkOperationsHelper_BuildPredicateQuery_ThrowsWhenUpdateOnColIsEmpty()
        {
            // Arrange
            var targetAlias = "Target";
            var updateOn1 = new string[0];

            var conditions = new List<PredicateCondition>()
            {
                new PredicateCondition()
                {
                    Expression = ExpressionType.NotEqual,
                    LeftName = "Description",
                    Value = "null",
                    SortOrder = 1
                },
                new PredicateCondition()
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Price",
                    Value = "50",
                    ValueType = typeof(decimal),
                    SortOrder = 2
                },
            };

            BulkOperationsHelper.BuildPredicateQuery(updateOn1, conditions, targetAlias, null);
            BulkOperationsHelper.BuildPredicateQuery(null, conditions, targetAlias, null);
            
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildValueSet_WithOneValue()
        {
            // Arrange
            HashSet<String> columns = new HashSet<string>();
            columns.Add("TestColumn");

            // Act
            string result = BulkOperationsHelper.BuildValueSetForParameterisedColumns(columns, "Id");

            // Assert
            Assert.AreEqual("(@TestColumn)", result);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildValueSet_WithMultipleValues()
        {
            // Arrange
            HashSet<String> columns = new HashSet<string>();
            columns.Add("TestColumnA");
            columns.Add("TestColumnB");

            // Act
            string result = BulkOperationsHelper.BuildValueSetForParameterisedColumns(columns, "Id");

            // Assert
            Assert.AreEqual("(@TestColumnA, @TestColumnB)", result);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildValueSet_WithMultipleValuesWhenIdentitySet()
        {
            // Arrange
            HashSet<String> columns = new HashSet<string>();
            columns.Add("TestColumnA");
            columns.Add("TestColumnB");
            columns.Add("Id");

            // Act
            string result = BulkOperationsHelper.BuildValueSetForParameterisedColumns(columns, "Id");

            // Assert
            Assert.AreEqual("(@TestColumnA, @TestColumnB)", result);
        }

        [TestMethod]
        public void BulkOperationsHelper_AddSqlParamsForUpdateQuery_GetsTypeAndValue()
        {
            Book book = new Book()
            {
                ISBN = "Some ISBN",
                Price = 23.99M,
                BestSeller = true
            };

            HashSet<string> columns = new HashSet<string>();
            columns.Add("ISBN");
            columns.Add("Price");
            columns.Add("BestSeller");

            List<SqlParameter> sqlParams = new List<SqlParameter>();
            List<PropertyInfo> propertyInfoList = typeof(Book).GetProperties().OrderBy(x => x.Name).ToList();


            BulkOperationsHelper.AddSqlParamsForQuery(propertyInfoList, sqlParams, columns, book);

            Assert.AreEqual(3, sqlParams.Count);
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildMatchTargetOnListWithMultipleValues_ReturnsCorrectString()
        {
            // Arrange
            var columns = GetTestColumns();

            // ACt
            var result = BulkOperationsHelper.BuildMatchTargetOnList(columns, null, new Dictionary<string, string>());

            // Assert
            Assert.AreEqual(result, "WHERE [id] = @id AND [Name] = @Name AND [Town] = @Town AND [Email] = @Email AND [IsCool] = @IsCool");
        }

        [TestMethod]
        public void BulkOperationsHelper_BuildMatchTargetOnListWithSingleValue_ReturnsCorrectString()
        {
            // Arrange
            var columns = new HashSet<string>() { "id" };

            // ACt
            var result = BulkOperationsHelper.BuildMatchTargetOnList(columns, new Dictionary<string, string>() { { "id", "DEFAULT_COLLATION" } }, new Dictionary<string, string>());

            // Assert
            Assert.AreEqual(result, "WHERE [id] = @id COLLATE DEFAULT_COLLATION");
        }

        private HashSet<string> GetTestColumns()
        {
            HashSet<string> parameters = new HashSet<string>();

            parameters.Add("id");
            parameters.Add("Name");
            parameters.Add("Town");
            parameters.Add("Email");
            parameters.Add("IsCool");

            return parameters;
        }

        private List<Book> GetBookList()
        {
            List<Book> bookList = new List<Book>()
            {
                new Book(){Description = "This is book 1", Id = 0, ISBN = "123456789", Title = "Book1"},
                new Book(){Description = "This is book 2", Id = 0, ISBN = "123456789", Title = "Book2"},
                new Book(){Description = "This is book 3", Id = 0, ISBN = "123456789", Title = "Book3"}
            };

            return bookList;
        }
    }
}
