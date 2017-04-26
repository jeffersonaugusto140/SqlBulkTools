using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Transactions;
using Microsoft.SqlServer.Types;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Ploeh.AutoFixture;
using SqlBulkTools.Enumeration;
using SqlBulkTools.TestCommon;
using SqlBulkTools.TestCommon.Model;
using SqlBulkTools.IntegrationTests.Data;
using System.Threading.Tasks;

namespace SqlBulkTools.IntegrationTests
{
    [TestClass]
    public class BulkOperationsAsyncIt
    {
        private const int RepeatTimes = 1;
        private DataAccess _dataAccess;
        private BookRandomizer _randomizer;
        private List<Book> _bookCollection;
        private string _connectionString;

        [TestInitialize]
        public void Setup()
        {
            _dataAccess = new DataAccess();
            _randomizer = new BookRandomizer();
            _connectionString = ConfigurationManager.ConnectionStrings["SqlBulkToolsTest"].ConnectionString;
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdate_ExcludeAllColumnsFromUpdate()
        {
            const int rows = 20;
            var bulk = new BulkOperations();

            // Clean up existing records in Books table.
            await BulkDelete(_dataAccess.GetBookList());

            _bookCollection = new List<Book>();

            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));

            decimal dustyBookPrice = 49.99M;
            string dustyBookTitle = "Snapchat for Dummies";
            DateTime dustyBookCreatedAt = DateTime.UtcNow;

            // Stick this book in the middle of the collection.
            var dustyBook = new Book()
            {
                ISBN = "123456789111",
                Price = dustyBookPrice,
                Title = dustyBookTitle,
                CreatedAt = dustyBookCreatedAt
            };           

            _bookCollection.Add(dustyBook);

            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));

            Book brandNewBook = null;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(_bookCollection)
                        .WithTable("dbo.Books")
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.CreatedAt)
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .ExcludeAllColumnsFromUpdate()
                        .CommitAsync(conn);

                    // Update old record in memory... For test to be successful, this will be ignored from update. 
                    var dustyBookDetails = _bookCollection.First(x => x.ISBN.Equals("123456789111"));

                    dustyBookDetails.CreatedAt = DateTime.UtcNow;
                    dustyBookDetails.Title = "Snapchat for Dummies 2: Adult edition";
                    dustyBookDetails.Price = 69.99M;

                    // Add a new record to existing collection... For test to be successful, this record will be inserted.
                    brandNewBook = new Book()
                    {
                        ISBN = "9827349872398",
                        Price = 88.88M,
                        Title = "Elon Musk really is an alien in disguise",
                        CreatedAt = DateTime.UtcNow
                    };

                    _bookCollection.Add(brandNewBook);

                    // Rerun BulkInsertOrUpdate
                    await bulk.Setup<Book>()
                        .ForCollection(_bookCollection)
                        .WithTable("dbo.Books")
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.CreatedAt)
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .ExcludeAllColumnsFromUpdate()
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var dustyBookInDb = _dataAccess.GetBookList(dustyBook.ISBN).Single();
            var brandNewBookInDb = _dataAccess.GetBookList(brandNewBook.ISBN).SingleOrDefault();

            // Ensure dustybook hasn't changed
            Assert.AreEqual(dustyBookPrice, dustyBookInDb.Price);
            Assert.AreEqual(dustyBookTitle, dustyBookInDb.Title);
            Assert.IsTrue((dustyBookCreatedAt - dustyBookInDb.CreatedAt) < TimeSpan.FromSeconds(1));

            // Ensure brandNewBook is inserted
            Assert.IsNotNull(brandNewBookInDb);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdate_PassesWithCustomIdentityColumn()
        {
            const int rows = 30;
            var bulk = new BulkOperations(); 
            List<CustomIdentityColumnNameTest> customIdentityColumnList = new List<CustomIdentityColumnNameTest>();

            for (int i = 0; i < rows; i++)
            {
                customIdentityColumnList.Add(new CustomIdentityColumnNameTest
                {
                    ColumnA = i.ToString()
                });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<CustomIdentityColumnNameTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomIdentityColumnNameTest")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<CustomIdentityColumnNameTest>()
                        .ForCollection(customIdentityColumnList)
                        .WithTable("CustomIdentityColumnNameTest")
                        .AddColumn(x => x.Id, "ID_COMPANY")
                        .AddColumn(x => x.ColumnA)
                        .BulkInsertOrUpdate()
                        .SetIdentityColumn(x => x.Id)
                        .MatchTargetOn(x => x.ColumnA)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.IsTrue(_dataAccess.GetCustomIdentityColumnNameTestList().Count == rows);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertForComplexType_AddAllColumns()
        {
            const int rows = 30;
            var bulk = new BulkOperations();
            List<ComplexTypeModel> complexTypeModelList = new List<ComplexTypeModel>();

            for (int i = 0; i < rows; i++)
            {
                complexTypeModelList.Add(new ComplexTypeModel
                {
                    AverageEstimate = new EstimatedStats
                    {
                        TotalCost = 23.20
                    },
                    MinEstimate = new EstimatedStats
                    {
                        TotalCost = 10.20
                    },
                    Competition = 30,
                    SearchVolume = 234.34

                });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<ComplexTypeModel>()
                        .ForDeleteQuery()
                        .WithTable("ComplexTypeTest")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<ComplexTypeModel>()
                        .ForCollection(complexTypeModelList)
                        .WithTable("ComplexTypeTest")
                        .AddAllColumns()
                        .BulkInsert()
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.IsTrue(_dataAccess.GetComplexTypeModelCount() > 0);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdateForComplexType_AddAllColumns()
        {
            const int rows = 30;
            var bulk = new BulkOperations();
            List<ComplexTypeModel> complexTypeModelList = new List<ComplexTypeModel>();

            for (int i = 0; i < rows; i++)
            {
                complexTypeModelList.Add(new ComplexTypeModel
                {
                    AverageEstimate = new EstimatedStats
                    {
                        TotalCost = 23.20
                    },
                    MinEstimate = new EstimatedStats
                    {
                        TotalCost = 10.20
                    },
                    Competition = 30,
                    SearchVolume = 234.34

                });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<ComplexTypeModel>()
                        .ForDeleteQuery()
                        .WithTable("ComplexTypeTest")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<ComplexTypeModel>()
                        .ForCollection(complexTypeModelList)
                        .WithTable("ComplexTypeTest")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.Id)
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.IsTrue(_dataAccess.GetComplexTypeModelCount() > 0);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertForComplexType_AddColumnsManually()
        {
            const int rows = 30;
            var bulk = new BulkOperations();
            List<ComplexTypeModel> complexTypeModelList = new List<ComplexTypeModel>();

            for (int i = 0; i < rows; i++)
            {
                complexTypeModelList.Add(new ComplexTypeModel
                {
                    AverageEstimate = new EstimatedStats
                    {
                        TotalCost = 23.20
                    },
                    MinEstimate = new EstimatedStats
                    {
                        TotalCost = 10.20
                    },
                    Competition = 30,
                    SearchVolume = 234.34

                });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<ComplexTypeModel>()
                        .ForDeleteQuery()
                        .WithTable("ComplexTypeTest")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<ComplexTypeModel>()
                        .ForCollection(complexTypeModelList)
                        .WithTable("ComplexTypeTest")
                        .AddColumn(x => x.AverageEstimate.CreationDate)
                        .AddColumn(x => x.AverageEstimate.TotalCost)
                        .AddColumn(x => x.Competition)
                        .AddColumn(x => x.MinEstimate.CreationDate, "MinEstimate_CreationDate") // Testing custom column mapping
                        .AddColumn(x => x.MinEstimate.TotalCost)
                        .AddColumn(x => x.SearchVolume)
                        .BulkInsert()
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.IsTrue(_dataAccess.GetComplexTypeModelCount() > 0);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsert()
        {
            const int rows = 1000;

            await BulkDelete(_dataAccess.GetBookList());
            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            List<long> results = new List<long>();

            Trace.WriteLine("Testing BulkInsert with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = await BulkInsert(_bookCollection);

                results.Add(time);
            }
            double avg = results.Average(l => l);

            Trace.WriteLine("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            Assert.AreEqual(rows * RepeatTimes, _dataAccess.GetBookCount());
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsert_WithAllColumns()
        {
            const int rows = 1000;

            await BulkDelete(_dataAccess.GetBookList());

            List<Book> randomCollection = _randomizer.GetRandomCollection(rows);

            await BulkInsertAllColumns(randomCollection);

            var expected = randomCollection.First();
            var actual = _dataAccess.GetBookList(isbn: expected.ISBN).First();

            Assert.AreEqual(expected.Title, actual.Title);
            Assert.AreEqual(expected.Description, actual.Description);
            Assert.AreEqual(expected.Price, actual.Price);
            Assert.AreEqual(expected.WarehouseId, actual.WarehouseId);
            Assert.AreEqual(expected.BestSeller, actual.BestSeller);

            Assert.AreEqual(rows * RepeatTimes, _dataAccess.GetBookCount());
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdate()
        {
            const int rows = 500, newRows = 500;

            await BulkDelete(_dataAccess.GetBookList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            Trace.WriteLine("Testing BulkInsertOrUpdate with " + (rows + newRows) + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                await BulkInsert(_bookCollection);

                // Update some rows
                for (int j = 0; j < 200; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;
                }

                // Add new rows
                _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));


                long time = await BulkInsertOrUpdate(_bookCollection);
                results.Add(time);

                Assert.AreEqual(rows + newRows, _dataAccess.GetBookCount());

            }

            double avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }


        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdateAllColumns()
        {
            const int rows = 1000, newRows = 500;

            await BulkDelete(_dataAccess.GetBookList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            Trace.WriteLine("Testing BulkInsertOrUpdateAllColumns with " + (rows + newRows) + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                await BulkInsert(_bookCollection);

                // Update some rows
                for (int j = 0; j < 200; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;
                }

                // Add new rows
                _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));


                long time = await BulkInsertOrUpdateAllColumns(_bookCollection);
                results.Add(time);

                Assert.AreEqual(rows + newRows, _dataAccess.GetBookCount());

            }

            double avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }


        [TestMethod]
        public async Task SqlBulkTools_BulkUpdate()
        {
            const int rows = 1000;

            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());

            await BulkDelete(_dataAccess.GetBookList());

            List<long> results = new List<long>();

            Trace.WriteLine("Testing BulkUpdate with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {

                _bookCollection = _randomizer.GetRandomCollection(rows);
                await BulkInsert(_bookCollection);

                // Update half the rows
                for (int j = 0; j < rows / 2; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;

                }

                long time = await BulkUpdate(_bookCollection);
                results.Add(time);

                var testUpdate = _dataAccess.GetBookList().FirstOrDefault();
                Assert.AreEqual(_bookCollection[0].Price, testUpdate?.Price);
                Assert.AreEqual(_bookCollection[0].Title, testUpdate?.Title);
                Assert.AreEqual(_dataAccess.GetBookCount(), _bookCollection.Count);

                await BulkDelete(_bookCollection);
            }
            double avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }


        [TestMethod]
        public async Task SqlBulkTools_BulkUpdateOnIdentityColumn()
        {
            const int rows = 500;

            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());
            BulkOperations bulk = new BulkOperations();

            await BulkDelete(_dataAccess.GetBookList());
            _bookCollection = _randomizer.GetRandomCollection(rows);

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                    .ForCollection(_bookCollection)
                    .WithTable("Books")
                    .AddAllColumns()
                    .BulkInsert()
                    .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                    .CommitAsync(conn);


                    // Update half the rows
                    for (int j = 0; j < rows / 2; j++)
                    {
                        var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                        var prevId = _bookCollection[j].Id;
                        _bookCollection[j] = newBook;
                        _bookCollection[j].Id = prevId;
                    }

                    await bulk.Setup<Book>()
                        .ForCollection(_bookCollection)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .MatchTargetOn(x => x.Id)
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var testUpdate = _dataAccess.GetBookList().FirstOrDefault();
            Assert.AreEqual(_bookCollection[0].Price, testUpdate?.Price);
            Assert.AreEqual(_bookCollection[0].Title, testUpdate?.Title);
            Assert.AreEqual(_bookCollection.Count, _dataAccess.GetBookCount());
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkDelete()
        {
            const int rows = 500;

            _bookCollection = _randomizer.GetRandomCollection(rows);
            await BulkDelete(_dataAccess.GetBookList());

            List<long> results = new List<long>();

            Trace.WriteLine("Testing BulkDelete with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                await BulkInsert(_bookCollection);
                long time = await BulkDelete(_bookCollection);
                results.Add(time);
                Assert.AreEqual(0, _dataAccess.GetBookCount());
            }
            double avg = results.Average(l => l);
            Trace.WriteLine("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }

        [TestMethod]
        [MyExpectedException(typeof(IdentityException), "Cannot update identity column 'Id'. SQLBulkTools requires the SetIdentityColumn method to be configured if an identity column is being used. Please reconfigure your setup and try again.")]
        public async Task SqlBulkTools_IdentityColumnWhenNotSet_ThrowsIdentityException()
        {
            const int rows = 30;
            // Arrange
            await BulkDelete(_dataAccess.GetBookList());
            _bookCollection = _randomizer.GetRandomCollection(rows);

            BulkOperations bulk = new BulkOperations();

            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                await bulk.Setup<Book>()
                    .ForCollection(_bookCollection)
                    .WithTable("Books")
                    .AddAllColumns()
                    .BulkUpdate()
                    .MatchTargetOn(x => x.Id)
                    .CommitAsync(conn);
            }
        }

        [TestMethod]
        public async Task SqlBulkTools_IdentityColumnSet_UpdatesTargetWhenSetIdentityColumn()
        {
            // Arrange
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(rows);
            string testDesc = "New Description";

            await BulkInsert(_bookCollection);

            _bookCollection = _dataAccess.GetBookList();
            _bookCollection.First().Description = testDesc;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(_bookCollection)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .SetIdentityColumn(x => x.Id)
                        .MatchTargetOn(x => x.Id)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
            // Assert
            Assert.AreEqual(testDesc, _dataAccess.GetBookList().First().Description);
        }

        [TestMethod]
        public async Task SqlBulkTools_WithConflictingTableName_DeletesAndInsertsToCorrectTable()
        {
            // Arrange     
            const int rows = 30;      
            BulkOperations bulk = new BulkOperations();

            List<SchemaTest2> conflictingSchemaCol = new List<SchemaTest2>();

            for (int i = 0; i < rows; i++)
            {
                conflictingSchemaCol.Add(new SchemaTest2() { ColumnA = "ColumnA " + i });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<SchemaTest2>()
                        .ForCollection(conflictingSchemaCol)
                        .WithTable("AnotherSchema.SchemaTest")
                        .AddColumn(x => x.ColumnA)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ColumnA)
                        .CommitAsync(conn); // Remove existing rows

                    await bulk.Setup<SchemaTest2>()
                        .ForCollection(conflictingSchemaCol)
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn); // Add new rows

                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_dataAccess.GetSchemaTest2List().Any());

        }

        [TestMethod]
        public async Task SqlBulkTools_WithCustomSchema_WhenWithTableIncludesSchemaName()
        {
            // Arrange      
            const int rows = 30;     
            BulkOperations bulk = new BulkOperations();

            List<SchemaTest2> conflictingSchemaCol = new List<SchemaTest2>();

            for (int i = 0; i < rows; i++)
            {
                conflictingSchemaCol.Add(new SchemaTest2() { ColumnA = "ColumnA " + i });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<SchemaTest2>()
                        .ForCollection(conflictingSchemaCol)
                        .WithTable("AnotherSchema.SchemaTest")
                        .AddColumn(x => x.ColumnA)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ColumnA)
                        .CommitAsync(conn); // Remove existing rows

                    await bulk.Setup<SchemaTest2>()
                        .ForCollection(conflictingSchemaCol)
                        .WithTable("[AnotherSchema].[SchemaTest]")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn); // Add new rows
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_dataAccess.GetSchemaTest2List().Any());

        }

        [TestMethod]
        [MyExpectedException(typeof(SqlBulkToolsException), "Table name can't contain more than one period '.' character.")]
        public async Task SqlBulkTools_ThrowsException_WhenTableNameIsIncorrect()
        {
            // Arrange           
            BulkOperations bulk = new BulkOperations();

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<SchemaTest2>()
                        .ForCollection(new List<SchemaTest2>())
                        .WithTable("SchemaTest.AnotherSchema.TooManyPeriods")
                        .AddColumn(x => x.ColumnA)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ColumnA)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
        }

        [TestMethod]
        [MyExpectedException(typeof(SqlBulkToolsException), "Schema has already been defined in WithTable method.")]
        public async Task SqlBulkTools_ThrowsException_WhenSchemaDefinedTwice()
        {
            // Arrange           
            BulkOperations bulk = new BulkOperations();

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<SchemaTest2>()
                        .ForCollection(new List<SchemaTest2>())
                        .WithTable("SchemaTest.AnotherSchema")
                        .WithSchema("YetAnotherSchema")
                        .AddColumn(x => x.ColumnA)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ColumnA)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkDeleteOnId_AddItemsThenRemovesAllItems()
        {
            // Arrange    
            const int rows = 30;       
            BulkOperations bulk = new BulkOperations();

            List<SchemaTest1> col = new List<SchemaTest1>();

            for (int i = 0; i < rows; i++)
            {
                col.Add(new SchemaTest1() { ColumnB = "ColumnA " + i });
            }

            // Act
            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {

                    await bulk.Setup<SchemaTest1>()
                        .ForCollection(col)
                        .WithTable("SchemaTest") // Don't specify schema. Default schema dbo is used. 
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                }
                trans.Complete();
            }

            using (SqlConnection secondConn = new SqlConnection(_connectionString))
            {

                var allItems = _dataAccess.GetSchemaTest1List();
                await bulk.Setup<SchemaTest1>()
                    .ForCollection(allItems)
                    .WithTable("SchemaTest")
                    .AddColumn(x => x.Id)
                    .BulkDelete()
                    .MatchTargetOn(x => x.Id)
                    .CommitAsync(secondConn);
            }

            // Assert
            Assert.IsFalse(_dataAccess.GetSchemaTest1List().Any());
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkUpdate_PartialUpdateOnlyUpdatesSelectedColumns()
        {
            // Arrange
            const int rows = 30;
            BulkOperations bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            await BulkDelete(_dataAccess.GetBookList());
            await BulkInsert(_bookCollection);

            // Update just the price on element 5
            int elemToUpdate = 5;
            decimal updatedPrice = 9999999;
            var originalElement = _bookCollection.ElementAt(elemToUpdate);
            _bookCollection.ElementAt(elemToUpdate).Price = updatedPrice;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    // Act           
                    await bulk.Setup<Book>()
                        .ForCollection(_bookCollection)
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.AreEqual(updatedPrice, _dataAccess.GetBookList(originalElement.ISBN).First().Price);

            /* Profiler shows: MERGE INTO [SqlBulkTools].[dbo].[Books] WITH (HOLDLOCK) AS Target USING #TmpTable 
             * AS Source ON Target.ISBN = Source.ISBN WHEN MATCHED THEN UPDATE SET Target.Price = Source.Price, 
             * Target.ISBN = Source.ISBN ; DROP TABLE #TmpTable; */
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertWithColumnMappings_CorrectlyMapsColumns()
        {
            const int rows = 30;
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < rows; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
                        .BulkInsert()
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_dataAccess.GetCustomColumnMappingTests().Any());
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdateWithColumnMappings_CorrectlyMapsColumns()
        {
            const int rows = 30;
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < rows; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .SetBatchQuantity(5)
                        .CommitAsync(conn);

                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.NaturalIdTest)
                        .UpdateWhen(x => x.ColumnXIsDifferent != "me")
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_dataAccess.GetCustomColumnMappingTests().Any());
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdateWithManualColumnMappings_CorrectlyMapsColumns()
        {
            const int rows = 30;
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < rows; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {

                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddColumn(x => x.ColumnXIsDifferent, "ColumnX")
                        .AddColumn(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .AddColumn(x => x.NaturalIdTest, "NaturalId")
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.NaturalIdTest)
                        .UpdateWhen(x => x.ColumnXIsDifferent != "me")
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_dataAccess.GetCustomColumnMappingTests().Any());
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkUpdateWithManualColumnMappings_CorrectlyMapsColumns()
        {
            const int rows = 30;
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < rows; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddColumn(x => x.ColumnXIsDifferent, "ColumnX")
                        .AddColumn(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .AddColumn(x => x.NaturalIdTest, "NaturalId")
                        .BulkInsert()
                        .CommitAsync(conn);

                    foreach (var item in col)
                    {
                        item.ColumnXIsDifferent = "Updated";
                    }

                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddColumn(x => x.ColumnXIsDifferent, "ColumnX")
                        .AddColumn(x => x.NaturalIdTest, "NaturalId")
                        .BulkUpdate()
                        .MatchTargetOn(x => x.NaturalIdTest)
                        .UpdateWhen(x => x.ColumnXIsDifferent != "me")
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_dataAccess.GetCustomColumnMappingTests().First().ColumnXIsDifferent == "Updated");
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkUpdateWithColumnMappings_CorrectlyMapsColumns()
        {
            const int rows = 30;
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < rows; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalIdTest = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForDeleteQuery()
                        .WithTable("CustomColumnMappingTests")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
                        .BulkInsert()
                        .CommitAsync(conn);

                    foreach (var item in col)
                    {
                        item.ColumnXIsDifferent = "Updated";
                    }

                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForCollection(col)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .CustomColumnMapping(x => x.NaturalIdTest, "NaturalId")
                        .BulkUpdate()
                        .MatchTargetOn(x => x.NaturalIdTest)
                        .UpdateWhen(x => x.ColumnXIsDifferent != "me")
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_dataAccess.GetCustomColumnMappingTests().First().ColumnXIsDifferent == "Updated");
        }

        [TestMethod]
        public async Task SqlBulkTools_WhenUsingReservedSqlKeywords()
        {
            const int rows = 30;
            BulkOperations bulk = new BulkOperations();

            var list = new List<ReservedColumnNameTest>();

            for (int i = 0; i < rows; i++)
            {
                list.Add(new ReservedColumnNameTest() { Key = i });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<ReservedColumnNameTest>()
                        .ForDeleteQuery()
                        .WithTable("ReservedColumnNameTests")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<ReservedColumnNameTest>()
                        .ForCollection(list)
                        .WithTable("ReservedColumnNameTests")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.Id)
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.IsTrue(_dataAccess.GetReservedColumnNameTests().Count == rows);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdate_TestIdentityOutput()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType
                        .InputOutput).CommitAsync(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdate_TestNullComparisonWithMatchTargetOn()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            books.ElementAt(0).Title = "Test_Null_Comparison";
            books.ElementAt(0).ISBN = null;
            await BulkInsert(books);

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().Single(x => x.Title == "Test_Null_Comparison");

            Assert.AreEqual(rows, _dataAccess.GetBookList().Count);
            Assert.AreEqual(null, test.ISBN);

        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdate_ExcludeColumnTest()
        {
            const int rows = 30;
            // Remove existing records for a fresh test
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();
            // Get a list with random data
            List<Book> books = _randomizer.GetRandomCollection(rows);

            // Set the original date as the date Donald Trump somehow won the US election. 
            var originalDate = new DateTime(2016, 11, 9);
            // Set the new date as the date Trump's presidency will end
            var updatedDate = new DateTime(2020, 11, 9);

            // Add dates to initial list
            books.ForEach(x =>
            {
                x.CreatedAt = originalDate;
                x.ModifiedAt = originalDate;
            });

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {

                    // Insert initial list
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);

                    // Update list with new dates
                    books.ForEach(x =>
                    {
                        x.CreatedAt = updatedDate;
                        x.ModifiedAt = updatedDate;
                    });

                    // Insert a random record
                    books.Add(new Book() { CreatedAt = updatedDate, ModifiedAt = updatedDate, Price = 29.99M, Title = "Trump likes woman", ISBN = "1234567891011" });

                    /* CreatedAt is added if record doesn't exist, but 
                     * if this operation is repeated, it's ignored. */

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id)
                        .ExcludeColumnFromUpdate(x => x.CreatedAt)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
            string updatedIsbn = books[10].ISBN;
            string addedIsbn = books.Last().ISBN;
            var updatedBookUnderTest = _dataAccess.GetBookList(updatedIsbn).First();
            var createdBookUnderTest = _dataAccess.GetBookList(addedIsbn).First();

            Assert.AreEqual(updatedDate, updatedBookUnderTest.ModifiedAt); // The ModifiedAt should be updated
            Assert.AreEqual(originalDate, updatedBookUnderTest.CreatedAt); // The CreatedAt should be unchanged       
            Assert.AreEqual(updatedDate, createdBookUnderTest.CreatedAt); // CreatedAt should be new date because it was an insert
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdateWithSelectedColumns_TestIdentityOutput()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);

        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsert_TestIdentityOutput()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());

            BulkOperations bulk = new BulkOperations();
            List<Book> books = _randomizer.GetRandomCollection(rows);

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(17); // Random between random items before test and total items after test. 
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);
        }



        [TestMethod]
        public async Task SqlBulkTools_BulkInsertWithSelectedColumns_TestIdentityOutput()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());

            List<Book> books = _randomizer.GetRandomCollection(rows);

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    BulkOperations bulk = new BulkOperations();
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 5000
                        })
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.PublishDate)
                        .BulkInsert()
                        .TmpDisableAllNonClusteredIndexes()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }

            var actual = _dataAccess.GetBookList().ElementAt(15); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == actual.ISBN);

            Assert.AreEqual(expected.Id, actual.Id);
            Assert.AreEqual(expected.Title, actual.Title);
            Assert.AreEqual(expected.Price, actual.Price);
            Assert.AreEqual(expected.Description, actual.Description);
            Assert.AreEqual(expected.ISBN, actual.ISBN);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkDeleteWithSelectedColumns_TestIdentityOutput()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());

            _dataAccess.ReseedBookIdentity(10);

            List<Book> books = _randomizer.GetRandomCollection(rows);
            await BulkInsert(books);

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    BulkOperations bulk = new BulkOperations();
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 5000
                        })
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var test = books.First();

            Assert.IsTrue(test.Id == 10 || test.Id == 11);

            // Reset identity seed back to default
            _dataAccess.ReseedBookIdentity(0);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkUpdateWithSelectedColumns_TestIdentityOutput()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());

            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);
            await BulkInsert(books);

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            var test = _dataAccess.GetBookList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);
        }

        [TestMethod]
        [MyExpectedException(typeof(SqlBulkToolsException), "Only value, string, char[], byte[], SqlGeometry, SqlGeography and SqlXml types can be used with SqlBulkTools. Refer to https://msdn.microsoft.com/en-us/library/cc716729(v=vs.110).aspx for more details.")]
        public async Task SqlBulkTools_BulkInsertAddInvalidDataType_ThrowsSqlBulkToolsExceptionException()
        {
            const int rows = 30;
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);
            await BulkInsert(books);

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.InvalidType)
                        .BulkInsert()
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertWithGenericType()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            _bookCollection = _randomizer.GetRandomCollection(rows);

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup()
                    .ForCollection(_bookCollection.Select(x => new { x.Description, x.ISBN, x.Id, x.Price }))
                    .WithTable("Books")
                    .AddColumn(x => x.Id)
                    .AddColumn(x => x.Description)
                    .AddColumn(x => x.ISBN)
                    .AddColumn(x => x.Price)
                    .BulkInsert()
                    .SetIdentityColumn(x => x.Id)
                    .CommitAsync(conn);

                }

                trans.Complete();
            }

            Assert.IsTrue(_dataAccess.GetBookList().Any());
        }

        [TestMethod]
        [MyExpectedException(typeof(SqlBulkToolsException), "No setter method available on property 'Id'. Could not write output back to property.")]
        public async Task SqlBulkTools_BulkInsertWithoutSetter_ThrowsMeaningfulException()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            _bookCollection = _randomizer.GetRandomCollection(rows);

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {

                    await bulk.Setup()
                            .ForCollection(
                                _bookCollection.Select(
                                    x => new { x.Description, x.ISBN, x.Id, x.Price }))
                            .WithTable("Books")
                            .AddColumn(x => x.Id)
                            .AddColumn(x => x.Description)
                            .AddColumn(x => x.ISBN)
                            .AddColumn(x => x.Price)
                            .BulkInsert()
                            .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                            .CommitAsync(conn);
                }

                trans.Complete();
            }

        }

        [TestMethod]
        [MyExpectedException(typeof(SqlBulkToolsException), "No setter method available on property 'Id'. Could not write output back to property.")]
        public async Task SqlBulkTools_BulkInsertOrUpdateWithPrivateIdentityField_ThrowsMeaningfulException()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);
            List<BookWithPrivateIdentity> booksWithPrivateIdentity = new List<BookWithPrivateIdentity>();

            books.ForEach(x => booksWithPrivateIdentity.Add(new BookWithPrivateIdentity()
            {
                ISBN = x.ISBN,
                Description = x.Description,
                Price = x.Price

            }));

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<BookWithPrivateIdentity>()
                            .ForCollection(booksWithPrivateIdentity)
                            .WithTable("Books")
                            .AddColumn(x => x.Id)
                            .AddColumn(x => x.Description)
                            .AddColumn(x => x.ISBN)
                            .AddColumn(x => x.Price)
                            .BulkInsertOrUpdate()
                            .MatchTargetOn(x => x.ISBN)
                            .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                            .CommitAsync(conn);
                }

                trans.Complete();
            }
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsert_KeepIdentity()
        {
            const int rows = 30;
            var bulk = new BulkOperations();
            List<Book> bookList = new List<Book>();

            for (int i = 0; i < rows; i++)
            {
                bookList.Add(new Book
                {
                    Id = i + 600000,
                    Description = "Some desc",
                    Price = 450.23M,
                    ISBN = "1234567891234"
                });
            }
            var rec = bookList.Select(x => x.Id).Distinct().Count() == bookList.Count();

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForDeleteQuery()
                        .WithTable("Books")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<Book>()
                        .ForCollection(bookList)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls
                        })
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.IsTrue(_dataAccess.GetBookList().First().Id == 600000);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdate_KeepIdentity()
        {
            const int rows = 30;

            var bulk = new BulkOperations();
            List<Book> bookList = new List<Book>();

            for (int i = 0; i < rows; i++)
            {
                bookList.Add(new Book
                {
                    Id = i + 5000,
                    Description = "Some desc",
                    Price = 450.23M,
                    ISBN = "1234567891234"
                });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForDeleteQuery()
                        .WithTable("Books")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<Book>()
                        .ForCollection(bookList)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity
                        })
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.Id)
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);

                    // Do some updates
                    foreach (var book in bookList)
                    {
                        book.Description = "Updated Description";
                    }

                    await bulk.Setup<Book>()
                        .ForCollection(bookList)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity
                        })
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.Id)
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.IsTrue(_dataAccess.GetBookList().First().Id == 5000);
            Assert.IsTrue(_dataAccess.GetBookList().First().Description.Equals("Updated Description"));
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdateWithDeletePredicate_OnlyDeletesRecordsFromSpecifiedWarehouse()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            for (int i = 0; i < books.Count; i++)
            {
                if (i > books.Count / 2 - 1)
                {
                    books[i].WarehouseId = 1;
                }
                else
                {
                    books[i].WarehouseId = 2;
                }
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    books = _randomizer.GetRandomCollection(rows);

                    for (int i = 0; i < books.Count; i++)
                    {
                        if (i > books.Count / 2 - 1)
                        {
                            books[i].WarehouseId = 1;
                        }
                        else
                        {
                            books[i].WarehouseId = 2;
                        }
                    }

                    // Only delete if WarehouseId is 1
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .DeleteWhen(x => x.WarehouseId == 1)
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }

            // 15 were initially added with warehouse id 2, 15 more were added in second insert. 
            Assert.AreEqual(rows, _dataAccess.GetBookList().Count(x => x.WarehouseId == 2));

            // 15 were initially added with warehouse id 1. 15 were deleted in second call and 15 were inserted.  
            Assert.AreEqual((rows / 2), _dataAccess.GetBookList().Count(x => x.WarehouseId == 1));
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkUpdateWithPredicate_OnlyUpdateWhenWarehouseIs1()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            for (int i = 0; i < books.Count; i++)
            {
                if (i > books.Count / 2 - 1)
                {
                    books[i].WarehouseId = 1;
                }
                else
                {
                    books[i].WarehouseId = 2;
                }
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    for (int i = 0; i < books.Count; i++)
                    {
                        books[i].Price = 99999999;
                    }

                    // Only update if warehouse is 1
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id)
                        .UpdateWhen(x => x.WarehouseId == 1)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }

            Assert.AreEqual(99999999, _dataAccess.GetBookList().First(x => x.WarehouseId == 1).Price);
            Assert.AreNotEqual(99999999, _dataAccess.GetBookList().First(x => x.WarehouseId == 2).Price);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkUpdateWithPredicate_WhenBestSellerTrue()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            books[17].BestSeller = true;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    books[17].Price = 1234567;

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id)
                        .UpdateWhen(x => x.BestSeller == true)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            string isbn = books[17].ISBN;

            Assert.AreEqual(1234567, _dataAccess.GetBookList(isbn).First().Price);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkUpdateWithPredicate_WhenBestSellerFalse()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            foreach (var book in books)
                book.BestSeller = true;

            books[17].BestSeller = false;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    books[17].Price = 1234567;

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .SetIdentityColumn(x => x.Id)
                        .UpdateWhen(x => x.BestSeller == false)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }


            string isbn = books[17].ISBN;

            Assert.AreEqual(1234567, _dataAccess.GetBookList(isbn).First().Price);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkUpdateWithPredicate_OnlyUpdateWhenPriceLessThanOrEqualTo20()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            for (int i = 0; i < books.Count; i++)
            {
                books[i].Price = 21;
            }

            books[0].Price = 15;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    books[0].Price = 17;

                    // Only update if price less than or equal to 20
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .UpdateWhen(x => x.Price <= 20).CommitAsync(conn);
                }

                trans.Complete();

            }
            string isbn = books[0].ISBN;

            Assert.AreEqual(1, _dataAccess.GetBookList().Count(x => x.Price <= 20));
            Assert.AreEqual(17, _dataAccess.GetBookList(isbn).First().Price);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkUpdateWithPredicate_OnlyDeleteWhenWarehouseIs1()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            for (int i = 0; i < books.Count; i++)
            {
                if (i > books.Count / 2 - 1)
                {
                    books[i].WarehouseId = 1;
                }
                else
                {
                    books[i].WarehouseId = 2;
                }
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    // Only delete if warehouse is 1
                    await bulk.Setup()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .DeleteWhen(x => x.WarehouseId == 1)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.IsFalse(_dataAccess.GetBookList().Any(x => x.WarehouseId == 1));
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkDeleteWithMultiplePredicate_WhenDescriptionIsNullAndPriceMoreThan10()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());

            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            for (int i = 0; i < books.Count; i++)
            {
                if (i < 5)
                {
                    books[i].Description = null;
                    books[i].Price = 12;
                }
                else
                    books[i].Price = 30;

            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    // Only delete when price more than 10 and description is null
                    await bulk.Setup()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .DeleteWhen(x => x.Price > 10)
                        .DeleteWhen(x => x.Description == null)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }



            Assert.AreEqual(25, _dataAccess.GetBookList().Count);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkDeleteWithMultiplePredicate_WhenDescriptionIsNotNullAndPriceLessThan10()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            for (int i = 0; i < books.Count; i++)
            {
                if (i >= 10 && i < 15)
                {
                    books[i].Description = null;
                    books[i].Price = 5;
                }
                else
                    books[i].Price = 9;

            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    // Only delete when price more than 10 and description is null
                    await bulk.Setup()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .DeleteWhen(x => x.Price < 10)
                        .DeleteWhen(x => x.Description != null)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.AreEqual(5, _dataAccess.GetBookList().Count);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkUpdateWithPredicate_OnlyDeleteWhenPriceMoreThan50()
        {
            const int rows = 30;
            await BulkDelete(_dataAccess.GetBookList());
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(rows);

            for (int i = 0; i < books.Count; i++)
            {
                if (i != 23 && i != 5)
                    books[i].Price = 51;
                else
                {
                    books[i].Price = 32;
                }
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    // Only delete if price more than 50
                    await bulk.Setup()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .DeleteWhen(x => x.Price > 50)
                        .CommitAsync(conn); ;
                }

                trans.Complete();
            }

            Assert.AreEqual(2, _dataAccess.GetBookList().Count);
        }

        [TestMethod]
        public async Task SqlBulkTools_BulkInsertOrUpdate_TestDataTypes()
        {
            const int rows = 50;
            await BulkDelete(_dataAccess.GetBookList());

            var todaysDate = DateTime.Today;

            BulkOperations bulk = new BulkOperations();

            List<TestDataType> dataTypeTestLargerCol = new List<TestDataType>(); // Uses BulkCopy strategy

            for (int i = 0; i < rows; i++)
            {
                dataTypeTestLargerCol.Add(GetTestDataType(todaysDate));
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<TestDataType>()
                        .ForDeleteQuery()
                        .WithTable("TestDataTypes")
                        .Delete()
                        .AllRecords()
                        .CommitAsync(conn);

                    await bulk.Setup<TestDataType>()
                        .ForCollection(dataTypeTestLargerCol)
                        .WithTable("TestDataTypes")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.GuidTest)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            using (var conn = new SqlConnection(_connectionString))
            using (var command = new SqlCommand("SELECT TOP 1 * FROM [dbo].[TestDataTypes]", conn)
            {
                CommandType = CommandType.Text
            })
            {
                conn.Open();

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Assert.AreEqual(232.43F, reader["FloatTest"]);
                        Assert.AreEqual(43243.34, reader["FloatTest2"]);
                        Assert.AreEqual(178.43M, reader["DecimalTest"]);
                        Assert.AreEqual(24333.99M, reader["MoneyTest"]);
                        Assert.AreEqual(103.32M, reader["SmallMoneyTest"]);
                        Assert.AreEqual(32.53F, reader["RealTest"]);
                        Assert.AreEqual(154343.3434342M, reader["NumericTest"]);
                        Assert.AreEqual(todaysDate, reader["DateTimeTest"]);
                        Assert.AreEqual(new DateTime(2008, 12, 12, 10, 20, 30), reader["DateTime2Test"]);
                        Assert.AreEqual(new DateTime(2005, 7, 14), reader["SmallDateTimeTest"]);
                        Assert.AreEqual(new DateTime(2007, 7, 5), reader["DateTest"]);
                        Assert.AreEqual(new TimeSpan(23, 32, 23), reader["TimeTest"]);
                        Assert.AreEqual("This is some text.", reader["TextTest"]);
                        Assert.AreEqual("Some", reader["CharTest"].ToString().Trim());
                        Assert.AreEqual(126, (byte)reader["TinyIntTest"], "Testing TinyIntTest");
                        Assert.AreEqual(342324324324324324, reader["BigIntTest"]);
                        Assert.AreEqual("<title>The best SQL Bulk tool</title>", reader["XmlTest"]);
                        Assert.AreEqual("SomeText", reader["NCharTest"].ToString().Trim());
                        CollectionAssert.AreEqual(new byte[] { 3, 3, 32, 4 }, (byte[])reader["ImageTest"], "ImageTest");
                        CollectionAssert.AreEqual(new byte[] { 0, 3, 3, 2, 4, 3 }, (byte[])reader["BinaryTest"], "Testing BinaryTest");
                        CollectionAssert.AreEqual(new byte[] { 3, 23, 33, 243 }, (byte[])reader["VarBinaryTest"], "Testing VarBinaryTest");
                        Assert.IsNotNull(reader["TestSqlGeometry"]);
                        Assert.IsNotNull(reader["TestSqlGeography"]);
                    }
                }
            }
        }

        private TestDataType GetTestDataType(DateTime todaysDate)
        {
            var dataTypeTestRecord = new TestDataType()
            {
                BigIntTest = 342324324324324324,
                TinyIntTest = 126,
                DateTimeTest = todaysDate,
                DateTime2Test = new DateTime(2008, 12, 12, 10, 20, 30),
                DateTest = new DateTime(2007, 7, 5, 20, 30, 10),
                TimeTest = new TimeSpan(23, 32, 23),
                SmallDateTimeTest = new DateTime(2005, 7, 14),
                BinaryTest = new byte[] { 0, 3, 3, 2, 4, 3 },
                VarBinaryTest = new byte[] { 3, 23, 33, 243 },
                DecimalTest = 178.43M,
                MoneyTest = 24333.99M,
                SmallMoneyTest = 103.32M,
                RealTest = 32.53F,
                NumericTest = 154343.3434342M,
                FloatTest = 232.43F,
                FloatTest2 = 43243.34,
                TextTest = "This is some text.",
                GuidTest = Guid.NewGuid(),
                CharTest = "Some",
                XmlTest = "<title>The best SQL Bulk tool</title>",
                NCharTest = "SomeText",
                ImageTest = new byte[] { 3, 3, 32, 4 },
                TestSqlGeometry = SqlGeometry.Point(-2.74612, 53.881238, 4326),
                TestSqlGeography = SqlGeography.Point(-5, 43.432, 4326)
            };

            return dataTypeTestRecord;
        }

        private async Task<long> BulkInsert(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 5000
                        })
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.PublishDate)
                        .BulkInsert()
                        .TmpDisableAllNonClusteredIndexes()
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private async Task<long> BulkInsertAllColumns(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()

                        .ForCollection(col)
                        .WithTable("Books")
                        .WithBulkCopySettings(new BulkCopySettings()
                        {
                            BatchSize = 8000,
                            BulkCopyTimeout = 500
                        })
                        .AddAllColumns()
                        .BulkInsert()
                        .TmpDisableAllNonClusteredIndexes()
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private async Task<long> BulkInsertOrUpdate(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.PublishDate)
                        .BulkInsertOrUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private async Task<long> BulkInsertOrUpdateAllColumns(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();

            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsertOrUpdate()
                        .SetIdentityColumn(x => x.Id)
                        .MatchTargetOn(x => x.ISBN)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private async Task<long> BulkUpdate(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            var watch = Stopwatch.StartNew();
            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.PublishDate)
                        .BulkUpdate()
                        .MatchTargetOn(x => x.ISBN)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private async Task<long> BulkDelete(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();

            var watch = System.Diagnostics.Stopwatch.StartNew();
            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(col)
                        .WithTable("Books")
                        .AddColumn(x => x.ISBN)
                        .BulkDelete()
                        .MatchTargetOn(x => x.ISBN)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }
    }
}