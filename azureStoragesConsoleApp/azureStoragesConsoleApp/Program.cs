using CsvHelper;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace azureStoragesConsoleApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            
            var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            IConfigurationRoot Configuration = builder.Build();
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                Configuration.GetSection("connectionString").Value);

            string connectionString = Configuration.GetSection("sqlConnectionString").Value;


            

            /* Reading file at given path using command line arguments */
         
            //var command = args[0];

            //if (command == "--file")
            //{
            //    var filePath = args[1];
            //    Console.WriteLine($"Single file {filePath} selected");
            //    await ProcessSingleFile(filePath,storageAccount);
            //}

            /* Reading Directory at given path using command line arguments */

            //  var command = args[0];

              //if (command == "--dir")
              //{
              //    var directoryPath = args[1];
              //    var fileType = args[2];
              //    Console.WriteLine($"Directory {directoryPath} selected");
              //    await ProcessDirectory(directoryPath, fileType, storageAccount);
              //}

              

        
            // SYNCING API DATA TO JSON FILE THEN UPLOADING TO BLOB
          /*  while (true)
            {

                await SyncApiToJsonToBlob(storageAccount);
                Console.WriteLine("\n Next Iteration After 1hr");
                Thread.Sleep(3600000);

            }
            */

            



            //Console.ReadKey();

            //await SyncSqlToJsonFileToBlob(connectionString, storageAccount);

            //await WriteToBlobFromFileAsync(@"C:\Users\Rishabh\source\repos\azureStoragesConsoleApp\azureStoragesConsoleApp\orders.csv", storageAccount);

            //await SyncToBlob(connectionString, storageAccount);

            /*
            while (true)
               {
                
                await SyncSqlToJsonFileToBlob(connectionString, storageAccount);
                Console.WriteLine("\n Next Iteration After 10sec");
                Thread.Sleep(10000);
              
               }
            */ 

            ///* BLOB INSERT */
            //int i = 5;
            //while (i != 0)
            //{
            //   // await WriteBlockBlobAsync(i, "Hello " + i,storageAccount);
            //    i--;
            //}
            ///* BLOB INSERT */



            /* TABLE INSERT AND RETRIEVE  */
            /* 
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("customers");
            await table.CreateIfNotExistsAsync();
            var user = new Customer("Rob", "Rob@localhost.local");
            CreateUser(table, user);
            await GetAllCustomers(table);
            */
            /* TABLE INSERT AND RETRIEVE  */



            ///* QUEUE INSERT AND PEEK */

            //await InsertToQueueAsync("task1", storageAccount);

            //await PeekQueueAsync(storageAccount);

            /* QUEUE INSERT AND PEEK */



        }

        /* CreateUser() for inserting value in table */
        static void CreateUser(CloudTable table, Customer user)
        {
            TableOperation insert = TableOperation.Insert(user);

            table.ExecuteAsync(insert);
        }

        /* GetAllCustomers() for fetching all records from table */
        static async Task GetAllCustomers(CloudTable table)
        {
            TableQuery<Customer> query = new TableQuery<Customer>()
                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "US"));
            var customers = await table.ExecuteQuerySegmentedAsync(query, null);
            foreach (Customer customer in customers)
            {
                Console.WriteLine(customer.Name);
            }
        }

        /* writeBlockBlobAsync() for inserting direct message to blob */
        static async Task WriteBlockBlobAsync(int id, string messageString, CloudStorageAccount storageAccount)
        {
           
            CloudBlobContainer cloudBlobContainer = null;
            try
            {
                // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();
                // Create a container called 'dataLogger-messages' and append a GUID value to it to make the name unique. 
                cloudBlobContainer = cloudBlobClient.GetContainerReference("idacsblob");

                bool wasCreated = await cloudBlobContainer.CreateIfNotExistsAsync();
                if (wasCreated)
                {
                    Console.WriteLine("Successfully created container idacsblob");

                    var permissions = new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob };
                    await cloudBlobContainer.SetPermissionsAsync(permissions).ConfigureAwait(false);
                    Console.WriteLine($"Permissions set");
                }

                await cloudBlobContainer.GetBlockBlobReference(id.ToString()).UploadTextAsync(messageString);



            }
            catch (StorageException ex)
            {
                Console.WriteLine("Error returned from the service: {0}", ex.Message);
            }

        }

        /* InsertToQueueAsync() to insert message/task into the queue */
        static async Task InsertToQueueAsync(string task, CloudStorageAccount storageAccount)
        {
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            CloudQueue queue = queueClient.GetQueueReference("tasks");

            await queue.CreateIfNotExistsAsync();

            CloudQueueMessage message = new CloudQueueMessage(task);
            var time = new TimeSpan(24, 0, 0);
            await queue.AddMessageAsync(message, time, null, null, null);

            
        }

        /* PeekQueueAsync() to read messages/tasks from the queue without dequeue them */
        static async Task PeekQueueAsync( CloudStorageAccount storageAccount)
        {
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            CloudQueue queue = queueClient.GetQueueReference("tasks");
            CloudQueueMessage message = await queue.PeekMessageAsync();
            Console.WriteLine($"Queue {message.AsString}");
        }

        public static async Task SyncToBlob(string connectionString, CloudStorageAccount storageAccount)
        {

            string filePath = @"C:\Users\Rishabh\source\repos\azureStoragesConsoleApp\azureStoragesConsoleApp\blobfile.txt";

            SqlDataReader rdr = null;
           
            try
            {
                
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    conn.Open();

                    string lastId = File.ReadAllText(@"C:\Users\Rishabh\source\repos\azureStoragesConsoleApp\azureStoragesConsoleApp\log.txt");
                    if (string.Empty != lastId)
                    {
                        await File.WriteAllTextAsync(filePath, "");
                        Console.WriteLine("Calling different procedure now with id "+lastId);
                    }
                    else
                    {
                        SqlCommand cmd = new SqlCommand(
                        "[dbo].[SelectAllProducts]", conn);

                        rdr = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection);
                        int lastRecordId = 0;

                        while (rdr.Read())
                        {
                            int id = 0;

                            id = Convert.ToInt32(rdr["brand_id"]);
                            lastRecordId = id;
                            string data = "{ \"id\": " + rdr["brand_id"].ToString() + "" +
                                           ", \"Brand Name \": " + rdr["brand_name"].ToString() + "" +
                                            " }";
                            Console.WriteLine($"data: {data}");

                            List<string> records = File.ReadAllLines(filePath).ToList();
                            records.Add(data);
                            await File.WriteAllLinesAsync(filePath, records);
                            //await WriteBlockBlobAsync(id, data, storageAccount);
                            await WriteToBlobFromFileAsync(filePath, storageAccount);
                        }
                        if (!rdr.Read())
                        {
                            File.WriteAllText(@"C:\Users\Rishabh\source\repos\azureStoragesConsoleApp\azureStoragesConsoleApp\log.txt", lastRecordId.ToString());
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message}");
            }


        }

        public static async Task SyncSqlToJsonFileToBlob(string connectionString, CloudStorageAccount storageAccount)
        {
            try
            {
                string filePath = @"C:\Users\Rishabh\source\repos\azureStoragesConsoleApp\azureStoragesConsoleApp\data.json";       
                // Check if file exists
                if (File.Exists(filePath))
                {
                    await File.WriteAllTextAsync(filePath, " ");
                }
                SqlDataReader rdr = null;
                string json = string.Empty;
                List<object> records = new List<object>();
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand("[dbo].[GetAllFromMesOpsMaster]", conn);
                    rdr = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection);
                    while (rdr.Read())
                    { 
                        IDictionary<string, object> record = new Dictionary<string, object>();
                        for (int i = 0; i < rdr.FieldCount; i++)
                        {
                            record.Add(rdr.GetName(i), rdr[i]);
                        }
                        records.Add(record);   
                    }
                }
                json = JsonConvert.SerializeObject(records);
                Console.WriteLine(json);
                //writing to filePath
                using (StreamWriter sw = new StreamWriter(filePath))
                {
                    Console.WriteLine($"Writing Sql to Json at {filePath}");
                    sw.Write(json);
                }
                await WriteToBlobFromFileAsync(filePath, storageAccount);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message}");
            }
        }

        static async Task WriteToBlobFromFileAsync(string filePath, CloudStorageAccount storageAccount)
        {

            CloudBlobContainer cloudBlobContainer = null;
            try
            {
                // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();
                // Create a container called 'dataLogger-messages' and append a GUID value to it to make the name unique. 
                cloudBlobContainer = cloudBlobClient.GetContainerReference("idacsblob");

                bool wasCreated = await cloudBlobContainer.CreateIfNotExistsAsync();
                if (wasCreated)
                {
                    Console.WriteLine("Successfully created container idacsblob");

                    var permissions = new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob };
                    await cloudBlobContainer.SetPermissionsAsync(permissions).ConfigureAwait(false);
                    Console.WriteLine($"Permissions set");
                }
                string filename = Path.GetFileNameWithoutExtension(filePath);
                string extension = Path.GetExtension(filePath);
                Console.WriteLine($"Uploading Json to blob from {filePath}");
                await cloudBlobContainer.GetBlockBlobReference($"{filename}-{DateTime.Now.ToString()}{extension}").UploadFromFileAsync(filePath);



            }
            catch (StorageException ex)
            {
                Console.WriteLine("Error returned from the service: {0}", ex.Message);
            }

        }

        private static async Task ProcessSingleFile(string filePath , CloudStorageAccount storageAccount)
        {
            var fileProcessor = new FileProcessor(filePath);
           await fileProcessor.Process(storageAccount);
        }

        //  Access Files from a directory
        private static async Task ProcessDirectory(string directoryPath, string fileType, CloudStorageAccount storageAccount)
        {
            switch (fileType)
            {
                case "txt":
                    string[] files = Directory.GetFiles(directoryPath, ".txt");
                    foreach(var filePath in files)
                    {
                        var fileProcessor = new FileProcessor(filePath);
                        await fileProcessor.Process(storageAccount);
                    }
                    break;
                case "csv":
                    string[] csvFiles = Directory.GetFiles(directoryPath, ".csv");
                    foreach (var filePath in csvFiles)
                    {
                        var fileProcessor = new FileProcessor(filePath);
                        await fileProcessor.Process(storageAccount);
                    }
                    break;
                case "json":
                    string[] jsonFiles = Directory.GetFiles(directoryPath, ".json");
                    foreach (var filePath in jsonFiles)
                    {
                        var fileProcessor = new FileProcessor(filePath);
                        await fileProcessor.Process(storageAccount);
                    }
                    break;
                default:
                    Console.WriteLine($"Error {fileType} is not supported!");
                    break;
            }
        }


        private static async Task SyncApiToJsonToBlob(CloudStorageAccount storageAccount)
        {
            string path = Directory.GetCurrentDirectory();
            string rootDirectoryPath = new DirectoryInfo(path).Parent.Parent.Parent.FullName;
            string rootFilePath = Path.Combine(rootDirectoryPath, "ApiToJson.json");
            if (!File.Exists(rootFilePath))
            {
                File.Create(rootFilePath);
            }
            HttpWebRequest WebReq = (HttpWebRequest)WebRequest.Create(string.Format("https://jsonplaceholder.typicode.com/posts"));
            WebReq.Method = "GET";
            HttpWebResponse WebResp = (HttpWebResponse)WebReq.GetResponse();
            Console.WriteLine(WebResp.StatusCode);
            Console.WriteLine(WebResp.Server);
            string jsonString;
            using (Stream stream = WebResp.GetResponseStream())   //modified from your code since the using statement disposes the stream automatically when done
            {
                StreamReader reader = new StreamReader(stream, System.Text.Encoding.UTF8);
                jsonString = reader.ReadToEnd();
            }
            //writing to filePath
            using (StreamWriter sw = new StreamWriter(rootFilePath))
            {
                sw.Write(jsonString);
            }
            await WriteToBlobFromFileAsync(rootFilePath, storageAccount);

            Console.WriteLine("Api to Json To Blob upload complete!");
        }

    }

}
