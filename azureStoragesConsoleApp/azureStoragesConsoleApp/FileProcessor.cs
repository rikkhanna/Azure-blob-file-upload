using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using static System.Console;


namespace azureStoragesConsoleApp
{
    internal class FileProcessor
    {
        private static readonly string CompletedDirectoryName = "complete";
        public string InputFilePath { get; }
        public FileProcessor(string filePath)
        {
            InputFilePath = filePath;
        }
        public async Task Process(CloudStorageAccount storageAccount)
        {
           WriteLine($"Begin process of {InputFilePath}");

            // Check if file exists
            if (!File.Exists(InputFilePath))
            {
                WriteLine($"ERROR: file {InputFilePath} does not exist.");
                return;
            }
            //getting root directory path 
            string rootDirectoryPath = new DirectoryInfo(InputFilePath).Parent.FullName;
            WriteLine("Root Directory path  " + rootDirectoryPath);
            // complete directory to move files after processing
            string completeDirPath = Path.Combine(rootDirectoryPath, CompletedDirectoryName);
            // create complete directory at path if didn't exists
            if (!Directory.Exists(completeDirPath))
            {
                WriteLine($"creating directory at {completeDirPath}");
                Directory.CreateDirectory(completeDirPath);
            }
            string inputFileName = Path.GetFileName(InputFilePath);
            string extension = Path.GetExtension(InputFilePath);
            string Completedfilename = $"{Path.GetFileNameWithoutExtension(InputFilePath)}-{Guid.NewGuid().ToString()}{extension}";
            string completedFilePath = Path.Combine(completeDirPath, Completedfilename);
            WriteLine($"uploading file {inputFileName} to blob");
            await WriteToBlobFromFileAsync(InputFilePath, storageAccount);
            // move a file to complete folder after processing
            File.Move(InputFilePath, completedFilePath);
            WriteLine("File upload complete!");
        }

        private async Task WriteToBlobFromFileAsync(string filePath, CloudStorageAccount storageAccount)
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
                    WriteLine("Successfully created container idacsblob");

                    var permissions = new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob };
                    await cloudBlobContainer.SetPermissionsAsync(permissions).ConfigureAwait(false);
                    WriteLine($"Permissions set");
                }
                string filename = Path.GetFileNameWithoutExtension(filePath);
                string extension = Path.GetExtension(filePath);
                await cloudBlobContainer.GetBlockBlobReference($"{filename}-{DateTime.UtcNow.ToString()}{extension}").UploadFromFileAsync(filePath);
            }
            catch (StorageException ex)
            {
                WriteLine("Error returned from the service: {0}", ex.Message);
            }

        }

    }
}
