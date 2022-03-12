using Azure.Storage.Blobs;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;

namespace cosmos_changefeed_function
{
    public static class ConsumeChangeFeed
    {
        [FunctionName("ConsumeChangeFeed")]
        public static void Run([CosmosDBTrigger(
            databaseName: "items-db",
            collectionName: "item-price-container",
            ConnectionStringSetting = "cosmos-connection-string",
            LeaseCollectionName = "leases", 
            CreateLeaseCollectionIfNotExists = true, 
            StartFromBeginning = true)]IReadOnlyList<Document> input, ILogger log)
        {
            if (input != null && input.Count > 0)
            {
                log.LogInformation("Documents modified " + input.Count);                
                UploadDataAsync(input);
            }
        }

        /// <summary>
        /// Upload the documents as received into blob storage
        /// </summary>
        /// <param name="inputDocuments"></param>
        private static async void UploadDataAsync(IReadOnlyList<Document> inputDocuments)
        {
            BlobContainerClient container = new BlobContainerClient
                (Environment.GetEnvironmentVariable("CloudStorageAccount"), "itemscontainer");
            await container.CreateIfNotExistsAsync();
            using (var memoryStream = new MemoryStream())
            {
                foreach (var document in inputDocuments)
                {
                    LoadStreamWithJson(memoryStream, document);
                    await container.UploadBlobAsync(document.Id, memoryStream);
                }               
            }           
        }

        /// <summary>
        /// MemoryStream
        /// </summary>
        /// <param name="ms"></param>
        /// <param name="obj"></param>
        private static void LoadStreamWithJson(Stream ms, object obj)
        {
            StreamWriter writer = new StreamWriter(ms);
            writer.Write(obj);
            writer.Flush();
            ms.Position = 0;
        }
    }
}
