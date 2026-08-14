/*==============================================================================
(C) Copyright 2026 John J Kauflin, All rights reserved.
--------------------------------------------------------------------------------
DESCRIPTION:  Class to run scheduled jobs via Azure Function TimerTrigger for
                the web application
                
--------------------------------------------------------------------------------
Modification History
2025-04-30 JJK  Initial version
2025-05-03 JJK  Fixed error in dynamic object value get
2026-08-14 JJK  Moving this to jjkWebAsyncFunctions project to run as a 
                separate Azure Function App
                
================================================================================*/
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;

namespace jjkWebAsyncFunctions
{
    public class ScheduledJobs
    {
        private readonly ILogger _logger;
        private readonly string? apiCosmosDbConnStr;
        private readonly string databaseId;

        public ScheduledJobs (ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<ScheduledJobs>();
            apiCosmosDbConnStr = Environment.GetEnvironmentVariable("API_COSMOS_DB_CONN_STR");
            databaseId = "jjkdb1";
        }

        [Function("PurgeDatabase")]
        //[TimerTrigger("0 0 * * * *")] // Runs every hour
        //[TimerTrigger("0 0 0 * * *")] // Runs every day at midnight UT
        public async Task Run([TimerTrigger("0 0 0 * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"PurgeDatabase Timer trigger function executed at: {DateTime.Now}");
            
            if (myTimer.ScheduleStatus is not null)
            {
                _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            }

            await DeleteItems("GenvMetricPoint",5);
            await DeleteItems("GenvImage",5);
            await DeleteItems("MetricPoint",90);
        }

        private async Task DeleteItems(string containerId, int daysToKeep) {
            DateTime currDateTime = DateTime.Now;
            int maxYearMonthDay = int.Parse(currDateTime.AddDays(-daysToKeep).ToString("yyyyMMdd"));
            int maxPurgeCnt = 5000;
            _logger.LogInformation($"Purging {containerId}, # days to keep = {daysToKeep}, maxYearMonthDay = {maxYearMonthDay}, maxPurgeCnt = {maxPurgeCnt}");
            var queryDefinition = new QueryDefinition(
                "SELECT c.id, c.PointDay FROM c WHERE c.PointDay < @maxYearMonthDay")
                .WithParameter("@maxYearMonthDay", maxYearMonthDay);
            //var queryRequestOptions = new QueryRequestOptions { MaxItemCount = maxPurgeCnt };

            CosmosClient cosmosClient = new CosmosClient(apiCosmosDbConnStr); 
            Database db = cosmosClient.GetDatabase(databaseId);
            Container container = db.GetContainer(containerId);

            int cnt = 0;
            string id = "";
            long partitionKey;
            var feed = container.GetItemQueryIterator<dynamic>(queryDefinition);
            //    requestOptions: queryRequestOptions
            //);

            while (feed.HasMoreResults)
            {
                var response = await feed.ReadNextAsync();
                foreach (var item in response)
                {
                    id = item.id.ToString();
                    partitionKey = item.PointDay.Value;
                    //_logger.LogInformation($"{cnt}, id: {item.id}, PointDay: {item.PointDay.Value} ");
                    if (cnt < maxPurgeCnt)
                    {
                        await container.DeleteItemAsync<object>(id, new PartitionKey(partitionKey));
                        cnt++;
                    }
                }
            }
            _logger.LogInformation($"{containerId}, Purged cnt = {cnt}");
        }

    }
}
