SELECT TOP (1000) [id]
      ,[Avgconfidence]
      ,[label]
      ,[EventProcessedUtcTime]
      ,[PartitionId]
      ,[EventEnqueuedUtcTime]
      ,[MessageId]
      ,[CorrelationId]
      ,[ConnectionDeviceId]
      ,[ConnectionDeviceGenerationId]
      ,[EnqueuedTime]
      ,[count]
      ,[inserttime]
  FROM [dbo].[visionkitcount] where ConnectionDeviceId = 'aicam01'