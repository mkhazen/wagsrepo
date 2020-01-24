// Databricks notebook source
    import scala.collection.JavaConverters._
    import com.microsoft.azure.eventhubs._
    import java.util.concurrent._
    import scala.collection.immutable._
    import scala.concurrent.Future
    import scala.concurrent.ExecutionContext.Implicits.global

    val namespaceName = "IDIincoming"
    val eventHubName = "evetincoming"
    val sasKeyName = "rw"
    val sasKey = "KLrDLpiZ90K7MKRr07CBV15r5Lpdpq5C1a3BQg6iiZ4="
    val connStr = new ConnectionStringBuilder()
                .setNamespaceName(namespaceName)
                .setEventHubName(eventHubName)
                .setSasKeyName(sasKeyName)
                .setSasKey(sasKey)


// COMMAND ----------

