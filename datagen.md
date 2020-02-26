# Data generator for IDI

## Goal

create a source code to create fake data point and send to event hub

## Project

Create a project as console project with C# as the language

Add nuget packages Microsoft.Azure.EventHubs

add includes

```
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Amqp.Framing;
```

Now the whole class with details

```
namespace ididatagen
{
    class Program
    {
        private static EventHubClient eventHubClient;
        private const string EventHubConnectionString = "Endpoint=sb://eventnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xxxxxxxxxxxxxxxxxxxxxxxxx";
        private const string EventHubName = "eventhubname";

        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
            //Console.WriteLine("Hello World!");
        }

        private static async Task MainAsync(string[] args)
        {
            // Creates an EventHubsConnectionStringBuilder object from the connection string, and sets the EntityPath.
            // Typically, the connection string should have the entity path in it, but this simple scenario
            // uses the connection string from the namespace.
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            await SendMessagesToEventHub(100, 0i);

            await eventHubClient.CloseAsync();

            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();
        }

        // Uses the event hub client to send 100 messages to the event hub.
        private static async Task SendMessagesToEventHub(int numMessagesToSend, int startfrom = 0)
        {
            for (var i = startfrom; i < startfrom + numMessagesToSend; i++)
            {
                try
                {
                    var message = $"Message {i}";

                    ididata datagen = new ididata();
                    datagen.eventdatetime = DateTime.Now;
                    datagen.customername = "idicustomer " + i.ToString();
                    datagen.dob = "02/22/1990";
                    datagen.address = "1435 Lake cook rd";
                    datagen.city = "deerfield";
                    datagen.state = "IL";
                    datagen.zip = "60015";
                    datagen.country = "US";

                    string jsonString;
                    jsonString = JsonSerializer.Serialize(datagen);

                    Console.WriteLine($"Sending message: {message}");
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(jsonString)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(10);
            }

            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }

    }

    public class ididata
    {
        public DateTime eventdatetime { get; set; }
        public int precvalue { get; set; }
        public string customername { get; set; }
        public string dob { get; set; }
        public string address { get; set; }
        public string address1 { get; set; }
        public string city { get; set; }
        public string state { get; set; }
        public string zip { get; set; }
        public string country { get; set; }


    }

}
```