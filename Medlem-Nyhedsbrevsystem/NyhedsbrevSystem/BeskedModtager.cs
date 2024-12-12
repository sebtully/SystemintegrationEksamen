using System.Globalization;
using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Medlem_Nyhedsbrevsystem;

class BeskedModtager
{
    private readonly string _hostName;
    private readonly string _queueName;

    public BeskedModtager(string hostName, string queueName)
    {
        _hostName = hostName;
        _queueName = queueName;
    }

    public void StartLytning()
    {
        var factory = new ConnectionFactory() { HostName = _hostName };
        var connection = factory.CreateConnection();
        var channel = connection.CreateModel();

        channel.QueueDeclare(queue: _queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            Console.WriteLine("Modtaget rå besked: {0}", message);

            // Udfør oversættelse
            var medlemData = JsonConvert.DeserializeObject<Medlem>(message);
            var nyhedsbrevData = new NyhedsbrevData
            {
                FuldeNavn = $"{medlemData.Navn} {medlemData.Efternavn}",
                Email = medlemData.Email,
                Dato = DateTime.ParseExact(medlemData.Dato, "dd.MM.yyyy", CultureInfo.InvariantCulture).ToString("MM/dd/yyyy")
            };

            Console.WriteLine("Oversat til Nyhedsbrev format: {0}", JsonConvert.SerializeObject(nyhedsbrevData));
        };

        channel.BasicConsume(queue: _queueName, autoAck: true, consumer: consumer);
        Console.WriteLine("Lytter efter beskeder...");
    }
}