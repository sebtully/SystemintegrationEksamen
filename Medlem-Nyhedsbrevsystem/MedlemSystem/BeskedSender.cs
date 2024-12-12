using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace Medlem_Nyhedsbrevsystem;

class BeskedSender
{
    private readonly string _hostName;
    private readonly string _queueName;

    public BeskedSender(string hostName, string queueName)
    {
        _hostName = hostName;
        _queueName = queueName;
    }

    public void SendBesked(object message)
    {
        var factory = new ConnectionFactory() { HostName = _hostName };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare(queue: _queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

        var messageBody = JsonConvert.SerializeObject(message);
        var body = Encoding.UTF8.GetBytes(messageBody);

        channel.BasicPublish(exchange: "", routingKey: _queueName, basicProperties: null, body: body);
        Console.WriteLine("Besked sendt: {0}", messageBody);
    }
}