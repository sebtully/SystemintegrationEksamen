using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;

namespace DetGamleBolsjekogeri;

public class BeskedModtager
{
    public void StartLytning()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare(queue: "medlem_tjek_anmodning", durable: false, exclusive: false, autoDelete: false, arguments: null);
        channel.QueueDeclare(queue: "medlem_tjek_svar", durable: false, exclusive: false, autoDelete: false, arguments: null);

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (model, ea) =>
        {
            var beskedBody = Encoding.UTF8.GetString(ea.Body.ToArray());
            var anmodning = JsonConvert.DeserializeObject<dynamic>(beskedBody);
            string email = anmodning.Email;

            Console.WriteLine($"[MedlemSystem] Modtaget anmodning om at tjekke email: {email}");
            
            var erMedlem = email == "Sebastian@gmail.com";

            var svarBesked = new { Email = email, ErMedlem = erMedlem, MedlemskabType = erMedlem ? "Premium" : "Ikke-Medlem" };
            var svarBody = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(svarBesked));

            var props = ea.BasicProperties;
            var svarProps = channel.CreateBasicProperties();
            svarProps.CorrelationId = props.CorrelationId;

            channel.BasicPublish(exchange: "", routingKey: props.ReplyTo, basicProperties: svarProps, body: svarBody);
            Console.WriteLine($"[MedlemSystem] Sendt svar for email: {email}");
        };

        channel.BasicConsume(queue: "medlem_tjek_anmodning", autoAck: true, consumer: consumer);
        Console.WriteLine("Lytter efter anmodninger...");
        Console.ReadLine();
    }
}