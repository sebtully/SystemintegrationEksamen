using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;

namespace DetGamleBolsjekogeri;

public class BeskedSender
{
    public async Task SendBesked(string email)
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare(queue: "medlem_tjek_anmodning", durable: false, exclusive: false, autoDelete: false, arguments: null);
        channel.QueueDeclare(queue: "medlem_tjek_svar", durable: false, exclusive: false, autoDelete: false, arguments: null);

        var korrelationsId = Guid.NewGuid().ToString();
        var props = channel.CreateBasicProperties();
        props.CorrelationId = korrelationsId;
        props.ReplyTo = "medlem_tjek_svar";

        var besked = new { Email = email };
        var beskedBody = JsonConvert.SerializeObject(besked);
        var body = Encoding.UTF8.GetBytes(beskedBody);

        // Send anmodning
        channel.BasicPublish(exchange: "", routingKey: "medlem_tjek_anmodning", basicProperties: props, body: body);
        Console.WriteLine($"[AktivitetSystem] Sendt anmodning om at tjekke email: {email}");

        // TaskCompletionSource til at vente på svar
        var tcs = new TaskCompletionSource<string>();

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (model, ea) =>
        {
            if (ea.BasicProperties.CorrelationId == korrelationsId)
            {
                var svar = Encoding.UTF8.GetString(ea.Body.ToArray());
                Console.WriteLine($"[AktivitetSystem] Modtaget svar: {svar}");

                // Fuldfører tasken med svaret
                tcs.SetResult(svar);
            }
        };

        channel.BasicConsume(queue: "medlem_tjek_svar", autoAck: true, consumer: consumer);

        Console.WriteLine("Venter på svar...");
        var svarBesked = await tcs.Task;  // Venter på svar
        Console.WriteLine($"[AktivitetSystem] Færdig: {svarBesked}");
    }
}
