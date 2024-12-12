using System;
using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Medlem_Nyhedsbrevsystem;

class Program
{
    static void Main(string[] args)
    {
        string rabbitMqHost = "localhost";

        // Start beskedmodtageren for at simulere Nyhedsbrev systemet
        var modtager = new BeskedModtager(rabbitMqHost, "MedlemTilNyhedsbrevQueue");
        modtager.StartLytning();

        // Simuler afsendelse af en besked fra Medlem systemet
        var sender = new BeskedSender(rabbitMqHost, "MedlemTilNyhedsbrevQueue");

        var medlemData = new {
            Id = 123,
            Navn = "Sebastian",
            Efternavn = "Tully",
            Email = "Sebastian@gmail.com",
            Adresse = "123 Main Street",
            Dato = "21.12.2023"
        };

        sender.SendBesked(medlemData);

        Console.WriteLine("Besked sendt fra Medlemssystem.");
        Console.ReadLine();
    }
}