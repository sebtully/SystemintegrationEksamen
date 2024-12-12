using System;

namespace DetGamleBolsjekogeri;

class Program
{
    static async Task Main(string[] args)
    {
        string email = "Sebastian@gmail.com";

        var sender = new BeskedSender();
        var modtager = new BeskedModtager();
        
        var listenerTask = Task.Run(() => modtager.StartLytning());
        
        await sender.SendBesked(email);
        
        await listenerTask;
    }
}