using Google.Cloud.PubSub.V1;
using Grpc.Core;

Console.WriteLine("PUBLICADOR DE GOOGLE PUB/SUB (MODO CHAT)\n");

string projectId = "expanded-idiom-489914-h7";
string topicId = "";

Console.WriteLine("Ingrese el nombre del canal al que desea enviar mensajes: ");
topicId = Console.ReadLine();

TopicName topicName = TopicName.FromProjectTopic(projectId, topicId);
PublisherServiceApiClient publisherService = await PublisherServiceApiClient.CreateAsync();

try
{
    await publisherService.GetTopicAsync(topicName);
    Console.WriteLine($"Conectado al canal: {topicId}");
}
catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
{
    await publisherService.CreateTopicAsync(topicName);
    Console.WriteLine($"Canal '{topicId}' creado.");
}

PublisherClient publisher = await PublisherClient.CreateAsync(topicName);

Console.WriteLine("\nEscriba su mensaje y presione Enter para enviar:");
Console.WriteLine("(Presione Ctrl + C para salir)\n");

while (true)
{
    Console.Write("> ");
    string mensajeTexto = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(mensajeTexto)) continue;

    try
    {
        string messageId = await publisher.PublishAsync(mensajeTexto);
    }
    catch (Exception ex)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"[ERROR] No se pudo enviar el mensaje: {ex.Message}");
        Console.ResetColor();
    }
}