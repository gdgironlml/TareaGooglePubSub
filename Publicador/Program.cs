using Google.Cloud.PubSub.V1;
using Grpc.Core;

Console.WriteLine("PUBLICADOR DE GOOGLE PUB/SUB (MODO CHAT)\n");

string projectId = "expanded-idiom-489914-h7"; // ID del proyecto en Google Cloud
string topicId = ""; // Este será el tema o canal que crearemos

Console.WriteLine("Ingrese el nombre del canal al que desea enviar mensajes: "); 
topicId = Console.ReadLine();

TopicName topicName = TopicName.FromProjectTopic(projectId, topicId); // Es una funcion que combina el Id y el tema o canal para crear una ruta que Google entienda
PublisherServiceApiClient publisherService = await PublisherServiceApiClient.CreateAsync(); // Se inicializa el cliente de publicacion para interactuar con el servicio de Google Pub/Sub, que se usará para verificar si el canal existe y para crear uno nuevo si no existe

try
{
    await publisherService.GetTopicAsync(topicName); // intenta obtener el canal especificado, si no existe, lanza una excepcion que se captura en el bloque catch
    Console.WriteLine($"Conectado al canal: {topicId}");
}
catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
{
    await publisherService.CreateTopicAsync(topicName); // Si el canal no existe, se crea uno nuevo con el nombre especificado por el usuario
    Console.WriteLine($"Canal '{topicId}' creado.");
}

PublisherClient publisher = await PublisherClient.CreateAsync(topicName); // Se inicializa el cliente de publicación para el canal especificado, que se usará para enviar mensajes a ese canal

Console.WriteLine("\nEscriba su mensaje y presione Enter para enviar:");
Console.WriteLine("(Presione Ctrl + C para salir)\n");

while (true)
{
    Console.Write("> ");
    string mensajeTexto = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(mensajeTexto)) continue; // Si el mensaje está vacío o solo tiene espacios, se ignora y se solicita otro mensaje

    try
    {
        string messageId = await publisher.PublishAsync(mensajeTexto); // Intenta publicar el mensaje en el canal, si hay un error (como problemas de conexión), se captura en el bloque catch
    }
    catch (Exception ex)
    {
        Console.WriteLine($"ERROR: {ex.Message}\nEscribe el mensaje de nuevo: "); // Si ocurre un error al publicar, se muestra el mensaje de error y se solicita al usuario que escriba el mensaje nuevamente
    }
}