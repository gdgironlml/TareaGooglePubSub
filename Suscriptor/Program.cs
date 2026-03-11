using Google.Cloud.PubSub.V1;
using Grpc.Core;

string projectId = "expanded-idiom-489914-h7";
string subscriptionId = "SuscripcionPruebaTareaGooglePubSub";

Console.WriteLine("SUSCRIPTOR DE GOOGLE PUB/SUB\n");

string topicId = null;
SubscriptionName subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);

while (true)
{
    Console.WriteLine("Ingrese el nombre del canal (Topic) al que desea suscribirse:");
    topicId = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(topicId)) continue;

    subscriptionId = "Sub-Para-" + topicId;

    TopicName topicName = TopicName.FromProjectTopic(projectId, topicId);
    subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);

    try
    {
        PublisherServiceApiClient topicService = await PublisherServiceApiClient.CreateAsync();
        await topicService.GetTopicAsync(topicName);

        SubscriberServiceApiClient subscriberService = await SubscriberServiceApiClient.CreateAsync();
        try
        {
            await subscriberService.GetSubscriptionAsync(subscriptionName);
            Console.WriteLine($"[OK] Conectado al canal: {topicId}.");
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            Console.WriteLine($"Creando suscripción para {topicId}...");
            await subscriberService.CreateSubscriptionAsync(subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 60);
            Console.WriteLine($"Suscrito a {topicId}");
        }

        break;
    }
    catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"\n[ERROR]: El canal '{topicId}' no existe en Google Cloud.");
        Console.WriteLine("Por favor, asegúrese de que el Publicador lo haya creado primero.\n");
        Console.ResetColor();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\nERROR: {ex.Message}\n");
    }
}
Console.WriteLine("\nSuscrito... Esperando mensajes... (Presiona Ctrl+C para detener)");
SubscriberClient subscriber = await SubscriberClient.CreateAsync(subscriptionName);

await subscriber.StartAsync(async (PubsubMessage message, CancellationToken cancel) =>
{
    string texto = message.Data.ToStringUtf8();
    Console.WriteLine($"\n>> Mensaje recibido: {texto}");
    return SubscriberClient.Reply.Ack;
});

await Task.Delay(-1);