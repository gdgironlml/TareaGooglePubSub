using Google.Cloud.PubSub.V1;
using Grpc.Core;

string projectId = "expanded-idiom-489914-h7"; // ID del proyecto en Google Cloud
string subscriptionId = "SuscripcionPruebaTareaGooglePubSub"; // ID de la suscripción, se usará para crear una ruta que Google entienda, pero el nombre real de la suscripción se generará dinámicamente en función del canal al que el usuario quiera suscribirse

Console.WriteLine("SUSCRIPTOR DE GOOGLE PUB/SUB\n");

string topicId = null;
SubscriptionName subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId); // Se crea una ruta para la suscripción, pero el nombre real de la suscripción se generará dinámicamente en función del canal al que el usuario quiera suscribirse, por lo que esta ruta se actualizará dentro del bucle

while (true)
{
    Console.WriteLine("Ingrese el nombre del canal (Topic) al que desea suscribirse:");
    topicId = Console.ReadLine(); // Lee el nombre del canal al que el usuario desea suscribirse. Este nombre se usará para verificar si el canal existe y para crear una suscripción a ese canal si no existe una suscripción previa. Si el usuario ingresa un nombre de canal vacío o solo espacios, se ignora y se solicita nuevamente el nombre del canal.

    if (string.IsNullOrWhiteSpace(topicId)) continue; // Si el nombre del canal está vacío o solo tiene espacios, se ignora y se solicita nuevamente el nombre del canal.
    
    subscriptionId = "Sub-Para-" + topicId; // Se genera un nombre de suscripción dinámico basado en el nombre del canal ingresado por el usuario. Esto permite que cada canal tenga su propia suscripción única.

    TopicName topicName = TopicName.FromProjectTopic(projectId, topicId); // Se crea la ruta para el canal, usando el ID del proyecto y el nombre del canal ingresado por el usuario. Esta ruta se usará para verificar si el canal existe y para crear una suscripción a ese canal si no existe una suscripción previa.
    subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId); // Se actualiza la ruta de la suscripción con el nuevo nombre de suscripción generado dinámicamente. Esto asegura que cada canal tenga su propia suscripción única, y que el programa pueda verificar si esa suscripción ya existe o si necesita crear una nueva para ese canal específico.

    try
    {
        PublisherServiceApiClient topicService = await PublisherServiceApiClient.CreateAsync(); // Se inicializa el cliente de publicación para interactuar con el servicio de Google Pub/Sub
        SubscriberServiceApiClient subscriberService = await SubscriberServiceApiClient.CreateAsync(); // Se inicializa el cliente de suscripción para interactuar con el servicio de Google Pub/Sub
        try
        {
            await subscriberService.GetSubscriptionAsync(subscriptionName); // Intenta obtener la suscripción para el canal especificado, si existe, se muestra un mensaje indicando que ya está conectado al canal.
            Console.WriteLine($"[OK] Conectado al canal: {topicId}.");
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound) // Si la suscripción no existe, se intenta obtener el canal especificado, si no existe, lanza una excepción que se captura en el bloque catch
        {
            Console.WriteLine($"Creando suscripción para {topicId}...");
            await subscriberService.CreateSubscriptionAsync(subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 60);
            Console.WriteLine($"Suscrito a {topicId}");
        }

        break;
    }
    catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound) // Si el canal no existe, se muestra un mensaje de error indicando que el canal no existe y se solicita al usuario que ingrese un nombre de canal válido. Esto asegura que el programa solo intente suscribirse a canales que realmente existen en Google Cloud, y proporciona una retroalimentación clara al usuario en caso de errores.
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"\n[ERROR]: El canal '{topicId}' no existe en Google Cloud.");
        Console.WriteLine("Por favor, asegúrese de que el Publicador lo haya creado primero.\n");
        Console.ResetColor();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\nERROR: {ex.Message}\n"); // Si ocurre cualquier otro error (como problemas de conexión), se muestra el mensaje de error y se solicita al usuario que ingrese el nombre del canal nuevamente.
    }
}
Console.WriteLine("\nSuscrito... Esperando mensajes... (Presiona Ctrl+C para detener)");
SubscriberClient subscriber = await SubscriberClient.CreateAsync(subscriptionName); // Se inicializa el cliente de suscripción para la suscripción especificada, que se usará para recibir mensajes de ese canal específico. El programa luego entra en un bucle de espera para recibir mensajes, y cada vez que se recibe un mensaje, se muestra su contenido en la consola.

await subscriber.StartAsync(async (PubsubMessage message, CancellationToken cancel) => // Se inicia la recepción de mensajes de la suscripción, y se define una función asíncrona que se ejecuta cada vez que se recibe un mensaje. Esta función toma el mensaje recibido y un token de cancelación como parámetros, y devuelve una respuesta indicando si el mensaje fue procesado correctamente (Ack) o si hubo un error (Nack).
{
    string texto = message.Data.ToStringUtf8(); // Se convierte el contenido del mensaje de bytes a una cadena de texto utilizando UTF-8. Esto permite que el programa muestre el contenido del mensaje de manera legible en la consola.
    Console.WriteLine($"\n>> Mensaje recibido: {texto}");
    return SubscriberClient.Reply.Ack; // Se devuelve una respuesta de "Ack" para indicar que el mensaje fue procesado correctamente. Esto le dice a Google Pub/Sub que el mensaje puede ser eliminado de la cola de mensajes pendientes, ya que el programa ha confirmado que lo ha recibido y procesado sin errores.
});

await Task.Delay(-1); // El programa se mantiene en ejecución indefinidamente para seguir recibiendo mensajes. Esto asegura que el suscriptor siga activo y pueda recibir mensajes en cualquier momento, hasta que el usuario decida detenerlo manualmente (por ejemplo, presionando Ctrl+C).