using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Sample
{
	class Program
	{
		public const string Exchange = "exchange";
		public const string Queue = "queue";
		public const string RoutingKey = "routing-key";

		public static async Task Main()
		{
			var connectionFactory = new ConnectionFactory
			{
				Uri = new Uri("amqp://guest:guest@localhost:5672/%2f"),
				AutomaticRecoveryEnabled = true,
				//DispatchConsumersAsync = true,
				NetworkRecoveryInterval = TimeSpan.FromMilliseconds(300),
				TopologyRecoveryEnabled = true
			};

			using (var connection = connectionFactory.CreateConnection())
			{
				using (var channel = connection.CreateModel())
				{
					channel.ExchangeDeclare(Exchange, ExchangeType.Direct, durable: true, autoDelete: false);
					channel.QueueDeclare(Queue, durable: true, exclusive: false, autoDelete: false);
					channel.QueueBind(Queue, Exchange, RoutingKey);

					try
					{
						var threadCount = 10;
						var count = 1000;
						var messages = Enumerable.Range(1, count).ToLookup(i => i % threadCount, i => i.ToString());

						var publishStopwatch = Stopwatch.StartNew();
						var concurrentTasks = messages.Select(m => PushAsync(connection, m));
						await Task.WhenAll(concurrentTasks);
						publishStopwatch.Stop();

						var stat = channel.QueueDeclarePassive(Queue);
						var ips = count / publishStopwatch.Elapsed.TotalSeconds;
						Console.WriteLine(
							$" Sent messages: {stat.MessageCount}. Elapsed: {publishStopwatch.Elapsed}, IPS: {ips:F2}");

						var receiveStopwatch = Stopwatch.StartNew();
						var receivedMessages = await ReceiveAsync(connectionFactory, 100, count);
						receiveStopwatch.Stop();

						Console.WriteLine("Received");
						stat = channel.QueueDeclarePassive(Queue);
						Console.WriteLine("NeverCalled");

						ips = receivedMessages.Length / receiveStopwatch.Elapsed.TotalSeconds;
						Console.WriteLine(
							$"Received messages: {receivedMessages.Length}. Left: {stat.MessageCount}. Elapsed: {receiveStopwatch.Elapsed}, IPS: {ips:F2}");
					}
					finally
					{
						channel.QueueDelete(Queue);
						channel.ExchangeDelete(Exchange);
					}
				}
			}

			Console.WriteLine("Done.");
		}

		private static Task PushAsync(IConnection connection, IEnumerable<string> items)
		{
			return Task.Run(() =>
			{
				using (var channel = connection.CreateModel())
				{
					channel.ConfirmSelect();
					foreach (var item in items)
					{
						var data = Encoding.UTF8.GetBytes(item);
						var prop = channel.CreateBasicProperties();
						prop.MessageId = Guid.NewGuid().ToString();
						prop.Persistent = false; // true;
						channel.BasicPublish(Exchange, RoutingKey, true, prop, data);
						channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));
					}
				}
			});
		}

		private static async Task<string[]> ReceiveAsync(
			IConnectionFactory connectionFactory,
			int parallelCount,
			int expectedCount)
		{
			try
			{
				string[] result;
				using (var connection = connectionFactory.CreateConnection())
				{
					using (var channel = connection.CreateModel())
					{
						channel.BasicQos(0, (ushort) parallelCount, false);

						var received = new ConcurrentQueue<string>();
						var completed = new TaskCompletionSource<string[]>();

						var consumer = new EventingBasicConsumer(channel);
						consumer.Received += (s, e) =>
						{
							var body = Encoding.UTF8.GetString(e.Body.ToArray());
							received.Enqueue(body);
							channel.BasicAck(e.DeliveryTag, false);

							Console.WriteLine("Received:" + body);

							if (received.Count >= expectedCount)
							{
								completed.SetResult(received.ToArray());
							}
						};

						var tag = channel.BasicConsume(
							Queue,
							false,
							"",
							true,
							false,
							new Dictionary<string, object>(),
							consumer);

						result = await completed.Task;
						channel.BasicCancel(tag);

						Console.WriteLine("Before close channel");
					}

					Console.WriteLine("Before close connection");
				}

				Console.WriteLine("All closed - never called!");

				return result;
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex);
				throw;
			}
		}
	}
}