-module(amanda).
-author("Alyx Wolcott <contact@alyxw.me>").
-include("amqp_client.hrl").
-compile([export_all]).
-export([connect/2]).

connect(Host, Port) ->
	{ok, Channel, Queue} = start_amqp(),
	ok = setup_amqp_consumer(Channel, Queue),
	{ok, Sock} = gen_tcp:connect(Host, Port, [{packet, line}]),
	loop(Sock, Channel, Queue).

loop(Sock, Channel, Queue) ->
	receive
		{tcp, Sock, Data} ->
			send_amqp_message(Channel, <<"raw.in">>, Data),
			loop(Sock, Channel, Queue);
		{#'basic.deliver'{consumer_tag=_ConsumerTag, delivery_tag=_DeliveryTag,
			 redelivered=_Redelivered, exchange=_Exchange, 
			 routing_key=RoutingKey}, Content} ->
			 #amqp_msg{payload = Payload} = Content,
			 gen_tcp:send(Sock, Payload),
			 loop(Sock, Channel, Queue)
	end.

start_amqp() ->
	{ok, Connection} = amqp_connection:start(#amqp_params_network{
		username = "amanda",
		password = "password",
		virtual_host = "vhost",
		host = "localhost"
		}),
	{ok, Channel} = amqp_connection:open_channel(Connection),
	EDeclare = #'exchange.declare'{
		exchange = <<"raw_irc">>,
		type = <<"topic">>
	},
	#'exchange.declare_ok'{} = amqp_channel:call(Channel, EDeclare),
	QDeclare = #'queue.declare'{},
	#'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, #'queue.declare'{}),
	Binding = #'queue.bind'{
		queue = Queue,
		exchange = <<"raw_irc">>,
		routing_key = <<"raw.out">>}
	},
	#'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
	{ok, Channel, Queue}.

setup_amqp_consumer(Channel, Queue) ->
	BasicConsume = #'basic.consume'{
		queue = Queue,
		consumer_tag = <<"">>
	},
	#'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:subscribe(Channel, BasicConsume, self()),
	receive
		#'basic.consume_ok'{consumer_tag = ConsumerTag} -> ok
	end.

send_amqp_message(Channel, Key, Payload) ->
	BasicPublish = #'basic.publish'{exchange = <<"raw_irc">>, routing_key = Key},
	ok = amqp_channel:cast(Channel, BasicPublish, _MsgPayload = #amqp_msg{payload = Payload}).