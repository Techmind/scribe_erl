-ifndef(_receiver_included).
-define(_receiver_included, yeah).

-record(logMessage, {category, message}).

-record(receiverReply, {code=ok, response}).

-record(config, {port=1464 , 
	max_msg_per_second=2000000, 
	check_interval=3, 
	max_queue_size=2000000, stores=[]}).

-endif.
