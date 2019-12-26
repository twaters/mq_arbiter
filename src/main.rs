// TODO move a lot of this shared messaging stuf finto a real library...how do we do that???
// TODO Should this be more object-oriented, or is it OK the way it is...?

use std::collections::HashMap;
fn build_arbiter_header(server_ident : &str, message_id : u8) -> Vec<zmq::Message> {
	// identity
	// empty (identity delimeter)
	// empty ( spare )
	// empty ( spare )
	// empty ( spare )
	// message ID
	return vec!(zmq::Message::from(server_ident),
		zmq::Message::new(),
		zmq::Message::new(),
		zmq::Message::new(),
		zmq::Message::new(),
		zmq::Message::from(vec!(0, message_id)));
}

fn append_checksum(message : &mut Vec<zmq::Message>) {
	message.push(zmq::Message::new());
}

fn send_pong(server_ident : &str, socket : &zmq::Socket) -> zmq::Result<()> {
	let mut ping_msg = build_arbiter_header(&server_ident, 3);
	ping_msg.push(zmq::Message::new());
	append_checksum(&mut ping_msg);
	
	socket.send_multipart(ping_msg, 0)
}

fn receive_multi(socket : &zmq::Socket) -> Vec<zmq::Message> {
	let mut frame = Vec::new();
	let mut more = true;

	while more {
		let mut part_msg = zmq::Message::new();
		socket.recv(&mut part_msg, 0).unwrap();
		more = part_msg.get_more();
		frame.push(part_msg);
	}

	frame
}

fn get_message_type(header : &zmq::Message) -> u8 {
	if header.len() >= 2 {
		println!("got msg {}", header[1]);
		header[1]
	} else {
		255
	}
}

fn is_checksum_valid(message: &Vec<zmq::Message>) -> bool {
	true
}

fn validate_message(message : Vec<zmq::Message>) -> zmq::Result<Vec<zmq::Message>> {
	if message.len() != 8 {
		println!("bad frame len {}", message.len());
		return Err(zmq::Error::EMSGSIZE);
	} else if message[5].len() != 2 {
		println!("bad header len");
		return Err(zmq::Error::EPROTO);
	} else if message[5][1] > 3 {
		println!("wrong message type");
		return Err(zmq::Error::ENOTSUP);
	} else if !is_checksum_valid(&message) {
		println!("checksum fail");
		return Err(zmq::Error::EINVAL);
	}
	
	Ok(message)
}

fn receive_message(socket : &zmq::Socket) -> zmq::Result<Vec<zmq::Message>> {
	let message = receive_multi(&socket);
	validate_message(message)
}

fn wait_for_message(message_type : u8, timeout_ms : i64, socket : &zmq::Socket) ->zmq::Result<Vec<zmq::Message>> {
	if socket.poll(zmq::PollEvents::POLLIN, timeout_ms).unwrap() > 0 {
		let result = receive_message(&socket);
		if let Ok(message) = result {
			if get_message_type(&message[5]) == message_type {
				println!("looks good!");
				Ok(message)
			} else {
				println!("wrong type");
				Err(zmq::Error::EPROTO)
			}
		} else {
			println!("receive failed early");
			result
		}
	} else {
		println!("wait proto");
		Err(zmq::Error::EPROTO)
	}
}

fn spawn_child(identity_name : &str, registration_message : &zmq::Message, children : &mut HashMap<String, zmq::Socket>, ctx : &zmq::Context) {
	// Create a new socket and tie it with the identity...this is how we will communicate with the child processing thread
	let mut child_binding = "inproc://".to_string();
	child_binding.push_str(identity_name);
	
	let parent_socket = ctx.socket(zmq::PAIR).unwrap();
	parent_socket.bind(&child_binding).unwrap();
	children.insert(identity_name.to_string(), parent_socket);
		
	let child_socket = ctx.socket(zmq::PAIR).unwrap();
	child_socket.connect(&child_binding).unwrap();
	
	let ident = identity_name.to_string();

	// Start the child processing thread to handle pings and de-registrations
	std::thread::Builder::new().name(identity_name.to_string())
		.spawn(move || {
			loop {
				if wait_for_message(2, 5000, &child_socket).is_err() {
					// Send deregistration to the parent threac
					send_deregister(&ident, &child_socket).unwrap();
					println!("should deregister..");
					// Exit the thread
					break;
				}
			}
		}).unwrap();
}

fn register_node(identity_msg : &zmq::Message, registration_message : &zmq::Message, children : &mut HashMap<String, zmq::Socket>, ctx : &zmq::Context) {
	// Safely check the application (identity) name
	if let Some(identity_name) = identity_msg.as_str() {
		// Check whether the child already exists
		if let Some(_child) = children.get(identity_name) {
			println!("Already here");
		} else {
			spawn_child(&identity_name, &registration_message, children, &ctx);
		}
	}
	else {
		println!("Invalid identity name");
	}
}

fn send_ping(server_ident : &str, socket : &zmq::Socket) -> zmq::Result<()> {
	let mut ping_msg = build_arbiter_header(&server_ident, 2);
	ping_msg.push(zmq::Message::new());
	append_checksum(&mut ping_msg);
	
	socket.send_multipart(ping_msg, 0)
}

fn send_deregister(server_ident : &str, socket : &zmq::Socket) -> zmq::Result<()> {
	let mut dereg_msg = build_arbiter_header(&server_ident, 1);
	dereg_msg.push(zmq::Message::new());
	append_checksum(&mut dereg_msg);
	
	socket.send_multipart(dereg_msg, 0)
}

fn handle_ping(identity_msg : &zmq::Message, children : &HashMap<String, zmq::Socket>) {
	if let Some(identity_name) = identity_msg.as_str() {
		if let Some(child) = children.get(identity_name) {
			println!("Handling ping");
			// TODO SEems unfortunate that we need to re-build the entire ping message
			// just to forward it to the child thread
			send_ping(&identity_name, &child).unwrap();
		} else {
			println!("Ping received for invalid identity");
		}
	}
}

fn process_node_requests(node_request_socket : &zmq::Socket, ctx : &zmq::Context) {
	let mut children = HashMap::new();
	
	loop {
		// TODO this receives only on the main node rquest socket
		// But we need to add polling for the spawned child threads, as well
		let mut message = receive_multi(&node_request_socket);
		if message.len() == 8 && message[5].len() == 2 {
			// Remove the frames we need, but do it backward because swap_remove
			// doesn't maintain ordering otherwise
			let checksum = message.swap_remove(7);
			let data = message.swap_remove(6);
			let header = message.swap_remove(5);
			let identity = message.swap_remove(0);
			let identity_name = identity.as_str().unwrap();
			match header[1] {
				0 => {
					register_node(&identity, &data, &mut children, &ctx);
					send_pong(&identity_name, &node_request_socket).unwrap();
				},
				1 => println!("deregister received"),
				2 => {
					handle_ping(&identity, &children);
					send_pong(&identity_name, &node_request_socket).unwrap();
				},
				3 => println!("pong received"),
				_ => println!("Invalid message")
			}
		} else {
			println!("Invalid message format {} {}", message.len(), message[2].len());
		}
	}	
}

fn state_publisher(pub_state_socket : &zmq::Socket) {
	let mut i = 0;
	loop {
		i += 1;
		let state = zmq::Message::from_slice(i.to_string().as_bytes());
		pub_state_socket.send(state, 0).unwrap();

		println!("published {}", i);
		std::thread::sleep(std::time::Duration::from_millis(1000));
	}
}

fn main() {
	// Get the command-line arguments
	let mut node_request_bind = "tcp://*:5555";
	let mut arbiter_ident = "ARBITER";
	let mut pub_state_bind = "tcp://*:5556";
	
	let args: Vec<String> = std::env::args().collect();
	
	if args.len() > 1 {
		node_request_bind = &args[1];
	}
	if args.len() > 2 {
		arbiter_ident = &args[2];
	}
	if args.len() > 3 {
		pub_state_bind = &args[3];
	}
	
	// Setup the ZeroMQ context for the process
    let ctx = zmq::Context::new();

	// Startup the state publisher thread
	let pub_state_socket = ctx.socket(zmq::PUB).unwrap();
	pub_state_socket.bind(pub_state_bind).unwrap();
	std::thread::Builder::new().name("State_Publisher".to_string())
		.spawn(move || {
			state_publisher(&pub_state_socket);
		}).unwrap();

	// Setup the main router socket for processing node requests
	let node_request_socket = ctx.socket(zmq::ROUTER).unwrap();
	node_request_socket.set_identity(arbiter_ident.as_bytes()).unwrap();
	node_request_socket.bind(node_request_bind).unwrap();
	process_node_requests(&node_request_socket, &ctx);
}
