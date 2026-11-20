# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2019-2026, by Samuel Williams.

require_relative "client"
require_relative "endpoint"

require_relative "body/pipe"

module Async
	module HTTP
		# Wraps a client, address and headers required to initiate a connectio to a remote host using the CONNECT verb.
		# Behaves like a TCP endpoint for the purposes of connecting to a remote host.
		class Proxy
			# Raised when a CONNECT tunnel through a proxy cannot be established.
			class ConnectFailure < StandardError
				# The maximum number of bytes of the response body to read, see {body}.
				MAXIMUM_BODY_SIZE = 1024 * 8
				
				# Initialize the failure with the unsuccessful response.
				# @parameter response [Protocol::HTTP::Response] The failed response from the proxy.
				# @parameter body [String | Nil] The beginning of the response body, if any.
				def initialize(response, body = nil)
					super "Failed to connect: #{response.status}"
					@response = response
					@body = body
				end
				
				# @attribute [Protocol::HTTP::Response] The failed response from the proxy. It has already been closed.
				attr :response
				
				# @attribute [String | Nil] Up to {MAXIMUM_BODY_SIZE} bytes of the response body, which may explain why the proxy rejected the connection.
				#
				# The body is read before this error is raised, so a proxy which neither finishes the body nor closes the connection (e.g. an HTTP/1 response without `content-length` on a persistent connection) will delay the failure until the caller's own timeout, if any.
				attr :body
			end
			
			# A CONNECT tunnel, which is closed once its socket has been closed.
			#
			# Closing the socket of a {Body::Pipe} only ends the CONNECT request body, so the CONNECT response would remain open (and the proxy connection busy) until the proxy closes its end of the tunnel. Instead, the CONNECT response is closed as soon as the socket is closed, and the CONNECT request is closed once any data already written to the socket has been sent. HTTP/2 then resets the stream and HTTP/1 closes the connection, neither of which can be reused after a tunnel anyway.
			class Tunnel
				# The default time in seconds to wait for data already written to the socket to be sent to the proxy, after the socket is closed.
				CLOSE_TIMEOUT = 10
				
				# Extends the socket of a tunnel so that closing it closes the entire tunnel. These methods can be called from any thread.
				module Closable
					# Close the socket, and then the tunnel.
					def close
						if @tunnel
							# Any other descriptor for the socket (e.g. from `clone`) would keep it open, and the tunnel is about to be closed, so make sure they observe the end of the stream:
							begin
								self.shutdown(::Socket::SHUT_RDWR)
							rescue IOError, SystemCallError
								# The socket is already closed or shut down.
							end
						end
						
						super
					ensure
						close_tunnel
					end
					
					# Close the reading side of the socket, and then the tunnel if the writing side is also closed.
					def close_read
						super
					ensure
						close_tunnel if closed?
					end
					
					# Close the writing side of the socket, and then the tunnel if the reading side is also closed.
					def close_write
						super
					ensure
						close_tunnel if closed?
					end
					
					# A copy has its own file descriptor, so closing it must not close the tunnel.
					def initialize_copy(other)
						super
						
						@tunnel = nil
					end
					
					private
					
					def close_tunnel
						if tunnel = @tunnel
							@tunnel = nil
							tunnel.close
						end
					end
				end
				
				# @parameter timeout [Numeric] The time in seconds to wait for data already written to the socket to be sent to the proxy, after the socket is closed.
				def initialize(timeout = CLOSE_TIMEOUT)
					@timeout = timeout
					
					# What has happened to the tunnel: `:sent`, `:received` and `:closed`. A queue can be pushed to from any thread, and its timeout doesn't interrupt the waiting task with an exception, which Ruby 3.3.6 doesn't handle while waiting on a condition variable.
					@events = ::Thread::Queue.new
					
					@input = Body::Writable.new
					
					# The connection closes the request body once it has finished sending it:
					@request_body = ::Protocol::HTTP::Body::Completable.new(@input, proc{@events.push(:sent)})
					
					@response = nil
					@pipe = nil
				end
				
				# @attribute [Protocol::HTTP::Body::Readable] The body of the CONNECT request, which is everything written to the socket.
				attr :request_body
				
				# Open the tunnel using a successful CONNECT response, and start a task which closes the tunnel once its socket is closed.
				# @parameter response [Protocol::HTTP::Response] The CONNECT response.
				# @returns [Socket] The socket for the tunnel, which closes the tunnel when closed.
				def open(response, parent: Task.current)
					@response = response
					
					# The response has no body if the proxy has already finished it. The pipe closes the response body once it has finished receiving it:
					response_body = response.body || ::Protocol::HTTP::Body::Readable.new
					response_body = ::Protocol::HTTP::Body::Completable.new(response_body, proc{@events.push(:received)})
					
					@pipe = Body::Pipe.new(response_body, @input, task: parent)
					
					parent.async(transient: true, annotation: "#{self.class} closer.", &self.method(:closer))
					
					socket = @pipe.to_io
					socket.extend(Closable)
					socket.instance_variable_set(:@tunnel, self)
					
					return socket
				end
				
				# Close the tunnel. The tunnel is closed by its own task, so unlike stopping the tasks of the pipe, this can be called from any thread.
				def close
					@events.push(:closed)
				end
				
				# Close the request body of a tunnel which was not opened, so that the connection stops waiting for it.
				def abort
					@input.close
				end
				
				private
				
				# Wait until one of the given events has happened.
				# @returns [Symbol | Nil] The event which happened, or `nil` if the timeout expired first.
				def wait_for(*events, timeout: nil)
					deadline = Clock.now + timeout if timeout
					
					until event = events.find{|event| @happened.include?(event)}
						if deadline
							remaining = deadline - Clock.now
							return nil unless remaining > 0 and happened = @events.pop(timeout: remaining)
						else
							happened = @events.pop
						end
						
						@happened << happened
						
						# Once both directions have finished, there is nothing left to close:
						@happened << :finished if @happened.include?(:sent) and @happened.include?(:received)
					end
					
					return event
				end
				
				def closer(task)
					@happened = []
					
					# If the tunnel finishes without the socket being closed (e.g. each side of the socket was closed separately), there is nothing to do:
					return unless wait_for(:closed, :finished) == :closed
					
					# Close the CONNECT response and the socket. The pipe forwards any data already written to the socket, and then ends the CONNECT request:
					@pipe.finish
					
					unless wait_for(:sent, timeout: @timeout)
						# The proxy has stopped reading, so give up, which resets an HTTP/2 stream. An HTTP/1 connection which is blocked writing to the proxy is not interrupted:
						@response.close(::IO::TimeoutError.new("Timed out while closing the tunnel!"))
					end
				end
			end
			
			# Extends {Async::HTTP::Client} with proxy capabilities.
			module Client
				# Create a proxy instance for the given endpoint.
				# @parameter endpoint [Endpoint] The target endpoint to tunnel to.
				# @parameter headers [Hash | Nil] Optional headers to send with the CONNECT request.
				# @returns [Proxy] A proxy instance for establishing tunnels.
				def proxy(endpoint, headers = nil)
					Proxy.new(self, endpoint.authority(false), headers)
				end
				
				# Create a client that will proxy requests through the current client.
				def proxied_client(endpoint, headers = nil)
					proxy = self.proxy(endpoint, headers)
					
					return self.class.new(proxy.wrap_endpoint(endpoint))
				end
				
				# Create an endpoint that connects via this proxy.
				# @parameter endpoint [Endpoint] The target endpoint.
				# @parameter headers [Hash | Nil] Optional headers for the CONNECT request.
				# @returns [Endpoint] An endpoint that tunnels through the proxy.
				def proxied_endpoint(endpoint, headers = nil)
					proxy = self.proxy(endpoint, headers)
					
					return proxy.wrap_endpoint(endpoint)
				end
			end
			
			# Prepare and endpoint which can establish a TCP connection to the remote system.
			# @param client [Async::HTTP::Client] the client which will be used as a proxy server.
			# @param host [String] the hostname or address to connect to.
			# @param port [String] the port number to connect to.
			# @param headers [Array] an optional list of headers to use when establishing the connection.
			# @see IO::Endpoint#tcp
			def self.tcp(client, host, port, headers = nil)
				self.new(client, "#{host}:#{port}", headers)
			end
			
			# Construct a endpoint that will use the given client as a proxy for HTTP requests.
			# @param client [Async::HTTP::Client] the client which will be used as a proxy server.
			# @param endpoint [Async::HTTP::Endpoint] the endpoint to connect to.
			# @param headers [Array] an optional list of headers to use when establishing the connection.
			def self.endpoint(client, endpoint, headers = nil)
				proxy = self.new(client, endpoint.authority(false), headers)
				
				return proxy.endpoint(endpoint.url)
			end
			
			# @param client [Async::HTTP::Client] the client which will be used as a proxy server.
			# @param address [String] the address to connect to.
			# @param headers [Array] an optional list of headers to use when establishing the connection.
			# @param close_timeout [Numeric] the time in seconds to wait for data already written to a tunnel to be sent to the proxy, after the tunnel is closed.
			def initialize(client, address, headers = nil, close_timeout: Tunnel::CLOSE_TIMEOUT, **options)
				# Headers given as a trailing hash, e.g. `Proxy.new(client, address, "proxy-authorization" => token)`, are received as keyword arguments:
				if headers.nil?
					headers = options unless options.empty?
				elsif !options.empty?
					raise ArgumentError, "unknown keywords: #{options.keys.map(&:inspect).join(', ')}"
				end
				
				@client = client
				@address = address
				@headers = ::Protocol::HTTP::Headers[headers].freeze
				@close_timeout = close_timeout
			end
			
			attr :client
			
			# Close the underlying client connection.
			def close
				@client.close
			end
			
			# Establish a TCP connection to the specified host.
			#
			# The tunnel is closed when the returned socket is closed, or when the block exits. Any data already written to the socket is sent to the proxy first, unless that takes longer than the close timeout.
			# @return [Socket] a connected bi-directional socket.
			def connect(&block)
				tunnel = Tunnel.new(@close_timeout)
				
				begin
					response = @client.connect(authority: @address, headers: @headers, body: tunnel.request_body)
				ensure
					# If the request failed, e.g. the proxy closed the connection or the caller's timeout expired, a task may still be waiting to read the request body, and would never finish:
					tunnel.abort unless response
				end
				
				if response.success?
					socket = tunnel.open(response)
					
					return socket unless block_given?
					
					begin
						yield socket
					ensure
						socket.close
					end
				else
					# This ensures we don't leave a response dangling:
					tunnel.abort
					
					begin
						body = read_failure_body(response)
					ensure
						response.close
					end
					
					raise ConnectFailure.new(response, body)
				end
			end
			
			# @return [Async::HTTP::Endpoint] an endpoint that connects via the specified proxy.
			def wrap_endpoint(endpoint)
				Endpoint.new(endpoint.url, self, **endpoint.options)
			end
			
			private
			
			# Proxies often explain why they rejected a connection in the response body, so read the beginning of it. Failing to do so shouldn't hide the original failure.
			def read_failure_body(response)
				body = response.body
				return nil unless body
				
				buffer = String.new
				
				begin
					# A single chunk can be much larger than the limit, so only append what is still needed:
					while (remaining = ConnectFailure::MAXIMUM_BODY_SIZE - buffer.bytesize) > 0 and chunk = body.read
						buffer << chunk.byteslice(0, remaining)
					end
				rescue IOError, SystemCallError, ::Protocol::HTTP::Error => error
					# Anything else, e.g. `Async::TimeoutError`, should propagate to the caller:
					Console.debug(self, "Failed to read proxy response body!", error)
				end
				
				return buffer
			end
		end
		
		Client.prepend(Proxy::Client)
	end
end
