--!native
--!strict

const worker = require("@self/worker")

-- Types
export type Connection<T... = ...any> = {
	Type: "Connection",

	IsConnected: boolean,
	Reconnect: (self: Connection<T...>) -> (),
	Disconnect: (self: Connection<T...>) -> (),
}
type _Connection<T... = ...any> = {
	_signal: _Signal<T...>,
	_next: _Connection<T...>?,
	_prev: _Connection<T...>?,
	_func: (T...) -> (),
} & Connection<T...>

export type Signal<T... = ...any> = {
	Type: "Signal",

	Fire: (self: Signal<T...>, T...) -> (),
	Connect: (self: Signal<T...>, callback: (T...) -> ()) -> Connection<T...>,
	Once: (self: Signal<T...>, callback: (T...) -> ()) -> Connection<T...>,
	Wait: (self: Signal<T...>) -> T...,
	DisconnectAll: (self: Signal<T...>) -> (),
	Destroy: (self: Signal<T...>) -> (),
}
type _Signal<T... = ...any> = {
	_head: _Connection<T...>?,
	_rbxConn: RBXScriptConnection?,
} & Signal<T...>


-- Implementation
const Connection = {
	Type = "Connection",

	Reconnect = function<T...>(self: _Connection<T...>)
		if self.IsConnected then return end

		const signal = self._signal
		const nextConnection = signal._head

		if nextConnection ~= nil then nextConnection._prev = self end
		self._next = nextConnection
		signal._head = self

		self.IsConnected = true
	end,

	Disconnect = function<T...>(self: _Connection<T...>)
		if not self.IsConnected then return end

		const nextConnection, prevConnection = self._next, self._prev
		if nextConnection ~= nil then nextConnection._prev = prevConnection end
		if prevConnection ~= nil then prevConnection._next = nextConnection end
		self._next = nil
		self._prev = nil

		self.IsConnected = false
	end,
} :: Connection
(Connection::any).__index = Connection
(Connection::any).__mode = "v"



const Signal = {
	Type = "Signal",

	Fire = function<T...>(self: _Signal<T...>, ...: T...): ()
		local connection = self._head
		while connection ~= nil do
			task.spawn(worker(), connection._func, ...)
			connection = connection._next
		end
	end,

	Connect = function<T...>(self: _Signal<T...>, callback: (T...) -> ()): Connection<T...>
		const nextConnection = self._head

		const connection = setmetatable({}, Connection) :: _Connection<T...>
		connection._signal = self
		connection._next = nextConnection
		connection._func = callback
		connection.IsConnected = true

		if nextConnection ~= nil then nextConnection._prev = connection end
		self._head = connection
		
		return connection
	end,

	Once = function<T...>(self: _Signal<T...>, callback: (T...) -> ()): Connection<T...>
		local connection: Connection<T...>
		connection = self:Connect(function(...: T...)
			connection:Disconnect()
			callback(...)
		end)
		return connection
	end,

	Wait = function<T...>(self: _Signal<T...>): T...
		const co = coroutine.running()
		local connection: Connection<T...>
		connection = self:Connect(function(...: T...)
			connection:Disconnect()
			task.spawn(co, ...)
		end)
		return coroutine.yield()
	end,

	DisconnectAll = function<T...>(self: _Signal<T...>): ()
		local connection = self._head
		while connection ~= nil do
			connection = connection._next, connection:Disconnect()
		end
	end,

	Destroy = function<T...>(self: _Signal<T...>)
		self:DisconnectAll()
		if self._rbxConn then self._rbxConn:Disconnect() end
		table.clear(self)
	end,
} :: Signal
(Signal::any).__index = Signal


-- Constructors
const exports = {
    --[[
	    Construct a new signal.

	    Optionally pass in type args. 
			
	    e.g. signal.new<<(number, boolean)>>()
    ]]
    new = function<T...>(): Signal<T...>
	    return setmetatable({}, Signal) :: Signal<T...>
    end,

    --[[
		Wrap an existing RBXScriptSignal to make it compatible with other signals.

		Types automatically inferred.

		Always call Destroy on wrapped signals, can lead to memory leak otherwise.
	]]
	wrap = function<T...>(signal: RBXScriptSignal<T...>): Signal<T...>
		const self = setmetatable({}, Signal) :: _Signal<T...>
		self._rbxConn = signal:Connect(function(...: T...)
			self:Fire(...)
		end)
		return self
	end,
}

return exports
