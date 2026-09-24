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
	_signal: _Signal<T...>?,
	_func: (T...) -> (),
} & Connection<T...>

export type Signal<T... = ...any> = {
	Type: "Signal",

	Fire: (self: Signal<T...>, T...) -> (),
	Connect: (self: Signal<T...>, callback: (T...) -> ()) -> Connection<T...>,
	Once: (self: Signal<T...>, callback: (T...) -> ()) -> Connection<T...>,
	Wait: & ( (self: Signal<T...>) -> T... ) 
	      & ( (self: Signal<T...>, timeout: number) -> (nil|T...) ),
	DisconnectAll: (self: Signal<T...>) -> (),
	Destroy: (self: Signal<T...>) -> (),
}
type _Signal<T... = ...any> = {
	_connections: {_Connection<T...>},
	_rbxConn: RBXScriptConnection?,
} & Signal<T...>


-- Implementation
const Connection = {
	Type = "Connection",

	Reconnect = function<T...>(self: _Connection<T...>): ()
		if self.IsConnected == true or self._signal == nil then return end
		table.insert(self._signal._connections, self)
		self.IsConnected = true
	end,

	Disconnect = function<T...>(self: _Connection<T...>): ()
		if self.IsConnected == false or self._signal == nil then return end
		table.remove(self._signal._connections, table.find(self._signal._connections, self))
		self.IsConnected = false
	end,
} :: Connection
(Connection::any).__index = Connection
(Connection::any).__mode = "v"



const Signal = {
	Type = "Signal",

	Fire = function<T...>(self: _Signal<T...>, ...: T...): ()
		const temp = table.clone(self._connections)
		for i = #temp, 1, -1 do
			task.spawn(worker(), temp[i]._func, ...)
		end
	end,

	Connect = function<T...>(self: _Signal<T...>, callback: (T...) -> ()): Connection<T...>
		const connection = setmetatable({}, Connection) :: _Connection<T...>
		connection._signal = self
		connection._func = callback
		connection.IsConnected = true

		table.insert(self._connections, connection)
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

	Wait = function<T...>(self: _Signal<T...>, timeout: number?): T...
		const co = coroutine.running()
		
		local connection: Connection<T...>
		const resume = function(...: T...)
			connection:Disconnect()
			task.spawn(co, ...)
		end
		
		connection = self:Connect(resume)
		if timeout ~= nil then
			task.delay(timeout, resume)
		end
		
		return coroutine.yield()
	end,

	DisconnectAll = function<T...>(self: _Signal<T...>): ()
		for i, connection in self._connections do
			connection._signal = nil
		end
		table.clear(self._connections)
	end,

	Destroy = function<T...>(self: _Signal<T...>): ()
		self:DisconnectAll()
		if self._rbxConn then self._rbxConn:Disconnect() end
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
