import asyncio
import logging
import multiprocessing as mp
import pickle
import sys
import tempfile
from typing import Optional, Union


class _BaseWorkerProto(asyncio.Protocol):
    HEADER_BYTES = 4
    MAX_PACKET_SIZE = 2 ** (HEADER_BYTES * 8)
    BYTE_ORDER = 'big'
    DATA_BUFFER_SIZE = 2 ** 16

    class Message:
        def process(self, worker: Union['AsyncWorker', '_AsyncWorkerClient']):
            pass

    class ScheduleMessage(Message):
        def __init__(self, future_id: int, co, args, kwargs):
            self.future_id = future_id
            self.co = co
            self.args = args
            self.kwargs = kwargs

        def process(self, worker) -> None:
            """
            Start watcher coroutine and put this task to worker dict
            :param worker: server or client worker instance
            :return: None
            """
            t = worker.loop.create_task(self.async_process(worker))

            def on_task_done(future, context=None):
                del worker._executing[self.future_id]

            t.add_done_callback(on_task_done)

            worker._executing[self.future_id] = t

        async def async_process(self, worker: Union['AsyncWorker', '_AsyncWorkerClient']) -> None:
            """
            Coroutine which start actual requested work, watch till it's done and send Result message
            with return data or exception
            :param worker: server or client worker instance
            :return: None
            """
            worker.logger.info(f"Will schedule new coroutine {self}")
            reply = None
            try:
                res = await self.co(*self.args, **self.kwargs)
                reply = _BaseWorkerProto.ResultMessage(self.future_id, res, None)
            except asyncio.CancelledError:
                pass
            except Exception as ex:
                reply = _BaseWorkerProto.ResultMessage(self.future_id, None, ex)
            if reply:
                worker.logger.info(f"Coroutine done, will send {reply}")
                worker._send_object(reply)
            else:
                worker.logger.info(f"Coroutine {self.co} was cancelled by peer")

        def __str__(self):
            return f"schedule_{self.future_id}: {self.co} (args={self.args}, kwargs={self.kwargs})"

    class ResultMessage(Message):
        def __init__(self, future_id, res, exc):
            self.future_id = future_id
            self.result = res
            self.exception = exc

        def process(self, worker) -> None:
            """
            Find worker future by id and set result received from peer
            :param worker: server or client worker instance
            :return: None
            """
            f = worker._waiting.pop(self.future_id, None)
            if f:
                if self.exception:
                    f.set_exception(self.exception)
                else:
                    f.set_result(self.result)
            else:
                worker.logger.error(f"Unexpected result, no future for {self}")

        def __str__(self):
            return f"reply_{self.future_id}: res={self.result}, exception={self.exception}"

    class CancelMessage(Message):
        def __init__(self, future_id):
            self.future_id = future_id

        def process(self, worker) -> None:
            """
            Cancel message received once peer cancel worker coroutine execution, so need to cancel 
            async_process thing on this side
            :param worker: server or client worker instance
            :return: None
            """
            f = worker._executing[self.future_id]
            worker.logger.info(f"will stop this thing {f}")
            f.cancel()

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.loop = loop or asyncio.get_event_loop()

        self._server = False

        self._buffer = bytearray()
        self._current_packet_size = 0

        self._transport: asyncio.transports.Transport = None

        self._next_task_id = 0
        self._executing = {}
        self._waiting = {}

        self.logger = logging.getLogger()

    def schedule_coroutine(self, co: callable, *args, **kwargs) -> asyncio.Future:
        """
        Schedule coroutine to be executed on peer side and return future
        :param co: callable coroutine
        :param args: arguments
        :param kwargs: keyword arguments
        :return: asyncio.Future
        """
        t = self.loop.create_future()
        t_id = self._next_task_id

        def on_future_done(future: asyncio.Future):
            if future.cancelled():
                self._send_object(_BaseWorkerProto.CancelMessage(t_id))
                self._waiting.pop(t_id)

        t.add_done_callback(on_future_done)

        self._waiting[t_id] = t

        self._next_task_id += 1

        self._send_object(_BaseWorkerProto.ScheduleMessage(t_id, co, args, kwargs))

        return t

    def connection_made(self, transport: asyncio.transports.Transport):
        self._transport = transport

    def data_received(self, data: bytes):
        self._buffer += data

        while True:
            if not self._current_packet_size and len(self._buffer) >= self.HEADER_BYTES:
                self._current_packet_size = int.from_bytes(self._buffer[:self.HEADER_BYTES],
                                                           self.BYTE_ORDER,
                                                           signed=False) + self.HEADER_BYTES
            elif len(self._buffer) >= self._current_packet_size > 0:
                packet = self._buffer[self.HEADER_BYTES:self._current_packet_size]

                try:
                    p = pickle.loads(packet)
                except pickle.PickleError:
                    self.logger.exception("Failed to unpickle message")
                else:
                    self.logger.debug(f"Got {p}, will now process")
                    p.process(self)

                self._buffer = self._buffer[self._current_packet_size:]
                self._current_packet_size = 0
            else:
                break

    def _send_object(self, message: Message) -> None:
        """
        Pickle object and send it to peer
        :param message: Message instance
        :return: None
        """
        data = pickle.dumps(message)
        res = len(data).to_bytes(self.HEADER_BYTES, self.BYTE_ORDER) + data
        self._transport.write(res)


class _BaseNetwork:
    async def start_server(self, worker: 'AsyncWorker') -> asyncio.AbstractServer:
        """
        Create server and return instance
        :param worker: AsyncWorker instance
        :return: asyncio.AbstractServer
        """

    async def connect_to_server(self, client_worker: '_AsyncWorkerClient') -> asyncio.BaseTransport:
        """
        Connect to server created by parent process in start_server
        :param client_worker: _AsyncWorkerClient instance
        :return: asyncio.BaseTransport
        """


class _OverIP(_BaseNetwork):
    def __init__(self):
        self._port = None

    async def start_server(self, worker: 'AsyncWorker') -> asyncio.AbstractServer:
        """
        Start IP server on random port and store it for client process
        :param worker: AsyncWorker
        :return: server
        """
        server = await worker.loop.create_server(
            lambda: worker,
            host='127.0.0.1',
            port=0
        )
        self._port = server.sockets[0].getsockname()[1]
        return server

    async def connect_to_server(self, client_worker: '_AsyncWorkerClient') -> asyncio.BaseTransport:
        """
        Connect to server by corresponding port and return transport
        :param client_worker: _AsyncWorkerClient
        :return: transport
        """
        transport, _ = await client_worker.loop.create_connection(
            lambda: client_worker,
            host='127.0.0.1',
            port=self._port
        )
        return transport


if sys.platform == "linux":
    class _OverUnixSocket(_BaseNetwork):
        def __init__(self):
            with tempfile.NamedTemporaryFile(suffix='.socket') as f:
                self._path = f.name

        async def start_server(self, worker: 'AsyncWorker'):
            """
            Create Unix socket server
            :param worker: AsyncWorker
            :return: server
            """
            server = await worker.loop.create_unix_server(
                lambda: worker,
                self._path
            )
            return server

        async def connect_to_server(self, client_worker: '_AsyncWorkerClient'):
            """
            Connect to Unix socket created by parent process
            :param client_worker: _AsyncWorkerClient
            :return: transport
            """
            transport, _ = await client_worker.loop.create_unix_connection(
                lambda: client_worker,
                self._path
            )
            return transport


    network_type = _OverUnixSocket
else:
    network_type = _OverIP


class AsyncWorker(_BaseWorkerProto):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        super().__init__(loop)

        self._server = True

        self._client: mp.Process = None
        self._client_transport: asyncio.transports.Transport = None
        self._client_connected = self.loop.create_future()
        self._client_disconnected = self.loop.create_future()

        self.logger = logging.getLogger('AsyncWorkerServer')

        self._networking = network_type()

    async def start(self) -> None:
        """
        Spawn client process and wait for readiness
        :return: None
        """
        if not self._client:
            server = None
            try:
                server = await self._networking.start_server(self)
                self._client = mp.Process(target=_start_worker, args=(self._networking,))
                self._client.start()

                self._client_transport = await asyncio.wait_for(self._client_connected, timeout=30, loop=self.loop)

            finally:
                if server:
                    server.close()

    async def stop(self) -> None:
        """
        Stop client if any and wait for termination
        :return: None
        """
        # self.logger.debug(f"Executing: {self._executing}")
        # self.logger.debug(f"Waiting: {self._waiting}")
        if self._client:
            self.logger.debug("Waiting for child disconnect...")
            self._client_transport.write_eof()
            try:
                await asyncio.wait_for(self._client_disconnected,
                                       timeout=10, loop=self.loop)
                self._client_transport.close()
                self._client.join()
            except asyncio.TimeoutError:
                self.logger.error("Worker don't want to stop peacefully")
                self._client.kill()

    def connection_made(self, transport: asyncio.Transport):
        super().connection_made(transport)
        self.logger.debug(f'Got connection {transport.get_extra_info("socket")}')
        self._client_connected.set_result(transport)

    def connection_lost(self, exc: Optional[Exception]):
        self.logger.error(f"connection lost: {exc}")
        self._client_disconnected.set_result(True)

    async def __aenter__(self) -> 'AsyncWorker':
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()


class _AsyncWorkerClient(_BaseWorkerProto):
    def __init__(self, networking: _BaseNetwork, loop: Optional[asyncio.AbstractEventLoop] = None):
        super().__init__(loop)

        self._networking = networking
        self._transport = None
        self.logger = logging.getLogger('AsyncWorkerClient')

        self._stop_serving = self.loop.create_future()

        self._tasks = {}

    async def serve(self) -> None:
        """
        Connect to server and wait for data until eof
        :return: None
        """
        try:
            self._transport = await self._networking.connect_to_server(self)
            await self._stop_serving
        finally:
            self.logger.info("Stopping serving")
            if self._transport:
                self._transport.close()
        # self.logger.debug(f"Executing: {self._executing}")
        # self.logger.debug(f"Waiting: {self._waiting}")

    def connection_lost(self, exc: Optional[Exception]):
        if exc:
            self.logger.error(f"connection lost: {exc}")
        if not self._stop_serving.done():  # if eof already received
            if exc:
                self._stop_serving.set_exception(exc)
            else:
                self._stop_serving.set_result(True)

    def eof_received(self):
        self.logger.debug("Got eof, terminating...")
        self._stop_serving.set_result(True)


_client = None


def get_parent_process() -> Union[None, _AsyncWorkerClient]:
    """
    Get reference to
    :return: instance of _AsyncWorkerChild
    """
    return _client


def _start_worker(networking: _BaseNetwork):
    """
    Main function for client process, supposed to be called by multiprocessing once client is spawned
    :param networking:
    :return: None
    """
    global _client
    logger = logging.getLogger("AsyncWorkerClientMain")
    loop = asyncio.get_event_loop()
    if loop:
        loop.stop()

    loop = asyncio.new_event_loop()
    _client = _AsyncWorkerClient(networking, loop=loop)

    logger.info(f"I'm slave")
    try:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_client.serve())
    finally:
        loop.close()
