from ._http_manager import _HTTPManager
from ._websocket_stream import _FuturesWebSocketManager
from ._websocket_stream import USDT_PERPETUAL
from .usdt_perpetual import PUBLIC_WSS, PRIVATE_WSS
from . import _helpers
from concurrent.futures import ThreadPoolExecutor


ws_name = USDT_PERPETUAL


class HTTP(_HTTPManager):
    def get_instruments(self):
        """
        Get symbol info.

        :returns: Request results as dictionary.
        """

        suffix = "/contract/v3/public/copytrading/symbol/list"
        return self._submit_request(
            method="GET",
            path=self.endpoint + suffix
        )

    def place_order(self, **kwargs):
        """
        Places an active order. For more information, see
        https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.

        :param kwargs: See
            https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.
        :returns: Request results as dictionary.
        """

        suffix = "/contract/v3/private/copytrading/order/create"

        return self._submit_request(
            method="POST",
            path=self.endpoint + suffix,
            query=kwargs,
            auth=True
        )

    def get_orders(self, **kwargs):
        """
        Places an active order. For more information, see
        https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.

        :param kwargs: See
            https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.
        :returns: Request results as dictionary.
        """

        suffix = "/contract/v3/private/copytrading/order/list"

        return self._submit_request(
            method="GET",
            path=self.endpoint + suffix,
            query=kwargs,
            auth=True
        )

    def cancel_order(self, **kwargs):
        """
        Places an active order. For more information, see
        https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.

        :param kwargs: See
            https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.
        :returns: Request results as dictionary.
        """

        suffix = "/contract/v3/private/copytrading/order/cancel"

        return self._submit_request(
            method="POST",
            path=self.endpoint + suffix,
            query=kwargs,
            auth=True
        )

    def close_order(self, **kwargs):
        """
        Places an active order. For more information, see
        https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.

        :param kwargs: See
            https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.
        :returns: Request results as dictionary.
        """

        suffix = "/contract/v3/private/copytrading/order/close"

        return self._submit_request(
            method="POST",
            path=self.endpoint + suffix,
            query=kwargs,
            auth=True
        )

    def get_position(self, **kwargs):
        """
        Places an active order. For more information, see
        https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.

        :param kwargs: See
            https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.
        :returns: Request results as dictionary.
        """

        suffix = "/contract/v3/private/copytrading/position/list"

        return self._submit_request(
            method="GET",
            path=self.endpoint + suffix,
            query=kwargs,
            auth=True
        )

    def close_position(self, **kwargs):
        """
        Places an active order. For more information, see
        https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.

        :param kwargs: See
            https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.
        :returns: Request results as dictionary.
        """

        suffix = "/contract/v3/private/copytrading/position/close"

        return self._submit_request(
            method="POST",
            path=self.endpoint + suffix,
            query=kwargs,
            auth=True
        )

    def set_leverage(self, **kwargs):
        """
        Places an active order. For more information, see
        https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.

        :param kwargs: See
            https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.
        :returns: Request results as dictionary.
        """

        suffix = "/contract/v3/private/copytrading/position/set-leverage"

        return self._submit_request(
            method="POST",
            path=self.endpoint + suffix,
            query=kwargs,
            auth=True
        )

    def get_wallet_balance(self, **kwargs):
        """
        Places an active order. For more information, see
        https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.

        :param kwargs: See
            https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.
        :returns: Request results as dictionary.
        """

        suffix = "/contract/v3/private/copytrading/wallet/balance"

        return self._submit_request(
            method="GET",
            path=self.endpoint + suffix,
            query=kwargs,
            auth=True
        )

    def transfer(self, **kwargs):  # todo better name
        """
        Places an active order. For more information, see
        https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.

        :param kwargs: See
            https://bybit-exchange.github.io/docs/usdc/perpetual/#t-usdcplaceorder.
        :returns: Request results as dictionary.
        """

        suffix = "/contract/v3/private/copytrading/wallet/transfer"

        return self._submit_request(
            method="POST",
            path=self.endpoint + suffix,
            query=kwargs,
            auth=True
        )

    def place_active_order_bulk(self, orders: list, max_in_parallel=10):
        """
        Places multiple active orders in bulk using multithreading. For more
        information on place_active_order, see
        https://bybit-exchange.github.io/docs/inverse/#t-activeorders.

        :param list orders: A list of orders and their parameters.
        :param max_in_parallel: The number of requests to be sent in parallel.
            Note that you are limited to 50 requests per second.
        :returns: Future request result dictionaries as a list.
        """

        with ThreadPoolExecutor(max_workers=max_in_parallel) as executor:
            executions = [
                executor.submit(
                    self.place_active_order,
                    **order
                ) for order in orders
            ]
        executor.shutdown()
        return [execution.result() for execution in executions]

    def cancel_active_order_bulk(self, orders: list, max_in_parallel=10):
        """
        Cancels multiple active orders in bulk using multithreading. For more
        information on cancel_active_order, see
        https://bybit-exchange.github.io/docs/inverse/#t-activeorders.

        :param list orders: A list of orders and their parameters.
        :param max_in_parallel: The number of requests to be sent in parallel.
            Note that you are limited to 50 requests per second.
        :returns: Future request result dictionaries as a list.
        """

        with ThreadPoolExecutor(max_workers=max_in_parallel) as executor:
            executions = [
                executor.submit(
                    self.cancel_active_order,
                    **order
                ) for order in orders
            ]
        executor.shutdown()
        return [execution.result() for execution in executions]

    def replace_active_order_bulk(self, orders: list, max_in_parallel=10):
        """
        Replaces multiple active orders in bulk using multithreading. For more
        information on replace_active_order, see
        https://bybit-exchange.github.io/docs/inverse/#t-replaceactive.

        :param list orders: A list of orders and their parameters.
        :param max_in_parallel: The number of requests to be sent in parallel.
            Note that you are limited to 50 requests per second.
        :returns: Future request result dictionaries as a list.
        """

        with ThreadPoolExecutor(max_workers=max_in_parallel) as executor:
            executions = [
                executor.submit(
                    self.replace_active_order,
                    **order
                ) for order in orders
            ]
        executor.shutdown()
        return [execution.result() for execution in executions]


class WebSocket(_FuturesWebSocketManager):
    def __init__(self, **kwargs):
        super().__init__(ws_name, **kwargs)

        self.ws_public = None
        self.ws_private = None
        self.active_connections = []
        self.kwargs = kwargs
        self.public_kwargs = _helpers.make_public_kwargs(self.kwargs)

    def is_connected(self):
        return self._are_connections_connected(self.active_connections)

    def _ws_public_subscribe(self, topic, callback, symbol):
        if not self.ws_public:
            self.ws_public = _FuturesWebSocketManager(
                ws_name, **self.public_kwargs)
            self.ws_public._connect(PUBLIC_WSS)
            self.active_connections.append(self.ws_public)
        self.ws_public.subscribe(topic, callback, symbol)

    def _ws_private_subscribe(self, topic, callback):
        if not self.ws_private:
            self.ws_private = _FuturesWebSocketManager(
                ws_name, **self.kwargs)
            self.ws_private._connect(PRIVATE_WSS)
            self.active_connections.append(self.ws_private)
        self.ws_private.subscribe(topic, callback)

    def custom_topic_stream(self, wss_url, topic, callback):
        subscribe = _helpers.identify_ws_method(
            wss_url,
            {
                PUBLIC_WSS: self._ws_public_subscribe,
                PRIVATE_WSS: self._ws_private_subscribe
            })
        symbol = self._extract_symbol(topic)
        if symbol:
            subscribe(topic, callback, symbol)
        else:
            subscribe(topic, callback)

    # Private topics
    def position_stream(self, callback):
        """
        https://bybit-exchange.github.io/docs/copy_trading/#t-websocketposition
        """
        topic = "copyTradePosition"
        self._ws_private_subscribe(topic=topic, callback=callback)

    def execution_stream(self, callback):
        """
        https://bybit-exchange.github.io/docs/copy_trading/#t-websocketexecution
        """
        topic = "copyTradeExecution"
        self._ws_private_subscribe(topic=topic, callback=callback)

    def order_stream(self, callback):
        """
        https://bybit-exchange.github.io/docs/copy_trading/#t-websocketorder
        """
        topic = "copyTradeOrder"
        self._ws_private_subscribe(topic=topic, callback=callback)

    def wallet_stream(self, callback):
        """
        https://bybit-exchange.github.io/docs/copy_trading/#t-websocketwallet
        """
        topic = "copyTradeWallet"
        self._ws_private_subscribe(topic=topic, callback=callback)
