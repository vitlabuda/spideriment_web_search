# SPDX-License-Identifier: BSD-3-Clause
#
# Copyright (c) 2021 Vít Labuda. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
# following conditions are met:
#  1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
#     disclaimer.
#  2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
#     following disclaimer in the documentation and/or other materials provided with the distribution.
#  3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
#     products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from typing import List
import socket
from Settings import Settings
from SearchResult import SearchResult
from SearchResultsFetcherException import SearchResultsFetcherException
from msgess.msgess import MsgESS


class SearchResultsFetcher:
    _QUERY_MESSAGE_CLASS: int = 1
    _RESPONSE_MESSAGE_CLASS: int = 2

    def __init__(self, search_query: str, max_results: int, use_quotient_based_scoring: bool):
        self._search_query: str = search_query
        self._max_results: int = max_results
        self._use_quotient_based_scoring: bool = use_quotient_based_scoring

    def fetch_results_from_search_server(self) -> List[SearchResult]:
        server_socket = None

        try:
            server_socket = self._connect_to_search_server()

            server_msgess = MsgESS(server_socket)
            server_msgess.set_compress_messages(False)

            self._send_query(server_msgess)

            return self._receive_results(server_msgess)

        except OSError:
            raise SearchResultsFetcherException("Failed to establish a connection to the search server!")

        except MsgESS.MsgESSException:
            raise SearchResultsFetcherException("An error occurred while communicating with the search server!")

        finally:
            if server_socket:
                try:
                    server_socket.close()
                except OSError:
                    pass

    def _connect_to_search_server(self) -> socket.socket:
        server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_sock.connect(Settings.SEARCH_SERVER_SOCKET_PATH)

        return server_sock

    def _send_query(self, server_msgess: MsgESS) -> None:
        server_msgess.send_json_object({
            "search_query": self._search_query,
            "max_results": self._max_results,
            "use_quotient_based_scoring": self._use_quotient_based_scoring
        }, SearchResultsFetcher._QUERY_MESSAGE_CLASS)

    def _receive_results(self, server_msgess: MsgESS) -> List[SearchResult]:
        received_json, received_message_class = server_msgess.receive_json_object()

        if received_message_class != SearchResultsFetcher._RESPONSE_MESSAGE_CLASS:
            raise SearchResultsFetcherException("The app received an unexpected message from the search server!")

        try:
            return [SearchResult(search_result_object) for search_result_object in received_json["search_results"]]
        except Exception:  # This will catch all the KeyErrors, ValueErrors, IndexErrors etc., that might occur
            raise SearchResultsFetcherException("The app received invalid data from the search server!")
