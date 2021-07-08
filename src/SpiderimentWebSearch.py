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


import os.path
import flask
from Settings import Settings
from SearchResultsFetcher import SearchResultsFetcher
from SearchResultsFetcherException import SearchResultsFetcherException


os.chdir(os.path.dirname(os.path.realpath(__file__)))

app = flask.Flask(__name__)


@app.route("/", methods=["GET"])
def s_main_page():
    return flask.render_template("index.html",
                                 search_query="",
                                 max_results=Settings.DEFAULT_MAX_RESULTS,
                                 use_quotient_based_scoring=Settings.DEFAULT_USE_QUOTIENT_BASED_SCORING)


@app.route("/search", methods=["GET"])
def s_search():
    # Get the request arguments
    search_query = flask.request.args.get("q", default="", type=str).strip()
    if not search_query:
        return flask.redirect(flask.url_for("s_main_page"))

    max_results = flask.request.args.get("max", default=-1, type=int)
    if max_results <= 0:
        max_results = Settings.DEFAULT_MAX_RESULTS

    use_quotient_based_scoring = flask.request.args.get("qbs", default=False, type=bool)

    # Perform the search
    try:
        search_results_fetcher = SearchResultsFetcher(search_query, max_results, use_quotient_based_scoring)
        search_results = search_results_fetcher.fetch_results_from_search_server()
    except SearchResultsFetcherException as e:
        return flask.render_template("error.html",
                                     search_query=search_query,
                                     max_results=max_results,
                                     use_quotient_based_scoring=use_quotient_based_scoring,
                                     error_message=e.error_message_for_webpage)

    return flask.render_template("results.html",
                                 search_query=search_query,
                                 max_results=max_results,
                                 use_quotient_based_scoring=use_quotient_based_scoring,
                                 search_results=search_results)


if __name__ == '__main__':
    app.run(debug=Settings.DEBUG)
