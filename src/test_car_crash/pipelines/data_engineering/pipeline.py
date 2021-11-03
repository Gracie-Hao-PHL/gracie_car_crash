# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Example data engineering pipeline with PySpark.
"""

from kedro.pipeline import Pipeline, node

from test_car_crash.nodes.primary import unix_to_datetime, prm_spine
from test_car_crash.nodes.feature import fea_total_miles, fea_rush_hour_ratio, fea_prev_crash
from test_car_crash.nodes.model_input import mod_master


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(unix_to_datetime, ["raw_car_crash"], "prm_car_crash", tags=["car_crash"]),
            node(unix_to_datetime, ["raw_car_location"], "prm_car_location", tags=["car_location"]),
            node(
                prm_spine, ["prm_car_location"], "prm_spine"
            ),  # spine will be at the unit of analysis
            node(fea_total_miles, ["prm_spine", "prm_car_location"], "fea_total_miles"),
            node(fea_rush_hour_ratio, ["prm_spine", "prm_car_location"], "fea_rush_hour_ratio"),
            node(fea_prev_crash, ["prm_spine", "prm_car_crash"], "fea_prev_crash"),
            node(
                mod_master,
                inputs=["prm_spine", "fea_total_miles","fea_rush_hour_ratio", "fea_prev_crash"],
                outputs="mod_master",
            ),
        ]
    )
