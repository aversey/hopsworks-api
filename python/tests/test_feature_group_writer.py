#
#   Copyright 2023 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from hsfs import feature_group
from hsfs.engine import python


class TestFeatureGroupWriter:
    def test_fg_writer_context_manager(self, mocker, dataframe_fixture_basic):
        mock_insert = mocker.patch("hsfs.feature_group.FeatureGroup.insert")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        with fg.multi_part_insert() as writer:
            assert writer._feature_group == fg
            assert writer._feature_group._multi_part_insert is True
            writer.insert(dataframe_fixture_basic)

        mock_insert.assert_called_once_with(
            features=dataframe_fixture_basic,
            overwrite=False,
            operation="upsert",
            storage=None,
            write_options={"start_offline_materialization": False},
            validation_options={"fetch_expectation_suite": False},
            transformation_context=None,
            transform=True,
        )
        assert fg._multi_part_insert is False

    def test_fg_writer_cache_management(self, mocker, dataframe_fixture_basic):
        engine = python.Engine()
        mocker.patch("hsfs.engine.get_instance", return_value=engine)
        mocker.patch("hopsworks_common.client.get_instance")
        producer, feature_writers, writer_m = (
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
        )
        headers = {
            "projectId": str(99).encode("utf8"),
            "featureGroupId": str(32).encode("utf8"),
            "subjectId": str(12).encode("utf8"),
        }
        mock_init_kafka_resources = mocker.patch(
            "hsfs.core.kafka_engine._init_kafka_resources",
            return_value=(producer, headers, feature_writers, writer_m),
        )
        mocker.patch("hsfs.core.kafka_engine.encode_complex_features")
        mocker.patch("hsfs.core.job.Job")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            features=engine.parse_schema_feature_group(dataframe_fixture_basic, "HUDI"),
            stream=True,
        )
        fg.feature_store = mocker.MagicMock()

        with fg.multi_part_insert() as writer:
            assert writer._feature_group == fg
            assert writer._feature_group._multi_part_insert is True
            writer.insert(dataframe_fixture_basic)

            # after insert cache should be populated
            assert writer._feature_group._multi_part_insert is True
            assert writer._feature_group._kafka_producer == producer
            assert writer._feature_group._feature_writers == feature_writers
            assert writer._feature_group._writer == writer_m
            assert writer._feature_group._kafka_headers == headers

            writer.insert(dataframe_fixture_basic)
            # after second insert should have been called only once
            mock_init_kafka_resources.assert_called_once()

        # leaving context, cache should be reset and call flush
        producer.flush.assert_called_once()
        assert fg._multi_part_insert is False
        assert fg._kafka_producer is None
        assert fg._feature_writers is None
        assert fg._kafka_headers is None
        assert fg._writer is None

    def test_fg_writer_without_context_manager(self, mocker, dataframe_fixture_basic):
        engine = python.Engine()
        mocker.patch("hsfs.engine.get_instance", return_value=engine)
        mocker.patch("hopsworks_common.client.get_instance")
        producer, feature_writers, writer_m = (
            mocker.MagicMock(),
            mocker.MagicMock(),
            mocker.MagicMock(),
        )
        headers = {
            "projectId": str(99).encode("utf8"),
            "featureGroupId": str(32).encode("utf8"),
            "subjectId": str(12).encode("utf8"),
        }
        mock_init_kafka_resources = mocker.patch(
            "hsfs.core.kafka_engine._init_kafka_resources",
            return_value=(producer, headers, feature_writers, writer_m),
        )
        mocker.patch("hsfs.core.kafka_engine.encode_complex_features")
        mocker.patch("hsfs.core.job.Job")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            features=engine.parse_schema_feature_group(dataframe_fixture_basic, "HUDI"),
            stream=True,
        )
        fg.feature_store = mocker.MagicMock()

        fg.multi_part_insert(dataframe_fixture_basic)

        # after first insert cache should be populated
        assert fg._multi_part_insert is True
        assert fg._kafka_producer == producer
        assert fg._feature_writers == feature_writers
        assert fg._kafka_headers == headers
        assert fg._writer == writer_m

        fg.multi_part_insert(dataframe_fixture_basic)
        # after second insert should have been called only once
        mock_init_kafka_resources.assert_called_once()

        # finalize should reset cache and call flush
        fg.finalize_multi_part_insert()
        producer.flush.assert_called_once()
        assert fg._multi_part_insert is False
        assert fg._kafka_producer is None
        assert fg._feature_writers is None
        assert fg._kafka_headers is None
        assert fg._writer is None
