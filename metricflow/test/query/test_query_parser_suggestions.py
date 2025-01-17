# Need to ignore flake8 as there are blank lines in the test outputs, which throws "W293 blank line contains whitespace"
# flake8: noqa

import logging
import textwrap

import pytest

from metricflow.errors.errors import UnableToSatisfyQueryError
from metricflow.model.objects.common import YamlConfigFile
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.test.fixtures.model_fixtures import query_parser_from_yaml

logger = logging.getLogger(__name__)


EXTENDED_BOOKINGS_YAML = textwrap.dedent(
    """\
    data_source:
      name: bookings_source

      sql_query: |
        -- User Defined SQL Query
        SELECT * FROM bookings_source_table

      measures:
        - name: bookings
          expr: "1"
          agg: sum
          create_metric: true
        - name: instant_bookings
          expr: is_instant
          agg: sum_boolean
          create_metric: true
        - name: booking_value
          agg: sum
          create_metric: true
        - name: max_booking_value
          agg: max
          expr: booking_value
          create_metric: true
        - name: min_booking_value
          agg: min
          expr: booking_value
          create_metric: true
        - name: bookers
          expr: guest_id
          agg: count_distinct
          create_metric: true
        - name: average_booking_value
          expr: booking_value
          agg: average
          create_metric: true
        - name: booking_payments
          expr: booking_value
          agg: sum
          create_metric: true
        - name: referred_bookings
          expr: referrer_id
          agg: count
          create_metric: true

      dimensions:
        - name: is_instant
          type: categorical
        - name: ds
          type: time
          type_params:
            is_primary: True
            time_granularity: day

      identifiers:
        - name: listing
          type: foreign
          expr: listing_id
    """
)

LISTINGS_YAML = textwrap.dedent(
    """\
    data_source:
      name: listings_latest
      description: listings_latest
      owners:
        - support@transformdata.io

      sql_table: schema.table

      measures:
        - name: listings
          expr: 1
          agg: sum

      dimensions:
        - name: ds
          type: time
          expr: created_at
          type_params:
            is_primary: True
            time_granularity: day
        - name: created_at
          type: time
          type_params:
            time_granularity: day
        - name: country_latest
          type: categorical
          expr: country
        - name: capacity_latest
          type: categorical
          expr: capacity

      identifiers:
        - name: listing
          type: primary
          expr: listing_id
        - name: user
          type: foreign
          expr: user_id

      mutability:
        type: immutable
    """
)


def test_nonexistent_metric(time_spine_source: TimeSpineSource) -> None:  # noqa: D
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=EXTENDED_BOOKINGS_YAML)
    query_parser = query_parser_from_yaml([bookings_yaml_file], time_spine_source)

    with pytest.raises(UnableToSatisfyQueryError) as exception_info:
        query_parser.parse_and_validate_query(metric_names=["booking"], group_by_names=["is_instant"])

    assert (
        textwrap.dedent(
            """\
            Unable To Satisfy Query Error: Unknown metric: 'booking'
        
            Suggestions for 'booking':
                ['bookings',
                 'booking_value',
                 'instant_bookings',
                 'booking_payments',
                 'max_booking_value',
                 'min_booking_value']
            """
        ).rstrip()
        == str(exception_info.value)
    )


def test_non_existent_group_by(time_spine_source: TimeSpineSource) -> None:  # noqa: D
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=EXTENDED_BOOKINGS_YAML)
    query_parser = query_parser_from_yaml([bookings_yaml_file], time_spine_source)

    with pytest.raises(UnableToSatisfyQueryError) as exception_info:
        query_parser.parse_and_validate_query(metric_names=["bookings"], group_by_names=["is_instan"])

    assert (
        textwrap.dedent(
            """\
            Unable To Satisfy Query Error: Unknown element name 'is_instan' in dimension name 'is_instan'
            
            Suggestions for 'is_instan':
                ['is_instant']
            """
        ).rstrip()
        == str(exception_info.value)
    )


def test_invalid_group_by(time_spine_source: TimeSpineSource) -> None:  # noqa: D
    bookings_yaml_file = YamlConfigFile(filepath="inline_for_test_1", contents=EXTENDED_BOOKINGS_YAML)
    listings_yaml_file = YamlConfigFile(filepath="inline_for_test_2", contents=LISTINGS_YAML)

    query_parser = query_parser_from_yaml([bookings_yaml_file, listings_yaml_file], time_spine_source)

    with pytest.raises(UnableToSatisfyQueryError) as exception_info:
        query_parser.parse_and_validate_query(metric_names=["bookings"], group_by_names=["capacity_latest"])

    assert (
        textwrap.dedent(
            """\
            Unable To Satisfy Query Error: Dimensions ['capacity_latest'] cannot be resolved for metrics \
['bookings']. The invalid dimension may not exist, require an ambiguous join (e.g. a join path that can be satisfied \
in multiple ways), or require a fanout join.
            
            Suggestions for invalid dimension 'capacity_latest':
                ['listing__capacity_latest', 'listing__country_latest']
            """
        ).rstrip()
        == str(exception_info.value)
    )
