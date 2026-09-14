import pytest
from urban_platform.analytics.verification import assert_same_results


def test_equality_ignores_order_and_allows_only_small_float_error():
    assert_same_results([{"zone":1,"mean":0.3},{"zone":2,"mean":None}],
                        [{"zone":2,"mean":None},{"zone":1,"mean":0.1+0.2}])
    with pytest.raises(AssertionError):
        assert_same_results([{"zone":1,"mean":0.3}],[{"zone":1,"mean":0.4}])
    with pytest.raises(AssertionError):
        assert_same_results([{"zone":1,"count":10}],[{"zone":1,"count":11}])
