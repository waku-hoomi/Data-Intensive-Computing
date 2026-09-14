"""Order-independent aggregate equality with explicit floating-point tolerance."""
import math
import json
from decimal import Decimal


def assert_same_results(left, right, rel_tol=1e-9, abs_tol=1e-8):
    if len(left) != len(right):
        raise AssertionError(f"Row counts differ: {len(left)} != {len(right)}")
    def key(row):
        # Non-float grouping keys first; full canonical representation breaks ties.
        values = row.asDict() if hasattr(row,"asDict") else row
        identifiers = {k:v for k,v in values.items() if not isinstance(v,(float,Decimal))}
        return json.dumps(identifiers,sort_keys=True,default=str)
    for a,b in zip(sorted(left,key=key),sorted(right,key=key)):
        a = a.asDict() if hasattr(a,"asDict") else a
        b = b.asDict() if hasattr(b,"asDict") else b
        if a.keys() != b.keys():
            raise AssertionError(f"Columns differ: {a.keys()} != {b.keys()}")
        for field in a:
            x,y=a[field],b[field]
            if x is None or y is None:
                equal=x is y
            elif isinstance(x,(float,Decimal)) or isinstance(y,(float,Decimal)):
                x,y=float(x),float(y)
                equal=(math.isnan(x) and math.isnan(y)) or math.isclose(x,y,rel_tol=rel_tol,abs_tol=abs_tol)
            else:
                equal=x==y
            if not equal:
                raise AssertionError(f"Mismatch in {field}: {x!r} != {y!r}")
