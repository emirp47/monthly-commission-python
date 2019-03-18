import pymongo
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys

INF = 1000000
DEFAULT_COMMISSION = 500000
BOUND_COMMISSION = 10000000

def calc_commission(store):
    _commission = 0
    _contractQuatityInMonth = count_contract(store)
    _formulas = load_formula(store)
    _condition = None

    if isinstance(_formulas, list):
        for condition in _formulas:
            if condition["min"] <= _contractQuatityInMonth and condition["max"] > _contractQuatityInMonth:
                _condition = condition
                break
            pass # end if
        pass # end for
    pass # end if

    if _condition is not None:
        try:
            if _condition["total"] is not None:
                _commission = _condition["total"]
            pass
        except:
            _commission = _condition["unit"] * _contractQuatityInMonth
            pass
    else:
        _commission = DEFAULT_COMMISSION
    pass # end if

    _code = 0
    if (_commission > BOUND_COMMISSION):
        _code = 1
        _commission = BOUND_COMMISSION
    pass # end if

    store["body"] = {
        "code": _code,
        "comission": _commission
    }
    return store

def count_contract(store):
    # Start Database Config. -- All config will be able to get from evn.
    _dbConnectionInfo = load_db_connection_info(store)
    _moClient = pymongo.MongoClient(_dbConnectionInfo["DATABASE_CONNECTION"])
    _database = _moClient[_dbConnectionInfo["DATABASE_NAME"]]
    # End Database Config.

    # Start Query Database. -- All config will be able to get from evn.
    _contracts = _database[_dbConnectionInfo["DATABASE_COLLECTION"]]
    _queryParams = store["OPWIRE_REQUEST"]["query"]

    _userId = _queryParams["userId"][0]
    _month = _queryParams["month"][0]
    
    _dateTimeQuery = datetime.strptime(_month, "%Y-%m")
    _dateTimeQueryStart = _dateTimeQuery.replace(day=1)
    _dateTimeQueryEnd = _dateTimeQuery + relativedelta(months=1)

    _query = {"userId": _userId, 'createdAt': {'$lt': _dateTimeQueryEnd, '$gte': _dateTimeQueryStart}}
    _count = _contracts.count(_query)

    return _count

def load_db_connection_info(store):
    _dbConnectionInfo = {
        "DATABASE_CONNECTION" : "localhost:27017",
        "DATABASE_NAME" : "test",
        "DATABASE_COLLECTION" : "contracts"
    }
    return _dbConnectionInfo

def load_formula(store):
    _formula = [
        {
            "min": 0,
            "max": 3,
            "unit": 1000000
        },
        {
            "min": 4,
            "max": 6,
            "unit": 3000000
        },
        {
            "min": 7,
            "max": INF,
            "total": 1000000
        }
    ]
    return _formula