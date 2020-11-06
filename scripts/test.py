import datetime as dt
from datetime import datetime

if __name__ == "__main__":

    print(dt.datetime.now())
    print(str(dt.datetime.now()))
    string_now = str(dt.datetime.now())
    teste = dt.datetime.now().replace(second=0, microsecond=0).isoformat()
    print(dt.datetime.now().replace(second=0, microsecond=0).isoformat())
    print(type(teste))