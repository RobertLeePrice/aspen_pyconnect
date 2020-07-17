# Aspen-PyConnect

Aspen-PyConnect is a wrapper for pulling data from Aspen IP.21 historian servers using Python. The current version uses a SOAP connector by default.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install aspen-pyconnect.

```bash
pip install aspen-pyconnect
```

## Usage

### Opening a Connection

```python
from aspen_pyconnect import IP21Connector

aspen = IP21Connector(server='SERVER_NAME', user='DOMAIN\USERNAME', pw='PASSWORD')
aspen.connect()
```

### Loading a Single Tag from History into Pandas DataFrame

```python
import pandas as pd
from datetime import datetime

data = aspen.history(
    start_time=datetime(2020, 1, 1),
    end_time=datetime.now(),
    name='TAG_NAME',
    period='01:00:00',
    stepped=0
)
df = pd.DataFrame(data)
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)