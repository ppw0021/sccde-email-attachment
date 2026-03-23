"""
Count-up component.

This component makes various data-type columns and insert the value into it.
Inserted values increasing one by one.

** Parameter format.**

~~~
  {
    "interval_ms": 1000,
    "diff": 1
  }
~~~

| item        | description          |
|:--|:--|
| interval_ms | insert interval (ms) |
| diff        | increasing quant     |

**Out-port columns**

makes following columns.

| Column   | Data-type  | Description |
|:--|:--|:--|
| bool     | BOOLEAN    | bool(count % 2 == 0)                      |
| int8     | INT8       | count (with upper and lower limit)        |
| int16    | INT16      | count (with upper and lower limit)        |
| int32    | INT32      | count (with upper and lower limit)        |
| int64    | INT64      | count (with upper and lower limit)        |
| uint8    | UINT8      | count (with upper and lower limit) if count > 0 |
| uint16   | UINT16     | count (with upper and lower limit) if count > 0 |
| uint32   | UINT32     | count (with upper and lower limit) if count > 0 |
| uint64   | UINT64     | count (with upper and lower limit) if count > 0 |
| float    | FLOAT      | count + 0.123                             |
| double   | DOUBLE     | count + 0.123                             |
| str      | STRING     | str(count)                                |
| bin      | BINARY     | UTF8 string of str(count)                 |
"""
from __future__ import annotations

from speedbeesynapse.component.base import DataType, HiveComponentBase, HiveComponentInfo


class Param:

    """Parameter class."""

    def __init__(self, interval: int, diff: float) -> None:
        """Construct the class instance."""
        self.interval = interval
        self.diff = diff


@HiveComponentInfo(uuid='ccacd2ed-b829-45cf-8b2c-f54f6a5e84b3', name='Scalar type count-up', inports=0, outports=1)
class HiveComponent(HiveComponentBase):

    """Component main class."""

    def premain(self, _param: dict | str) -> None:
        """Prepare columns before running `main` method."""
        self.bool  = self.out_port1.Column('bool', DataType.BOOLEAN)
        self.i8  = self.out_port1.Column('int8', DataType.INT8)
        self.i16 = self.out_port1.Column('int16', DataType.INT16)
        self.i32 = self.out_port1.Column('int32', DataType.INT32)
        self.i64 = self.out_port1.Column('int64', DataType.INT64)
        self.u8  = self.out_port1.Column('uint8', DataType.UINT8)
        self.u16 = self.out_port1.Column('uint16', DataType.UINT16)
        self.u32 = self.out_port1.Column('uint32', DataType.UINT32)
        self.u64 = self.out_port1.Column('uint64', DataType.UINT64)
        self.flt = self.out_port1.Column('float', DataType.FLOAT)
        self.dbl = self.out_port1.Column('double', DataType.DOUBLE)
        self.str = self.out_port1.Column('str', DataType.STRING)
        self.bin = self.out_port1.Column('bin', DataType.BINARY)

    def main(self, raw_param: dict | str) -> None:
        """Execute main process."""
        count = 0
        param = self.parse_param(raw_param)

        for [ts, _skip] in self.interval_iteration(param.interval):
            self.bool.insert(count % 2, ts)
            self.i8.insert(count, ts)
            self.i16.insert(count, ts)
            self.i32.insert(count, ts)
            self.i64.insert(count, ts)
            if count >= 0:
                self.u8.insert(count, ts)
                self.u16.insert(count, ts)
                self.u32.insert(count, ts)
                self.u64.insert(count, ts)
            self.flt.insert(count + 0.123, ts)
            self.dbl.insert(count + 0.123, ts)
            self.str.insert(str(count), ts)
            self.bin.insert(bytes(str(count), 'utf8'), ts)

            count += param.diff

    def parse_param(self, param: dict | str) -> Param:
        """Parse parameter string or parameter dictionaly."""
        if isinstance(param, dict):
            interval = int(param.get('interval_ms', 1000))
            diff = int(param.get('diff', 1))
            return Param(interval*1000000, diff)

        return Param(1000000000, 5)
