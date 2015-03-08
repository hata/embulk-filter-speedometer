# Speedometer Filter Plugin for Embulk

Write log message of processed bytes and throughput periodically.

## Overview

- **Plugin type**: filter

## Configuration

- **log_interval_seconds**: Interval seconds to write log message periodically. (integer, optional, default: 10). If this value is set to 0, then interval message is not shown. It only show message when there is no active thread.
- **speed_limit**: Set maximum processing size per second. If 0 is set, then no limit. (integer, optional, default: 0)
- **delimiter**: Delimiter text to calculate delimiter length. (string, optional, default: ",")
- **record_padding_size**: Additional byte size for each record like a return code length. (integer, optional, default: 1)

## Example of Configuration

- Use default parameters. Log message is output every 10 seconds.

```yaml
filters:
  - type: speedometer
```

- Change log message interval from 10 seconds(default) to 20 seconds.

```yaml
filters:
  - type: speedometer
    log_interval_seconds: 20
```

- If it is required to set a speed limit for throughput in filter, then set bytes per second to **speed_limit** parameter. If it is not required, then set 0(default). The following example is to set 250kbytes per second. This is all thread's total speed limit.

```yaml
filters:
  - type: speedometer
    log_interval_seconds: 20
    speed_limit: 250000
```

- If it is required to change delimiter size and record padding size, then set **delimiter** and **record_padding_size** . Set text for **delimiter** to calculate length. Set integer to **record_padding_size** to add a length for each record. It is like return code length for each line.

```yaml
filters:
  - type: speedometer
    log_interval_seconds: 20
    speed_limit: 250000
    delimiter: ", "
    record_padding_size: 0
```

## Sample Log Message

```
2015-03-07 18:28:51.208 -0800 [INFO] (task-0007): {speedometer: {active: 0, total: 0.0b, sec: 0.00, speed: 0.0b/s}}
2015-03-07 18:28:51.397 -0800 [INFO] (task-0000): {speedometer: {active: 0, total: 144b, sec: 0.00, speed: 144kb/s}}
2015-03-07 18:29:01.401 -0800 [INFO] (task-0002): {speedometer: {active: 5, total: 9.2mb, sec: 10.0, speed: 999kb/s}}
2015-03-07 18:29:11.410 -0800 [INFO] (task-0008): {speedometer: {active: 5, total: 36.1mb, sec: 20.0, speed: 2.7mb/s}}
```

- **active**: running threads
- **total**: processed bytes. This size is calculated based on text data like csv. For example, boolean value is 4 bytes(true) or 5 bytes(false). The default configuration set a byte for delimiter and a byte padding for each record.
- **sec**: elapsed time.
- **speed**: bytes per second.

## Build

```
$ ./gradlew gem
```

## Note

The shown data is caled based on text data size while using this filter plugin. So, the data is not the same data as read bytes and write bytes n input and output plugins.


